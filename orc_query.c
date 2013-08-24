#include "postgres.h"
#include "orc_fdw.h"
#include "orc_query.h"
#include "orcUtil.h"

#include "access/skey.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

static Expr* OrcFindArgumentOfType(List *argumentList, NodeTag argumentType);
static OrcQueryOperator OrcGetQueryOperator(const char *operatorName);
static Node* BuildBaseConstraint(Var* variable);
static OpExpr* MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue);
static int OrcGetStrideStatistics(Var* variable, RowIndexEntry* entry, Datum* min, Datum* max);
/*
 * ApplicableOpExpressionList walks over all filter clauses that relate to this
 * foreign table, and chooses applicable clauses that we know we can translate
 * into Mongo queries. Currently, these clauses include comparison expressions
 * that have a column and a constant as arguments. For example, "o_orderdate >=
 * date '1994-01-01' + interval '1' year" is an applicable expression.
 *
 * This function is taken from mongo_fdw.
 */
List *
ApplicableOpExpressionList(RelOptInfo *baserel)
{
	List *opExpressionList = NIL;
	List *restrictInfoList = baserel->baserestrictinfo;
	ListCell *restrictInfoCell = NULL;

	foreach(restrictInfoCell, restrictInfoList)
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
		Expr *expression = restrictInfo->clause;
		NodeTag expressionType = 0;

		OpExpr *opExpression = NULL;
		char *operatorName = NULL;
		OrcQueryOperator orcQueryOperator = 0;
		List *argumentList = NIL;
		Var *column = NULL;
		Const *constant = NULL;
		bool constantIsArray = false;

		/* we only support operator expressions */
		expressionType = nodeTag(expression);
		if (expressionType != T_OpExpr)
		{
			continue;
		}

		opExpression = (OpExpr *) expression;
		operatorName = get_opname(opExpression->opno);

		orcQueryOperator = OrcGetQueryOperator(operatorName);
		if (orcQueryOperator < 0)
		{
			continue;
		}

		/*
		 * We only support simple binary operators that compare a column against
		 * a constant. If the expression is a tree, we don't recurse into it.
		 */
		argumentList = opExpression->args;
		column = (Var *) OrcFindArgumentOfType(argumentList, T_Var);
		constant = (Const *) OrcFindArgumentOfType(argumentList, T_Const);

		/*
		 * We don't push down operators where the constant is an array, since
		 * conditional operators for arrays in MongoDB aren't properly defined.
		 * For example, {similar_products : [ "B0009S4IJW", "6301964144" ]}
		 * finds results that are equal to the array, but {similar_products:
		 * {$gte: [ "B0009S4IJW", "6301964144" ]}} returns an empty set.
		 */
		if (constant != NULL)
		{
			Oid constantArrayTypeId = get_element_type(constant->consttype);
			if (constantArrayTypeId != InvalidOid)
			{
				constantIsArray = true;
			}
		}

		if (column != NULL && constant != NULL && !constantIsArray)
		{
			opExpressionList = lappend(opExpressionList, opExpression);
		}
	}

	return opExpressionList;
}

/*
 * MongoOperatorName takes in the given PostgreSQL comparison operator name, and
 * returns its equivalent in MongoDB.
 */
static OrcQueryOperator OrcGetQueryOperator(const char *operatorName)
{
	OrcQueryOperator orcOperatorName = -1;

	const int32 nameCount = 6;
	static const char *nameMappings[] =
	{ "=", "<", ">", "<=", ">=", "<>" };

	int32 nameIndex = 0;

	for (nameIndex = 0; nameIndex < nameCount; nameIndex++)
	{
		const char *pgOperatorName = nameMappings[nameIndex];
		if (strncmp(pgOperatorName, operatorName, NAMEDATALEN) == 0)
		{
			orcOperatorName = nameIndex;
			break;
		}
	}

	return orcOperatorName;
}

/*
 * FindArgumentOfType walks over the given argument list, looks for an argument
 * with the given type, and returns the argument if it is found.
 */
static Expr* OrcFindArgumentOfType(List *argumentList, NodeTag argumentType)
{
	Expr *foundArgument = NULL;
	ListCell *argumentCell = NULL;

	foreach(argumentCell, argumentList)
	{
		Expr *argument = (Expr *) lfirst(argumentCell);
		if (nodeTag(argument) == argumentType)
		{
			foundArgument = argument;
			break;
		}
	}

	return foundArgument;
}

/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. Note that this function assumes there is only one
 * relation for now.
 */
List *
BuildRestrictInfoList(List *qualList)
{
	List *restrictInfoList = NIL;

	ListCell *qualCell = NULL;
	foreach(qualCell, qualList)
	{
		RestrictInfo *restrictInfo = NULL;
		Node *qualNode = (Node *) lfirst(qualCell);

		restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
}

static Node *
BuildBaseConstraint(Var* variable)
{
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	/* Build these expressions with only one argument for now */
	lessThanExpr = MakeOpExpression(variable, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(variable, BTGreaterEqualStrategyNumber);

	/* Build base constaint as an and of two qual conditions */
	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}

static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Const *constantValue = NULL;
	OpExpr *expression = NULL;

	/* varchar causes problem, change it with text */
	if (typeId == VARCHAROID)
	{
		typeId = TEXTOID;
	}

	/* Load the operator from system catalogs */
	operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
	InvalidOid, /* no result type yet */
	false, /* no return set */
	(Expr *) variable, (Expr *) constantValue,
	InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}

/* Returns operator oid for the given type, access method, and strategy number. */
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
}

/* Updates the base constraint with the given min/max values. */
static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr); /* right op */
	Node *maxNode = get_rightop((Expr *) lessThanExpr); /* right op */
	Const *minConstant = NULL;
	Const *maxConstant = NULL;

	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	minConstant = (Const *) minNode;
	maxConstant = (Const *) maxNode;

	minConstant->constvalue = minValue;
	maxConstant->constvalue = maxValue;

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = true;
	maxConstant->constbyval = true;
}

/*
 * Creates a restriction list that contains the minimum and maximum values of the
 * requested columns in the current row stride.
 *
 * @param rowReader field reader for the whole column
 * @param strideNo
 */
List*
OrcCreateStrideRestrictions(FieldReader* rowReader, int strideNo)
{
	List *strideRestrictions = NIL;
	StructFieldReader* structReader = (StructFieldReader*) rowReader->fieldReader;
	FieldReader* subfield = NULL;
	RowIndex* rowIndex = NULL;
	RowIndexEntry* rowIndexEntry = NULL;
	Node* baseRestriction = NULL;
	Datum minValue = 0;
	Datum maxValue = 0;
	int iterator = 0;
	int hasStatistics = 0;

	for (iterator = 0; iterator < structReader->noOfFields; ++iterator)
	{
		subfield = structReader->fields[iterator];
		if (subfield->required)
		{
			rowIndex = subfield->rowIndex;
			if (strideNo > rowIndex->n_entry)
			{
				LogError("Row stride does not exist");
				return NULL;
			}

			rowIndexEntry = rowIndex->entry[strideNo];
			baseRestriction = BuildBaseConstraint(subfield->psqlVariable);

			hasStatistics = OrcGetStrideStatistics(subfield->psqlVariable, rowIndexEntry, &minValue,
					&maxValue);

			if (hasStatistics)
			{
				UpdateConstraint(baseRestriction, minValue, maxValue);
				strideRestrictions = lappend(strideRestrictions, baseRestriction);
			}

		}
	}

	return strideRestrictions;
}

/*
 * Reads the min/max value from the row index entry into datum pointers
 *
 * @return 1 for success, 0 when min/max value cannot be obtained
 */
static int OrcGetStrideStatistics(Var* variable, RowIndexEntry* entry, Datum* min, Datum* max)
{
	ColumnStatistics* statistics = entry->statistics;

	switch (variable->vartype)
	{
	case INT2OID:
	{
		*min = Int16GetDatum((short) statistics->intstatistics->minimum);
		*max = Int16GetDatum((short) statistics->intstatistics->maximum);
		return 1;
	}
	case INT4OID:
	{
		*min = Int32GetDatum((int) statistics->intstatistics->minimum);
		*max = Int32GetDatum((int) statistics->intstatistics->maximum);
		return 1;
	}
	case INT8OID:
	{
		*min = Int64GetDatum((int) statistics->intstatistics->minimum);
		*max = Int64GetDatum((int) statistics->intstatistics->maximum);
		return 1;
	}
	case FLOAT4OID:
	case FLOAT8OID:
	{
		*min = Float8GetDatum((double) statistics->doublestatistics->minimum);
		*max = Float8GetDatum((double) statistics->doublestatistics->maximum);
		return 1;
	}
	case BPCHAROID:
	{
		*min = DirectFunctionCall3(bpcharin, CStringGetDatum(statistics->stringstatistics->minimum),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(variable->vartypmod));
		*max = DirectFunctionCall3(bpcharin, CStringGetDatum(statistics->stringstatistics->maximum),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(variable->vartypmod));
		return 1;
	}
	case VARCHAROID:
	{
		*min =
		DirectFunctionCall3(varcharin, CStringGetDatum(statistics->stringstatistics->minimum),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(variable->vartypmod));
		*max =
		DirectFunctionCall3(varcharin, CStringGetDatum(statistics->stringstatistics->maximum),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(variable->vartypmod));
		return 1;
	}
	case TEXTOID:
	{
		*min = CStringGetTextDatum(statistics->stringstatistics->minimum);
		*max = CStringGetTextDatum(statistics->stringstatistics->maximum);
		return 1;
	}
	case DATEOID:
	{
		*min = DateADTGetDatum(statistics->datestatistics->minimum - ORC_PSQL_EPOCH_IN_DAYS);
		*max = DateADTGetDatum(statistics->datestatistics->maximum - ORC_PSQL_EPOCH_IN_DAYS);
		return 1;
	}
	default:
	{
		*min = 0;
		*max = 0;
		return 0;
	}
	}
}
