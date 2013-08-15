/*-------------------------------------------------------------------------
 *
 * orc_fdw.c
 *
 * Function definitions for JSON foreign data wrapper.
 *
 * Copyright (c) 2013, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "orc_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/int8.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "orc.pb-c.h"
#include "fileReader.h"

/* Local functions forward declarations */
static StringInfo OptionNamesString(Oid currentContextId);

static void OrcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static void OrcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static ForeignScan * OrcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
		ForeignPath *bestPath, List *targetList, List *scanClauses);
static void OrcExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState);
static void OrcBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot * OrcIterateForeignScan(ForeignScanState *scanState);
static void OrcReScanForeignScan(ForeignScanState *scanState);
static void OrcEndForeignScan(ForeignScanState *scanState);

static OrcFdwOptions * OrcGetOptions(Oid foreignTableId);
static char * OrcGetOptionValue(Oid foreignTableId, const char *optionName);
static double TupleCount(RelOptInfo *baserel, const char *filename);
static BlockNumber PageCount(const char *filename);
static List * ColumnList(RelOptInfo *baserel);
static bool OrcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *acquireSampleRowsFunc,
		BlockNumber *totalPageCount);
static int OrcAcquireSampleRows(Relation relation, int logLevel, HeapTuple *sampleRows, int targetRowCount,
		double *totalRowCount, double *totalDeadRowCount);

/**
 * Helper functions
 */
static void OrcGetNextStripe(OrcFdwExecState* execState);
static void FillTupleSlot(FieldReader* recordReader, Datum *columnValues, bool *columnNulls);

static Datum ColumnValue(FieldValue* fieldValue, int psqlType, int columnTypeMod);
static Datum ColumnValueArray(Field* field, Oid valueTypeId, int columnTypeMod, int listSize);

/* Declarations for dynamic loading */
PG_MODULE_MAGIC
;

PG_FUNCTION_INFO_V1(orc_fdw_handler);
PG_FUNCTION_INFO_V1(orc_fdw_validator);

/*
 * orc_fdw_handler creates and returns a struct with pointers to foreign table
 * callback functions.
 */
Datum orc_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	fdwRoutine->GetForeignRelSize = OrcGetForeignRelSize;
	fdwRoutine->GetForeignPaths = OrcGetForeignPaths;
	fdwRoutine->GetForeignPlan = OrcGetForeignPlan;
	fdwRoutine->ExplainForeignScan = OrcExplainForeignScan;
	fdwRoutine->BeginForeignScan = OrcBeginForeignScan;
	fdwRoutine->IterateForeignScan = OrcIterateForeignScan;
	fdwRoutine->ReScanForeignScan = OrcReScanForeignScan;
	fdwRoutine->EndForeignScan = OrcEndForeignScan;
	fdwRoutine->AnalyzeForeignTable = OrcAnalyzeForeignTable;

	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * orc_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid. The
 * filename option is required by the foreign table, so we error out if it is
 * not provided.
 */
Datum orc_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum optionArray = PG_GETARG_DATUM(0);
	Oid optionContextId = PG_GETARG_OID(1);
	List *optionList = untransformRelOptions(optionArray);
	ListCell *optionCell = NULL;
	bool filenameFound = false;

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionName = optionDef->defname;
		bool optionValid = false;

		int32 optionIndex = 0;
		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const OrcValidOption *validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId)
					&& (strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* if invalid option, display an informative error message */
		if (!optionValid)
		{
			StringInfo optionNamesString = OptionNamesString(optionContextId);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option \"%s\"", optionName), errhint("Valid options in this context are: %s", optionNamesString->data)));
		}

		if (strncmp(optionName, OPTION_NAME_FILENAME, NAMEDATALEN) == 0)
		{
			filenameFound = true;
		}
	}

	if (optionContextId == ForeignTableRelationId)
	{
		if (!filenameFound)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmsg("filename is required for orc_fdw foreign tables")));
		}
	}

	PG_RETURN_VOID() ;
}

/*
 * OptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string. The function
 * is unchanged from mongo_fdw.
 */
static StringInfo OptionNamesString(Oid currentContextId)
{
	StringInfo optionNamesString = makeStringInfo();
	bool firstOptionAppended = false;

	int32 optionIndex = 0;
	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const OrcValidOption *validOption = &(ValidOptionArray[optionIndex]);

		/* if option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionAppended)
			{
				appendStringInfoString(optionNamesString, ", ");
			}

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionAppended = true;
		}
	}

	return optionNamesString;
}

/*
 * OrcGetForeignRelSize obtains relation size estimates for a foreign table and
 * puts its estimate for row count into baserel->rows.
 */
/* FIXME Use footer here ? */
static void OrcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	OrcFdwOptions *options = OrcGetOptions(foreignTableId);

	double tupleCount = TupleCount(baserel, options->filename);
	double rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);

	double outputRowCount = clamp_row_est(tupleCount * rowSelectivity);
	baserel->rows = outputRowCount;
}

/*
 * JsonGetForeignPaths creates possible access paths for a scan on the foreign
 * table. Currently we only have one possible access path, which simply returns
 * all records in the order they appear in the underlying file.
 */
static void OrcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	Path *foreignScanPath = NULL;
	OrcFdwOptions *options = OrcGetOptions(foreignTableId);

	BlockNumber pageCount = PageCount(options->filename);
	double tupleCount = TupleCount(baserel, options->filename);

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan to account for
	 * the cost of parsing records.
	 */
	double tupleParseCost = cpu_tuple_cost * ORC_TUPLE_COST_MULTIPLIER;
	double tupleFilterCost = baserel->baserestrictcost.per_tuple;
	double cpuCostPerTuple = tupleParseCost + tupleFilterCost;
	double executionCost = (seq_page_cost * pageCount) + (cpuCostPerTuple * tupleCount);

	double startupCost = baserel->baserestrictcost.startup;
	double totalCost = startupCost + executionCost;

	/* create a foreign path node and add it as the only possible path */
	foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows, startupCost, totalCost,
	NIL, /* no known ordering */
	NULL, /* not parameterized */
	NIL); /* no fdw_private */

	add_path(baserel, foreignScanPath);
}

/*
 * OrcGetForeignPlan creates a ForeignScan plan node for scanning the foreign
 * table. We also add the query column list to scan nodes private list, because
 * we need it later for mapping columns.
 */
static ForeignScan *
OrcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId, ForeignPath *bestPath, List *targetList,
		List *scanClauses)
{
	ForeignScan *foreignScan = NULL;
	List *columnList = NULL;
	List *foreignPrivateList = NIL;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scanClauses into the plan node's qual list for the executor
	 * to check.
	 */
	scanClauses = extract_actual_clauses(scanClauses, false);

	/*
	 * As an optimization, we only add columns that are present in the query to
	 * the column mapping hash. To find these columns, we need baserel. We don't
	 * have access to baserel in executor's callback functions, so we get the
	 * column list here and put it into foreign scan node's private list.
	 */
	columnList = ColumnList(baserel);
	foreignPrivateList = list_make1(columnList);

	/* create the foreign scan node */
	foreignScan = make_foreignscan(targetList, scanClauses, baserel->relid,
	NIL, /* no expressions to evaluate */
	foreignPrivateList);

	return foreignScan;
}

/* OrcExplainForeignScan produces extra output for the Explain command. */
static void OrcExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState)
{
	Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	OrcFdwOptions *options = OrcGetOptions(foreignTableId);

	ExplainPropertyText("Orc File", options->filename, explainState);

	/* supress file size if we're not showing cost details */
	if (explainState->costs)
	{
		struct stat statBuffer;

		int statResult = stat(options->filename, &statBuffer);
		if (statResult == 0)
		{
			ExplainPropertyLong("Json File Size", (long) statBuffer.st_size, explainState);
		}
	}
}

/**
 * Iteratres to the next stripe and initializes the record reader.
 * Returns true if there is another stripe.
 */
static void OrcGetNextStripe(OrcFdwExecState* execState)
{
	Footer* footer = execState->footer;
	StripeInformation* stripeInfo = NULL;
	StripeFooter* stripeFooter = NULL;
	int result = 0;

	if (execState->nextStripeNumber < footer->n_stripes)
	{
		stripeInfo = footer->stripes[execState->nextStripeNumber];
		execState->nextStripeNumber++;

		stripeFooter = StripeFooterInit(execState->filename, stripeInfo, &execState->compressionParameters);

		result = FieldReaderInit(execState->recordReader, execState->filename, stripeInfo, stripeFooter,
				&execState->compressionParameters);

		if (result)
		{
			elog(ERROR, "Cannot read the next stripe information\n");
		}

		if (execState->stripeFooter)
		{
			pfree(execState->stripeFooter);
		}

		execState->stripeFooter = stripeFooter;
		execState->currentStripeInfo = stripeInfo;
	}
}

static void OrcInitializeFieldReader(OrcFdwExecState* execState, List* columns)
{
	FieldReader *recordReader = execState->recordReader;
	Footer* footer = execState->footer;
	ListCell* columnCell = NULL;
	Var *column = NULL;
	PostgresQueryInfo query;
	PostgresColumnInfo* queryColumns = NULL;
	int iterator = 0;
	int result = 0;

	/* allocate enough space for info */
	query.selectedColumns = palloc(sizeof(PostgresColumnInfo) * footer->types[0]->n_subtypes);
	queryColumns = query.selectedColumns;

	foreach(columnCell, columns)
	{
		column = (Var *) lfirst(columnCell);
		queryColumns[iterator].columnIndex = column->varattno - 1;
		queryColumns[iterator].columnTypeId = column->vartype;
		queryColumns[iterator].columnTypeMod = column->vartypmod;
		queryColumns[iterator].columnArrayTypeId = get_element_type(column->vartype);
		iterator++;
	}

	query.noOfSelectedColumns = iterator;

	/* Allocate space for column readers */
	result = FieldReaderAllocate(recordReader, footer, &query);

	if (result)
	{
		elog(ERROR, "Error while allocating initializing record reader\n");
	}

	/* Use next stripe to initialize record reader */
	OrcGetNextStripe(execState);

	pfree(queryColumns);
}

/*
 * OrcBeginForeignScan opens the underlying ORC file to read its PostScript and
 * Footer to get information. Then it initializes record reader for reading.
 * The function also creates a hash table that maps referenced column names to column index
 * and type information.
 */
static void OrcBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
	OrcFdwExecState *execState = NULL;
	ForeignScan *foreignScan = NULL;
	List *foreignPrivateList = NULL;
	Oid foreignTableId = InvalidOid;
	OrcFdwOptions *options = NULL;
	List *columnList = NULL;
	PostScript* postScript = NULL;
	Footer* footer = NULL;
	long postScriptOffset = 0;

	/* if Explain with no Analyze, do nothing */
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		return;
	}

	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	options = OrcGetOptions(foreignTableId);

	foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	foreignPrivateList = (List *) foreignScan->fdw_private;

	columnList = (List *) linitial(foreignPrivateList);

	execState = (OrcFdwExecState *) palloc(sizeof(OrcFdwExecState));
	execState->filename = options->filename;
	execState->currentLineNumber = 0;
	execState->nextStripeNumber = 0;
	execState->stripeFooter = NULL;
	execState->currentStripeInfo = NULL;

	postScript = PostScriptInit(options->filename, &postScriptOffset, &execState->compressionParameters);

	if (postScript == NULL)
	{
		elog(ERROR, "Cannot read postscript from the file\n");
	}

	execState->postScript = postScript;

	footer = FileFooterInit(options->filename, postScriptOffset - postScript->footerlength, postScript->footerlength,
			&execState->compressionParameters);

	if (footer == NULL)
	{
		elog(ERROR, "Cannot read file footer from the file\n");
	}

	execState->footer = footer;

	execState->recordReader = malloc(sizeof(FieldReader));

	OrcInitializeFieldReader(execState, columnList);

	scanState->fdw_state = (void *) execState;
}

/*
 * OrcIterateForeignScan reads the next record from the data file, converts it
 * to PostgreSQL tuple, and stores the converted tuple into the ScanTupleSlot as
 * a virtual tuple.
 */
static TupleTableSlot *
OrcIterateForeignScan(ForeignScanState *scanState)
{
	OrcFdwExecState *execState = (OrcFdwExecState *) scanState->fdw_state;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	StripeInformation* currentStripe = execState->currentStripeInfo;

	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum *columnValues = tupleSlot->tts_values;
	bool *columnNulls = tupleSlot->tts_isnull;
	int columnCount = tupleDescriptor->natts;

	/* initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	ExecClearTuple(tupleSlot);

	if (currentStripe == NULL)
	{
		/* file is empty */
		return NULL;
	}
	else if (execState->currentLineNumber >= currentStripe->numberofrows)
	{
		/* End of stripe, read next one */
		OrcGetNextStripe(execState);

		if (execState->nextStripeNumber >= execState->footer->n_stripes)
		{
			/* finish reading if there are no more stipes left */
			return NULL;
		}
	}

	FillTupleSlot(execState->recordReader, columnValues, columnNulls);
	execState->currentLineNumber++;

	ExecStoreVirtualTuple(tupleSlot);

	return tupleSlot;
}

/* OrcReScanForeignScan rescans the foreign table. */
static void OrcReScanForeignScan(ForeignScanState *scanState)
{
	/* TODO update here to not to read ps/footer again for efficiency */
	OrcEndForeignScan(scanState);
	OrcBeginForeignScan(scanState, 0);
}

/*
 * OrcEndForeignScan finishes scanning the foreign table, and frees the acquired
 * resources.
 */
static void OrcEndForeignScan(ForeignScanState *scanState)
{
	OrcFdwExecState *executionState = (OrcFdwExecState *) scanState->fdw_state;
	int result = 0;

	if (executionState == NULL)
	{
		return;
	}

	if (executionState->recordReader)
	{
		result = FieldReaderFree(executionState->recordReader);
		if (result)
		{
			elog(ERROR, "Error while deallocating record reader memory");
		}
		free(executionState->recordReader);
		executionState->recordReader = NULL;
	}

	if (executionState->stripeFooter)
	{
		stripe_footer__free_unpacked(executionState->stripeFooter, NULL);
		executionState->stripeFooter = NULL;
	}

	if (executionState->footer)
	{
		footer__free_unpacked(executionState->footer, NULL);
		executionState->footer = NULL;
	}

	if (executionState->postScript)
	{
		post_script__free_unpacked(executionState->postScript, NULL);
		executionState->postScript = NULL;
	}
}

/*
 * OrcGetOptions returns the option values to be used when reading and parsing
 * the orc file.
 */
static OrcFdwOptions *
OrcGetOptions(Oid foreignTableId)
{
	OrcFdwOptions *jsonFdwOptions = NULL;
	char *filename = NULL;

	filename = OrcGetOptionValue(foreignTableId, OPTION_NAME_FILENAME);

	jsonFdwOptions = (OrcFdwOptions *) palloc0(sizeof(OrcFdwOptions));
	jsonFdwOptions->filename = filename;

	return jsonFdwOptions;
}

/*
 * OrcGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *
OrcGetOptionValue(Oid foreignTableId, const char *optionName)
{
	ForeignTable *foreignTable = NULL;
	ForeignServer *foreignServer = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;
	char *optionValue = NULL;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}

/* TupleCount estimates the number of base relation tuples in the given file. */
static double TupleCount(RelOptInfo *baserel, const char *filename)
{
	double tupleCount = 0.0;

	BlockNumber pageCountEstimate = baserel->pages;
	if (pageCountEstimate > 0)
	{
		/*
		 * We have number of pages and number of tuples from pg_class (from a
		 * previous Analyze), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double density = baserel->tuples / (double) pageCountEstimate;
		BlockNumber pageCount = PageCount(filename);

		tupleCount = clamp_row_est(density * (double) pageCount);
	}
	else
	{
		/*
		 * Otherwise we have to fake it. We back into this estimate using the
		 * planner's idea of relation width, which may be inaccurate. For better
		 * estimates, users need to run Analyze.
		 */
		struct stat statBuffer;
		int tupleWidth = 0;

		int statResult = stat(filename, &statBuffer);
		if (statResult < 0)
		{
			/* file may not be there at plan time, so use a default estimate */
			statBuffer.st_size = 10 * BLCKSZ;
		}

		tupleWidth = MAXALIGN(baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));
		tupleCount = clamp_row_est((double) statBuffer.st_size / (double) tupleWidth);
	}

	return tupleCount;
}

/* PageCount calculates and returns the number of pages in a file. */
static BlockNumber PageCount(const char *filename)
{
	BlockNumber pageCount = 0;
	struct stat statBuffer;

	/* if file doesn't exist at plan time, use default estimate for its size */
	int statResult = stat(filename, &statBuffer);
	if (statResult < 0)
	{
		statBuffer.st_size = 10 * BLCKSZ;
	}

	pageCount = (statBuffer.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pageCount < 1)
	{
		pageCount = 1;
	}

	return pageCount;
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *
ColumnList(RelOptInfo *baserel)
{
	List *columnList = NIL;
	List *neededColumnList = NIL;
	AttrNumber columnIndex = 1;
	AttrNumber columnCount = baserel->max_attr;
	List *targetColumnList = baserel->reltargetlist;
	List *restrictInfoList = baserel->baserestrictinfo;
	ListCell *restrictInfoCell = NULL;

	/* first add the columns used in joins and projections */
	neededColumnList = list_copy(targetColumnList);

	/* then walk over all restriction clauses, and pull up any used columns */
	foreach(restrictInfoCell, restrictInfoList)
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
		Node *restrictClause = (Node *) restrictInfo->clause;
		List *clauseColumnList = NIL;

		/* recursively pull up any columns used in the restriction clause */
		clauseColumnList = pull_var_clause(restrictClause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

		neededColumnList = list_union(neededColumnList, clauseColumnList);
	}

	/* walk over all column definitions, and de-duplicate column list */
	for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
	{
		ListCell *neededColumnCell = NULL;
		Var *column = NULL;

		/* look for this column in the needed column list */
		foreach(neededColumnCell, neededColumnList)
		{
			Var *neededColumn = (Var *) lfirst(neededColumnCell);
			if (neededColumn->varattno == columnIndex)
			{
				column = neededColumn;
				break;
			}
		}

		if (column != NULL)
		{
			columnList = lappend(columnList, column);
		}
	}

	return columnList;
}

static void FillTupleSlot(FieldReader* recordReader, Datum *columnValues, bool *columnNulls)
{
	FieldReader* fieldReader = NULL;
	StructFieldReader* structFieldReader = NULL;
	ListFieldReader* listFieldReader = NULL;
	int columnNo = 0;
	Field field;
	int length = 0;
	int isNull = 0;

	structFieldReader = (StructFieldReader*) recordReader->fieldReader;

	for (columnNo = 0; columnNo < structFieldReader->noOfFields; ++columnNo)
	{
		fieldReader = structFieldReader->fields[columnNo];
		if (!fieldReader->required)
		{
			continue;
		}

		isNull = FieldReaderRead(fieldReader, &field, &length);

		if (isNull == 0)
		{
			if (fieldReader->kind == FIELD_TYPE__KIND__LIST)
			{
				if (field.isItemNull == NULL || field.list == NULL)
				{
					elog(ERROR, "Error reading list element from the field\n");
				}

				listFieldReader = (ListFieldReader*) fieldReader->fieldReader;
				columnValues[columnNo] = ColumnValueArray(&field, listFieldReader->itemReader.psqlKind, listFieldReader->itemReader.columnTypeMod, length);
				columnNulls[columnNo] = false;

				if (field.list)
					freeMemory(field.list);
				if (field.isItemNull)
					freeMemory(field.isItemNull);
				if (field.listItemSizes)
					freeMemory(field.listItemSizes);
			}
			else
			{
				columnValues[columnNo] = ColumnValue(&field.value, fieldReader->psqlKind,
						fieldReader->columnTypeMod);
				columnNulls[columnNo] = false;
			}
		}
		else if (isNull == -1)
		{
			elog(ERROR, "Error while decoding column data\n");
		}
	}

}

/*
 * ColumnValueArray uses array element type id to read the current array pointed
 * to by the jsonArray, and converts each array element with matching type to
 * the corresponding PostgreSQL datum. Then, the function constructs an array
 * datum from element datums, and returns the array datum. This function ignores
 * values that aren't type compatible with valueTypeId.
 */
static Datum ColumnValueArray(Field* field, Oid valueTypeId, int columnTypeMod, int listSize)
{
	Datum columnValueDatum = 0;
	ArrayType *columnValueObject = NULL;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;
	uint32 itemIndex = 0;
	FieldValue* itemFieldValue = field->list;
	char* isItemNull = field->isItemNull;
	uint32 datumArraySize = 0;

	/* allocate enough room for datum array's maximum possible size */
	Datum *datumArray = palloc0(listSize * sizeof(Datum));
	memset(datumArray, 0, listSize * sizeof(Datum));

	for (itemIndex = 0; itemIndex < listSize; itemIndex++)
	{
		if (!isItemNull[itemIndex])
		{
			datumArray[itemIndex] = ColumnValue(itemFieldValue, valueTypeId, columnTypeMod);
		}
		itemFieldValue++;
		datumArraySize++;
	}

	get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue, &typeAlignment);
	columnValueObject = construct_array(datumArray, datumArraySize, valueTypeId, typeLength, typeByValue,
			typeAlignment);

	columnValueDatum = PointerGetDatum(columnValueObject);
	return columnValueDatum;
}

/*
 * ColumnValue uses column type information to read the current value pointed to
 * by jsonValue, and converts this value to the corresponding PostgreSQL datum.
 * The function then returns this datum.
 */
static Datum ColumnValue(FieldValue* fieldValue, int psqlType, int columnTypeMod)
{
	Datum columnValue = 0;
	double doubleValue = 0;

	switch (psqlType)
	{
	case INT2OID:
	{
		columnValue = Int16GetDatum(fieldValue->value64);
		break;
	}
	case INT4OID:
	{
		columnValue = Int32GetDatum(fieldValue->value64);
		break;
	}
	case INT8OID:
	{
		columnValue = Int16GetDatum(fieldValue->value64);
		break;
	}
	case FLOAT4OID:
	{
		doubleValue = fieldValue->floatValue;
		columnValue = Float8GetDatum(doubleValue);
		break;
	}
	case FLOAT8OID:
	{
		columnValue = Float8GetDatum(fieldValue->doubleValue);
		break;
	}
	case NUMERICOID:
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmsg("Numeric type is not supported")));
		break;
	}
	case BOOLOID:
	{
		columnValue = BoolGetDatum(fieldValue->value8);
		break;
	}
	case BPCHAROID:
	{
		columnValue = DirectFunctionCall3(bpcharin, CStringGetDatum(fieldValue->binary),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(columnTypeMod));
		break;
	}
	case VARCHAROID:
	{
		columnValue = DirectFunctionCall3(varcharin, CStringGetDatum(fieldValue->binary),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(columnTypeMod));
		break;
	}
	case TEXTOID:
	{
		columnValue = CStringGetTextDatum(fieldValue->binary);
		break;
	}
	case DATEOID:
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
	default:
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmsg("cannot convert json type to column type"), errhint("column type: %u", (uint32) psqlType)));
		break;
	}
	}

	return columnValue;
}

/*
 * JsonAnalyzeForeignTable sets the total page count and the function pointer
 * used to acquire a random sample of rows from the foreign file.
 */
static bool OrcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *acquireSampleRowsFunc,
		BlockNumber *totalPageCount)
{
	Oid foreignTableId = RelationGetRelid(relation);
	OrcFdwOptions *options = OrcGetOptions(foreignTableId);
	BlockNumber pageCount = 0;
	struct stat statBuffer;

	int statResult = stat(options->filename, &statBuffer);
	if (statResult < 0)
	{
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", options->filename)));
	}

	/*
	 * Our estimate should return at least 1 so that we can tell later on that
	 * pg_class.relpages is not default.
	 */
	pageCount = (statBuffer.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pageCount < 1)
	{
		pageCount = 1;
	}

	(*totalPageCount) = pageCount;
	(*acquireSampleRowsFunc) = OrcAcquireSampleRows;

	return true;
}

/*
 * JsonAcquireSampleRows acquires a random sample of rows from the foreign
 * table. Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries. The actual number of rows
 * selected is returned as the function result. We also count the number of rows
 * in the collection and return it in total row count. We also always set dead
 * row count to zero.
 *
 * Note that the returned list of rows does not always follow their actual order
 * in the JSON file. Therefore, correlation estimates derived later could be
 * inaccurate, but that's OK. We currently don't use correlation estimates (the
 * planner only pays attention to correlation for index scans).
 */
static int OrcAcquireSampleRows(Relation relation, int logLevel, HeapTuple *sampleRows, int targetRowCount,
		double *totalRowCount, double *totalDeadRowCount)
{
	int sampleRowCount = 0;
	double rowCount = 0.0;
	double rowCountToSkip = -1; /* -1 means not set yet */
	double selectionState = 0;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext = NULL;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TupleTableSlot *scanTupleSlot = NULL;
	List *columnList = NIL;
	List *foreignPrivateList = NULL;
	ForeignScanState *scanState = NULL;
	ForeignScan *foreignScan = NULL;
	char *relationName = NULL;
	int executorFlags = 0;

	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	int columnCount = tupleDescriptor->natts;
	Form_pg_attribute *attributes = tupleDescriptor->attrs;

	/* create list of columns of the relation */
	int columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Var *column = (Var *) palloc0(sizeof(Var));

		/* only assign required fields for column mapping hash */
		column->varattno = columnIndex + 1;
		column->vartype = attributes[columnIndex]->atttypid;
		column->vartypmod = attributes[columnIndex]->atttypmod;

		columnList = lappend(columnList, column);
	}

	/* setup foreign scan plan node */
	foreignPrivateList = list_make1(columnList);
	foreignScan = makeNode(ForeignScan);
	foreignScan->fdw_private = foreignPrivateList;

	/* set up tuple slot */
	columnValues = (Datum *) palloc0(columnCount * sizeof(Datum));
	columnNulls = (bool *) palloc0(columnCount * sizeof(bool));
	scanTupleSlot = MakeTupleTableSlot();
	scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
	scanTupleSlot->tts_values = columnValues;
	scanTupleSlot->tts_isnull = columnNulls;

	/* setup scan state */
	scanState = makeNode(ForeignScanState);
	scanState->ss.ss_currentRelation = relation;
	scanState->ss.ps.plan = (Plan *) foreignScan;
	scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

	OrcBeginForeignScan(scanState, executorFlags);

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read and
	 * parse rows from the file using ReadLineFromFile and FillTupleSlot.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext, "orc_fdw temporary context",
	ALLOCSET_DEFAULT_MINSIZE,
	ALLOCSET_DEFAULT_INITSIZE,
	ALLOCSET_DEFAULT_MAXSIZE);

	/* prepare for sampling rows */
	selectionState = anl_init_selection_state(targetRowCount);

	for (;;)
	{
		/* check for user-requested abort or sleep */
		vacuum_delay_point();

		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		MemoryContextReset(tupleContext);
		MemoryContextSwitchTo(tupleContext);

		/* read the next record */
		OrcIterateForeignScan(scanState);

		MemoryContextSwitchTo(oldContext);

		/* if there are no more records to read, break */
		if (scanTupleSlot->tts_isempty)
		{
			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir. Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
		{
			sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor, columnValues, columnNulls);
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
			{
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount, &selectionState);
			}

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int rowIndex = (int) (targetRowCount * anl_random_fract());
				Assert(rowIndex >= 0);
				Assert(rowIndex < targetRowCount);

				heap_freetuple(sampleRows[rowIndex]);
				sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor, columnValues, columnNulls);
			}

			rowCountToSkip -= 1;
		}

		rowCount += 1;
	}

	/* clean up */
	MemoryContextDelete(tupleContext);
	pfree(columnValues);
	pfree(columnNulls);

	OrcEndForeignScan(scanState);

	/* emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(logLevel,
			(errmsg("\"%s\": file contains %.0f rows; %d rows in sample", relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}
