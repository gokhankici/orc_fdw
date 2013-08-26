/*-------------------------------------------------------------------------
 *
 * orc_fdw.h
 *
 * Type and function declarations for JSON foreign data wrapper.
 *
 * Copyright (c) 2013, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef ORC_FDW_H
#define ORC_FDW_H

#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"

#include "orc.pb-c.h"
#include "recordReader.h"

#define ENABLE_ROW_SKIPPING 1

/* Defines for valid option names and default values */
#define OPTION_NAME_FILENAME "filename"

#define ORC_TUPLE_COST_MULTIPLIER 10

/*
 * OrcValidOption keeps an option name and a context. When an option is passed
 * into orc_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct OrcValidOption
{
	const char *optionName;
	Oid optionContextId;

} OrcValidOption;


/* Array of options that are valid for orc_fdw */
static const uint32 ValidOptionCount = 3;
static const OrcValidOption ValidOptionArray[] =
{
	/* foreign table options */
	{ OPTION_NAME_FILENAME, ForeignTableRelationId },
};


/*
 * OrcFdwOptions holds the option values to be used when reading and parsing
 * the orc file. To resolve these values, we first check foreign table's
 * options, and if not present, we then fall back to the default values 
 * specified above.
 */
typedef struct OrcFdwOptions
{
	char *filename;

} OrcFdwOptions;


/*
 * OrcFdwExecState keeps foreign data wrapper specific execution state that we
 * create and hold onto when executing the query.
 */
typedef struct OrcFdwExecState
{
	char *filename;
	FILE *file;
	PostScript *postScript;
	Footer *footer;
	StripeFooter *stripeFooter;
	RowIndex** rowIndices;
	CompressionParameters compressionParameters;
	FieldReader* recordReader;
	MemoryContext orcContext;
	List* opExpressionList;

	uint32 nextStripeNumber;
	StripeInformation *currentStripeInfo;
	uint32 currentLineNumber;
} OrcFdwExecState;


/*
 * ColumnMapping reprents a hash table entry that maps a column name to column
 * related information. We construct these hash table entries to speed up the
 * conversion from JSON documents to PostgreSQL tuples; and each hash entry maps
 * the column name to the column's tuple index and its type-related information.
 */
typedef struct ColumnMapping
{
	char columnName[NAMEDATALEN];
	uint32 columnIndex;
	Oid columnTypeId;
	int32 columnTypeMod;
	Oid columnArrayTypeId;

} ColumnMapping;


/* Function declarations for foreign data wrapper */
extern Datum orc_fdw_handler(PG_FUNCTION_ARGS);
extern Datum orc_fdw_validator(PG_FUNCTION_ARGS);


#endif   /* orc_fdw_H */
