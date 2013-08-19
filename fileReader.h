#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc.pb-c.h"
#include "recordReader.h"
#include "orcUtil.h"

typedef struct
{
	uint32_t columnIndex;
	uint32_t columnTypeId;
	uint32_t columnTypeMod;
	uint32_t columnArrayTypeId;
} PostgresColumnInfo;

typedef struct
{
	/* list of selected table columns and their properties */
	PostgresColumnInfo* selectedColumns;
	int noOfSelectedColumns;
} PostgresQueryInfo;

PostScript* PostScriptInit(FILE* file, long* postScriptSizeOffset,
		CompressionParameters* parameters);

Footer* FileFooterInit(FILE* file, int footerOffset, long footerSize,
		CompressionParameters* parameters);

StripeFooter* StripeFooterInit(FILE* file, StripeInformation* stripeInfo,
		CompressionParameters* parameters);

int FieldReaderAllocate(FieldReader* reader, Footer* footer, PostgresQueryInfo* query);

int FieldReaderInit(FieldReader* fieldReader, FILE* file, StripeInformation* stripe,
		StripeFooter* stripeFooter, CompressionParameters* parameters);

#endif /* FILEREADER_H_ */
