#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc.pb-c.h"
#include "recordReader.h"
#include "orcUtil.h"

#define ORC_MAGIC "ORC"
#define ORC_MAGIC_LENGTH 3

/* macros for getting PostgreSQL column information of a field reader */
#define OrcGetPSQLIndex(fieldReader)  (fieldReader->psqlVariable->varattno - 1)
#define OrcGetPSQLType(fieldReader)  (fieldReader->psqlVariable->vartype)
#define OrcGetPSQLTypeMod(fieldReader)  (fieldReader->psqlVariable->vartypmod)
#define OrcGetPSQLChildType(fieldReader)  get_element_type(fieldReader->psqlVariable->vartype)

PostScript * PostScriptInit(FILE *file, long *postScriptSizeOffset, CompressionParameters *parameters);
Footer * FileFooterInit(FILE *file, int footerOffset, long footerSize, CompressionParameters *parameters);
StripeFooter * StripeFooterInit(FILE *file, StripeInformation *stripeInfo, CompressionParameters *parameters);

int FieldReaderAllocate(FieldReader *reader, Footer *footer, List *columns);
int FieldReaderInit(FieldReader *fieldReader, FILE *file, StripeInformation *stripe,
		StripeFooter *stripeFooter, CompressionParameters *parameters);
void FieldReaderSeek(FieldReader *rowReader, int strideNo);
int FieldReaderFree(FieldReader *reader);

#endif /* FILEREADER_H_ */
