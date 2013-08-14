#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc.pb-c.h"
#include "recordReader.h"
#include "util.h"

PostScript* PostScriptInit(char* orcFileName, long* postScriptSizeOffset, CompressionParameters* parameters);

Footer* FileFooterInit(char* orcFileName, int footerOffset, long footerSize,
		CompressionParameters* parameters);

StripeFooter* StripeFooterInit(char* orcFile, StripeInformation* stripeInfo,
		CompressionParameters* parameters);

int FieldReaderAllocate(FieldReader* reader, Footer* footer, char* selectedFields);

int FieldReaderInit(FieldReader* fieldReader, char* orcFileName, StripeInformation* stripe,
		StripeFooter* stripeFooter, CompressionParameters* parameters);

#endif /* FILEREADER_H_ */
