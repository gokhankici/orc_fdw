#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc_proto.pb-c.h"
#include "recordReader.h"
#include "util.h"

#define isComplexType(type) (type == TYPE__KIND__LIST || type == TYPE__KIND__STRUCT || type == TYPE__KIND__MAP)

PostScript* readPostscript(char* orcFileName, long* postScriptSizeOffset);

Footer* readFileFooter(char* orcFileName, int footerOffset, long footerSize);

StripeFooter* readStripeFooter(char* orcFile, StripeInformation* stripeInfo);

int StructReader_allocate(StructReader* reader, Footer* footer, char* selectedFields);

int StructReader_init(StructReader* structReader, char* orcFileName, long dataOffset, StripeFooter* stripeFooter);

#endif /* FILEREADER_H_ */
