#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc_proto.pb-c.h"
#include "recordReader.h"
#include "util.h"

#define isComplexType(type) (type == TYPE__KIND__LIST || type == TYPE__KIND__STRUCT || type == TYPE__KIND__MAP)
#define DIRECTORY_SIZE_GUESS 16384

/* read meta-data from the file */

int readPostscript(FILE* orcFile, PostScript ** postScriptPtr, int* postScriptSizePtr);

int readFileFooter(FILE* orcFile, Footer** footer, int footerOffsetFromEnd, long footerSize);

int readStripeFooter(FILE* orcFile, StripeFooter** stripeFooter, StripeInformation* stripeInfo);

int initStripeReader(Footer* footer, StructReader* reader);

/* read actual data from the file */

int readDataStream(StreamReader* streamReader, Type__Kind streamKind, FILE* orcFile, long offset, long length);

int readStripeData(StripeFooter* stripeFooter, long dataOffset, StructReader* structReader, FILE* orcFile);

#endif /* FILEREADER_H_ */
