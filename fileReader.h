#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <stdio.h>
#include <stdlib.h>
#include "orc_proto.pb-c.h"
#include "recordReader.h"

/* read meta-data from the file */

int readPostscript(FILE* orcFile, PostScript ** postScriptPtr, int* postScriptSizePtr);

int readFileFooter(FILE* orcFile, Footer** footer, int footerOffsetFromEnd, long footerSize);

int readStripeFooter(FILE* orcFile, StripeFooter** stripeFooter, StripeInformation* stripeInfo);

void initStripeReader(Footer* footer, StructReader* reader);

/* read actual data from the file */

int readDataStream(StreamReader* streamReader, Type__Kind streamKind, FILE* orcFile, long offset, long length);

int readStripeData(StripeFooter* stripeFooter, long dataOffset, StructReader* structReader, FILE* orcFile);

#endif /* FILEREADER_H_ */
