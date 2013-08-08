#include <stdio.h>
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

int printAllData(StructReader* structReader, int noOfRows)
{
	Reader* reader = NULL;
	int rowNo = 0;
	int columnNo = 0;
	FieldValue value;
	int length = 0;
	int isNull = 0;

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structReader->noOfFields; ++columnNo)
		{
			reader = &structReader->fields[columnNo];
			isNull = readPrimitiveType(reader, &value, &length);
			if (isNull)
			{
				printf("(NULL)|");
			}
			else if (isNull == 0)
			{
				printFieldValue(&value, reader->kind);
				printf("|");
			}
			else
			{
				return isNull;
			}
		}
		printf("\n");
	}

	return 0;
}

int main(int argc, char **argv)
{
	/*FILE* orcFile = fopen("/home/gokhan/orc-files/output.orc", "r");*/
	FILE* orcFile = fopen("timestamp.orc", "r");
	StructReader structReader;
	PostScript *postScript = NULL;
	Footer *footer = NULL;
	StripeInformation* stripe = NULL;
	StripeFooter* stripeFooter = NULL;
	uint8_t *stripeFooterBuffer = NULL;
	long stripeFooterOffset = 0;
	int postScriptSize = 0;
	long footerSize = 0;
	int result = 0;

	readPostscript(orcFile, &postScript, &postScriptSize);
	footerSize = postScript->footerlength;

	/* read the file footer */
	result = readFileFooter(orcFile, &footer, 1 + postScriptSize, footerSize);
	initStripeReader(footer, &structReader);

	int i = 0;
	for (i = 0; i < footer->n_stripes; i++)
	{
		stripe = footer->stripes[i];
		stripeFooterOffset = stripe->offset + stripe->datalength
				+ ((stripe->has_indexlength) ? stripe->indexlength : 0);
		result = readStripeFooter(orcFile, &stripeFooter, stripe);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe footer");
			exit(1);
		}

		result = readStripeData(stripeFooter, stripeFooterOffset - stripe->datalength, &structReader, orcFile);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe info");
			exit(1);
		}

		result = printAllData(&structReader, stripe->numberofrows);
		if (result)
		{
			fprintf(stderr, "Error while printing values");
			exit(1);
		}

		free(stripeFooterBuffer);
		stripe_footer__free_unpacked(stripeFooter, NULL);
	}

	post_script__free_unpacked(postScript, NULL);
	footer__free_unpacked(footer, NULL);

	return 0;
}
