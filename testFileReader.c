#include <stdio.h>
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

int printAllData(StructReader* structReader, int noOfRows)
{
	Reader* reader = NULL;
	int rowNo = 0;
	int columnNo = 0;
	Field field;
	int length = 0;
	int listLength = 0;
	int isNull = 0;
	int iterator = 0;
	Type__Kind listItemKind = 0;

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structReader->noOfFields; ++columnNo)
		{
			reader = structReader->fields[columnNo];
			isNull = readField(reader, &field, &length);
			if (isNull)
			{
				printf("(NULL)|");
			}
			else if (isNull == 0)
			{
				if (reader->kind == TYPE__KIND__LIST)
				{
					listLength = length;
					listItemKind = ((ListReader*)reader->fieldReader)->itemReader.kind;
					printf("[");
					for (iterator = 0; iterator < listLength; ++iterator)
					{
						length = field.listItemSizes ? field.listItemSizes[iterator] : 0;
						isNull = field.isItemNull[iterator];
						if (isNull)
						{
							printf("(NULL),");
						}
						else
						{
							printFieldValue(&field.list[iterator], listItemKind, length);
							printf(",");
						}
					}
					printf("]|");
					free(field.list);
					free(field.isItemNull);
					free(field.listItemSizes);
				}
				else
				{
					printFieldValue(&field.value, reader->kind, length);
					printf("|");
				}
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
	FILE* orcFile = fopen("/home/gokhan/orc-files/bigrow1.orc", "r");
//	FILE* orcFile = fopen("short.orc", "r");
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

	result = readPostscript(orcFile, &postScript, &postScriptSize);
	if (result)
	{
		fprintf(stderr, "Error while reading postscript\n");
		exit(1);
	}
	footerSize = postScript->footerlength;

	/* read the file footer */
	result = readFileFooter(orcFile, &footer, 1 + postScriptSize + footerSize, footerSize);
	if (result)
	{
		fprintf(stderr, "Error while reading file footer\n");
		exit(1);
	}

	result = initStripeReader(footer, &structReader);
	if (result)
	{
		fprintf(stderr, "Error while initializing structure reader\n");
		exit(1);
	}

	int i = 0;
	for (i = 0; i < footer->n_stripes; i++)
	{
		stripe = footer->stripes[i];
		stripeFooterOffset = stripe->offset + stripe->datalength
				+ ((stripe->has_indexlength) ? stripe->indexlength : 0);
		result = readStripeFooter(orcFile, &stripeFooter, stripe);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe footer\n");
			exit(1);
		}

		result = readStripeData(stripeFooter, stripeFooterOffset - stripe->datalength, &structReader, orcFile);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe data\n");
			exit(1);
		}

		result = printAllData(&structReader, stripe->numberofrows);
		if (result)
		{
			fprintf(stderr, "Error while printing values\n");
			exit(1);
		}

		free(stripeFooterBuffer);
		stripe_footer__free_unpacked(stripeFooter, NULL);
	}

	post_script__free_unpacked(postScript, NULL);
	footer__free_unpacked(footer, NULL);

	return 0;
}
