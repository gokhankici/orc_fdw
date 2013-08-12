#include <stdio.h>
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

int printAllData(FILE* file, StructReader* structReader, int noOfRows)
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
	if (file == NULL)
	{
		fprintf(stderr, "Cannot open file to read.");
		return 1;
	}

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structReader->noOfFields; ++columnNo)
		{
			reader = structReader->fields[columnNo];
			isNull = readField(reader, &field, &length);
			if (isNull)
			{
				fprintf(file, "(NULL)|");
			}
			else if (isNull == 0)
			{
				if (reader->kind == TYPE__KIND__LIST)
				{
					listLength = length;
					listItemKind = ((ListReader*) reader->fieldReader)->itemReader.kind;
					fprintf(file, "[");
					for (iterator = 0; iterator < listLength; ++iterator)
					{
						length = field.listItemSizes ? field.listItemSizes[iterator] : 0;
						isNull = field.isItemNull[iterator];
						if (isNull)
						{
							fprintf(file, "(NULL),");
						}
						else
						{
							printFieldValue(file, &field.list[iterator], listItemKind, length);
							fprintf(file, ",");
						}
					}
					fprintf(file, "]|");
					free(field.list);
					free(field.isItemNull);
					free(field.listItemSizes);
				}
				else
				{
					printFieldValue(file, &field.value, reader->kind, length);
					fprintf(file, "|");
				}
			}
			else
			{
				return isNull;
			}
		}
		fprintf(file, "\n");
		fflush(file);
	}

	return 0;
}

int main(int argc, char **argv)
{

	char *orcFileName = NULL;
	char *outputFileName = NULL;
	FILE* outputFile = NULL;

	StructReader structReader;
	PostScript *postScript = NULL;
	Footer *footer = NULL;
	StripeInformation* stripe = NULL;
	StripeFooter* stripeFooter = NULL;
	uint8_t *stripeFooterBuffer = NULL;
	long stripeFooterOffset = 0;
	long psOffset = 0;
	long footerSize = 0;
	int result = 0;

//	char orcFileName[] = "/home/gokhan/orc-files/output_gzip_lcomment.orc";
//	char outputFileName[] = "/tmp/test";
//	FILE* outputFile = stdout;

	if (argc < 2 || argc > 3)
	{
		printf("usage: testFileReader inputFile [outputFile]\n");
		return 1;
	}
	orcFileName = argv[1];

	if (argc == 2)
	{
		outputFile = stdout;
	}
	else
	{
		outputFileName = argv[2];
		outputFile = fopen(outputFileName, "w");
	}

	if (outputFile == NULL)
	{
		fprintf(stderr, "Cannot open file to write\n");
		return 1;
	}

	result = readPostscript(orcFileName, &postScript, &psOffset);
	if (result)
	{
		fprintf(stderr, "Error while reading postscript\n");
		exit(1);
	}
	footerSize = postScript->footerlength;

	/* read the file footer */
	result = readFileFooter(orcFileName, &footer, psOffset - footerSize, footerSize);
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
		result = readStripeFooter(orcFileName, &stripeFooter, stripe);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe footer\n");
			exit(1);
		}

		result = readStripeData(stripeFooter, stripeFooterOffset - stripe->datalength, &structReader,
				orcFileName);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe data\n");
			exit(1);
		}

		result = printAllData(outputFile, &structReader, stripe->numberofrows);
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
