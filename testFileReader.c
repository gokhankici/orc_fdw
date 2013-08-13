#include <stdio.h>
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"
int readAllData(StructReader* structReader, int noOfRows)
{
	Reader* reader = NULL;
	int rowNo = 0;
	int columnNo = 0;
	Field field;
	int length = 0;
	int isNull = 0;

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structReader->noOfFields; ++columnNo)
		{
			reader = structReader->fields[columnNo];
			if (!reader->required)
			{
				continue;
			}

			isNull = readField(reader, &field, &length);
			if (isNull == 0 && reader->kind == TYPE__KIND__LIST)
			{
				free(field.list);
				free(field.isItemNull);
				free(field.listItemSizes);
			}
		}
	}

	return 0;
}
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
			if (!reader->required)
			{
				fprintf(file, "-|");
				continue;
			}

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
					if (listLength > 0)
					{
						length = field.listItemSizes ? field.listItemSizes[0] : 0;
						isNull = field.isItemNull[0];
						if (isNull)
						{
							fprintf(file, "(NULL)");
						}
						else
						{
							printFieldValue(file, &field.list[0], listItemKind, length);
						}
					}
					for (iterator = 1; iterator < listLength; ++iterator)
					{
						length = field.listItemSizes ? field.listItemSizes[iterator] : 0;
						isNull = field.isItemNull[iterator];
						if (isNull)
						{
							fprintf(file, ",(NULL)");
						}
						else
						{
							fprintf(file, ",");
							printFieldValue(file, &field.list[iterator], listItemKind, length);
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

long totalBytesRead;
long totalUncompressedBytes;

int main(int argc, char **argv)
{

	char *orcFileName = NULL;
	char *outputFileName = NULL;
	FILE* outputFile = NULL;

	StructReader *structReader = NULL;
	PostScript *postScript = NULL;
	Footer *footer = NULL;
	StripeInformation* stripe = NULL;
	StripeFooter* stripeFooter = NULL;
	long stripeFooterOffset = 0;
	long psOffset = 0;
	long footerSize = 0;
	int result = 0;
	int iterator = 0;
	int noOfFields = 0;
	char* selectedFields = 0;

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

	postScript = readPostscript(orcFileName, &psOffset);
	if (postScript == NULL)
	{
		fprintf(stderr, "Error while reading postscript\n");
		exit(1);
	}
	footerSize = postScript->footerlength;

	/* read the file footer */
	footer = readFileFooter(orcFileName, psOffset - footerSize, footerSize);
	if (footer == NULL)
	{
		fprintf(stderr, "Error while reading file footer\n");
		exit(1);
	}

	noOfFields = footer->types[0]->n_subtypes;
	selectedFields = malloc(noOfFields);

	for (iterator = 0; iterator < noOfFields; ++iterator)
	{
//		if (iterator == 4 || iterator == 5 || iterator == 6 || iterator == 10)
//		{
//
//			selectedFields[iterator] = 1;
//		}
//		else
//		{
//			selectedFields[iterator] = 0;
//		}

		selectedFields[iterator] = 1;
	}

	structReader = malloc(sizeof(StructReader));

	result = StructReader_allocate(structReader, footer, selectedFields);
	if (result)
	{
		fprintf(stderr, "Error while initializing structure reader\n");
		exit(1);
	}

	totalBytesRead = 0;
	totalUncompressedBytes = 0;

	int i = 0;
	for (i = 0; i < footer->n_stripes; i++)
	{
		stripe = footer->stripes[i];
		stripeFooterOffset = stripe->offset + stripe->datalength
				+ ((stripe->has_indexlength) ? stripe->indexlength : 0);
		stripeFooter = readStripeFooter(orcFileName, stripe);
		if (stripeFooter == NULL)
		{
			fprintf(stderr, "Error while reading stripe footer\n");
			exit(1);
		}

		result = StructReader_init(structReader, orcFileName, stripeFooterOffset - stripe->datalength,
				stripeFooter);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe data\n");
			exit(1);
		}

//		result = printAllData(outputFile, structReader, stripe->numberofrows);
		result = readAllData(structReader, stripe->numberofrows);
		if (result)
		{
			fprintf(stderr, "Error while printing values\n");
			exit(1);
		}

		stripe_footer__free_unpacked(stripeFooter, NULL);
	}

	freeStructReader(structReader);
	if (outputFile != stdout)
		fclose(outputFile);

	printf("Total bytes read from the file: %ld\n", totalBytesRead);
	printf("Uncompressed size of the read data: %ld\n", totalUncompressedBytes);

	post_script__free_unpacked(postScript, NULL);
	footer__free_unpacked(footer, NULL);

	return 0;
}
