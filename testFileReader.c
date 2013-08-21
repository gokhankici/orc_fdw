#include <stdio.h>
#include "fileReader.h"
#include "recordReader.h"
#include "orcUtil.h"

int readAllData(FieldReader* fieldReader, int noOfRows);
int printAllData(FILE* file, FieldReader* structReader, int noOfRows);

int readAllData(FieldReader* fieldReader, int noOfRows)
{
	StructFieldReader* structFieldReader = NULL;
	int rowNo = 0;
	int columnNo = 0;
	Field field;
	int length = 0;
	int isNull = 0;

	structFieldReader = (StructFieldReader*) fieldReader->fieldReader;

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structFieldReader->noOfFields; ++columnNo)
		{
			fieldReader = structFieldReader->fields[columnNo];
			if (!fieldReader->required)
			{
				continue;
			}

			isNull = FieldReaderRead(fieldReader, &field, &length);
			if (isNull == 0 && fieldReader->kind == FIELD_TYPE__KIND__LIST)
			{
				freeMemory(field.list);
				freeMemory(field.isItemNull);
				freeMemory(field.listItemSizes);
			}
		}
	}

	return 0;
}

int printAllData(FILE* file, FieldReader* fieldReader, int noOfRows)
{
	StructFieldReader* structFieldReader = NULL;
	int rowNo = 0;
	int columnNo = 0;
	Field field;
	int length = 0;
	int listLength = 0;
	int isNull = 0;
	int iterator = 0;
	FieldType__Kind listItemKind = 0;
	if (file == NULL)
	{
		fprintf(stderr, "Cannot open file to read.");
		return 1;
	}

	structFieldReader = (StructFieldReader*) fieldReader->fieldReader;

	for (rowNo = 0; rowNo < noOfRows; rowNo++)
	{
		for (columnNo = 0; columnNo < structFieldReader->noOfFields; ++columnNo)
		{
			fieldReader = structFieldReader->fields[columnNo];
			if (!fieldReader->required)
			{
				fprintf(file, "-|");
				continue;
			}

			isNull = FieldReaderRead(fieldReader, &field, &length);
			if (isNull == 1)
			{
				fprintf(file, "(NULL)|");
			}
			else if (isNull == 0)
			{
				if (fieldReader->kind == FIELD_TYPE__KIND__LIST)
				{
					listLength = length;
					listItemKind = ((ListFieldReader*) fieldReader->fieldReader)->itemReader.kind;
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
							PrintFieldValue(file, &field.list[0], listItemKind, length);
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
							PrintFieldValue(file, &field.list[iterator], listItemKind, length);
						}
					}
					fprintf(file, "]|");
					freeMemory(field.list);
					freeMemory(field.isItemNull);
					freeMemory(field.listItemSizes);
				}
				else
				{
					PrintFieldValue(file, &field.value, fieldReader->kind, length);
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
	FILE* orcFile = NULL;
	FILE* outputFile = NULL;
	FieldReader *fieldReader = NULL;
	PostScript *postScript = NULL;
	Footer *footer = NULL;
	StripeInformation* stripe = NULL;
	StripeFooter* stripeFooter = NULL;
	CompressionParameters compressionParameters;
	long psOffset = 0;
	long footerSize = 0;
	int result = 0;
	int iterator = 0;
	int noOfFields = 0;
	int stripeIterator = 0;
	PostgresQueryInfo *query = NULL;

	if (argc < 2 || argc > 3)
	{
		printf("usage: testFileReader inputFile [outputFile]\n");
		return 1;
	}
	orcFileName = argv[1];
	orcFile = MyOpenFile(orcFileName, "r");

	if (argc == 2)
	{
		outputFile = stdout;
	}
	else
	{
		outputFileName = argv[2];
		outputFile = MyOpenFile(outputFileName, "w");
	}

	if (outputFile == NULL)
	{
		fprintf(stderr, "Cannot open file to write\n");
		return 1;
	}

	postScript = PostScriptInit(orcFile, &psOffset, &compressionParameters);
	if (postScript == NULL)
	{
		fprintf(stderr, "Error while reading postscript\n");
		exit(1);
	}
	footerSize = postScript->footerlength;

	/* read the file footer */
	footer = FileFooterInit(orcFile, psOffset - footerSize, footerSize, &compressionParameters);
	if (footer == NULL)
	{
		fprintf(stderr, "Error while reading file footer\n");
		exit(1);
	}

	noOfFields = footer->types[0]->n_subtypes;

	query = malloc(sizeof(PostgresQueryInfo));
	query->selectedColumns = malloc(sizeof(PostgresColumnInfo) * noOfFields);
	query->noOfSelectedColumns = noOfFields;

	for (iterator = 0; iterator < noOfFields; ++iterator)
	{
		query->selectedColumns[iterator].columnIndex = iterator;
	}

	fieldReader = alloc(sizeof(FieldReader));
	result = FieldReaderAllocate(fieldReader, footer, query);

	if (result)
	{
		fprintf(stderr, "Error while initializing field reader\n");
		exit(1);
	}

	for (stripeIterator = 0; stripeIterator < footer->n_stripes; stripeIterator++)
	{
		stripe = footer->stripes[stripeIterator];
		stripeFooter = StripeFooterInit(orcFile, stripe, &compressionParameters);
		if (stripeFooter == NULL)
		{
			fprintf(stderr, "Error while reading stripe footer\n");
			exit(1);
		}

		result = FieldReaderInit(fieldReader, orcFile, stripe, stripeFooter, &compressionParameters);
		if (result)
		{
			fprintf(stderr, "Error while reading stripe data\n");
			exit(1);
		}

//		result = printAllData(outputFile, fieldReader, stripe->numberofrows);
		result = readAllData(fieldReader, stripe->numberofrows);
		if (result)
		{
			fprintf(stderr, "Error while printing values\n");
			exit(1);
		}

		stripe_footer__free_unpacked(stripeFooter, NULL);
	}

	FieldReaderFree(fieldReader);

	if (outputFile != stdout)
	{
		MyCloseFile(outputFile);
	}

	MyCloseFile(orcFile);

	post_script__free_unpacked(postScript, NULL);
	footer__free_unpacked(footer, NULL);

	return 0;
}
