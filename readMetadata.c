#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

/* TODO remove these two later */
long totalBytesRead = 0;
long totalUncompressedBytes = 0;

char* getTypeKindName(Type__Kind kind);
void printType(Type** types, char* typeName, int typeIndex, int depth);
void printStatistics(ColumnStatistics* statistics, Type* type);
char* getStreamKindName(Stream__Kind kind);
char* getEncodingName(ColumnEncoding__Kind kind);
void printDataInHex(uint8_t* data, int length);
void printStripeInfo(StripeFooter* stripeFooter, unsigned long offset);

char* getTypeKindName(Type__Kind kind)
{
	if (kind == TYPE__KIND__BOOLEAN)
		return "BOOLEAN";
	else if (kind == TYPE__KIND__BYTE)
		return "BYTE";
	else if (kind == TYPE__KIND__SHORT)
		return "SHORT";
	else if (kind == TYPE__KIND__INT)
		return "INT";
	else if (kind == TYPE__KIND__LONG)
		return "LONG";
	else if (kind == TYPE__KIND__FLOAT)
		return "FLOAT";
	else if (kind == TYPE__KIND__DOUBLE)
		return "DOUBLE";
	else if (kind == TYPE__KIND__STRING)
		return "STRING";
	else if (kind == TYPE__KIND__BINARY)
		return "BINARY";
	else if (kind == TYPE__KIND__TIMESTAMP)
		return "TIMESTAMP";
	else if (kind == TYPE__KIND__LIST)
		return "LIST";
	else if (kind == TYPE__KIND__MAP)
		return "MAP";
	else if (kind == TYPE__KIND__STRUCT)
		return "STRUCT";
	else if (kind == TYPE__KIND__UNION)
		return "UNION";
	else if (kind == TYPE__KIND__DECIMAL)
		return "DECIMAL";
	else
		return NULL;
}

void printType(Type** types, char* typeName, int typeIndex, int depth)
{
	Type* type = types[typeIndex];
	Type* subType = NULL;
	Type__Kind kind = 0;
	int typeIterator = 0;
	int subTypeIndex = 0;
	int depthIterator = 0;
	char* subTypeName = NULL;

	for (depthIterator = 0; depthIterator < depth; ++depthIterator)
	{
		printf("    ");
	}
	printf("%2d) Name : %-8s | Kind : %s\n", typeIndex, typeName, getTypeKindName(type->kind));
	for (typeIterator = 0; typeIterator < type->n_subtypes; ++typeIterator)
	{
		subTypeIndex = type->subtypes[typeIterator];
		subType = types[subTypeIndex];
		kind = subType->kind;
		subTypeName = (typeIterator < type->n_fieldnames) ? type->fieldnames[typeIterator] : "";

		switch (kind)
		{
		case TYPE__KIND__STRUCT:
			printType(types, subTypeName, subTypeIndex, depth + 1);
			break;
		case TYPE__KIND__UNION:
			printType(types, subTypeName, subTypeIndex, depth + 1);
			break;
		case TYPE__KIND__LIST:
			/* print the name of the list */
			for (depthIterator = 0; depthIterator < depth + 1; ++depthIterator)
			{
				printf("    ");
			}
			printf("%2d) Name : %-8s | Kind : %s\n", subTypeIndex, subTypeName, getTypeKindName(kind));

			/* print the child of the list (which is the type of the list) */
			printType(types, "element", subTypeIndex + 1, depth + 2);
			break;
		case TYPE__KIND__MAP:
			/* print the name of the map */
			for (depthIterator = 0; depthIterator < depth + 1; ++depthIterator)
			{
				printf("    ");
			}
			printf("%2d) Name : %-8s | Kind : %s\n", subTypeIndex, subTypeName, getTypeKindName(kind));

			/* print the key of the map */
			printType(types, "key", subType->subtypes[0], depth + 2);
			/* print the value of the map */
			printType(types, "value", subType->subtypes[1], depth + 2);
			break;
		default:
			for (depthIterator = 0; depthIterator < depth + 1; ++depthIterator)
			{
				printf("    ");
			}
			printf("%2d) Name : %-8s | Kind : %s\n", subTypeIndex, subTypeName, getTypeKindName(kind));
			break;
		}
	}

}

void printStatistics(ColumnStatistics* statistics, Type* type)
{
	IntegerStatistics *intStatistics = NULL;
	DoubleStatistics *doubleStatistics = NULL;
	StringStatistics *stringStatistics = NULL;
	BucketStatistics *bucketStatistics = NULL;
	DecimalStatistics* decimalStatistics = NULL;

	switch (type->kind)
	{
	case TYPE__KIND__BYTE:
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		intStatistics = statistics->intstatistics;
		if (intStatistics)
		{
			printf("    min: %ld | max: %ld | sum: %ld\n", intStatistics->minimum, intStatistics->maximum,
					intStatistics->sum);
		}
		break;
	case TYPE__KIND__FLOAT:
	case TYPE__KIND__DOUBLE:
		doubleStatistics = statistics->doublestatistics;
		if (doubleStatistics)
		{
			printf("    min: %lf | max: %lf | sum: %lf\n", doubleStatistics->minimum, doubleStatistics->maximum,
					doubleStatistics->sum);
		}
		break;
	case TYPE__KIND__STRING:
		stringStatistics = statistics->stringstatistics;
		if (stringStatistics)
		{
			printf("    min: %s | max: %s\n", stringStatistics->minimum, stringStatistics->maximum);
		}
		break;
	case TYPE__KIND__BOOLEAN:
		bucketStatistics = statistics->bucketstatistics;
		if (bucketStatistics)
		{
			int i;
			for (i = 0; i < bucketStatistics->n_count; ++i)
			{
				printf("    count[%d]: %ld\n", i, bucketStatistics->count[i]);
			}
		}
		break;
	case TYPE__KIND__DECIMAL:
		decimalStatistics = statistics->decimalstatistics;
		if (decimalStatistics)
		{
			printf("    min: %s | max: %s | sum: %s", decimalStatistics->minimum, decimalStatistics->maximum,
					decimalStatistics->sum);
		}
		break;
	default:
		/* to display which type has uses which statistics type */
		if (statistics->intstatistics)
		{
			printf("    %s -> int\n", getTypeKindName(type->kind));
		}
		else if (statistics->doublestatistics)
		{
			printf("    %s -> double\n", getTypeKindName(type->kind));
		}
		else if (statistics->bucketstatistics)
		{
			printf("    %s -> bucket\n", getTypeKindName(type->kind));
		}
		else if (statistics->decimalstatistics)
		{
			printf("    %s -> decimal\n", getTypeKindName(type->kind));
		}

		break;
	}
}

char* getStreamKindName(Stream__Kind kind)
{
	if (kind == STREAM__KIND__PRESENT)
		return "PRESENT";
	else if (kind == STREAM__KIND__DATA)
		return "DATA";
	else if (kind == STREAM__KIND__LENGTH)
		return "LENGTH";
	else if (kind == STREAM__KIND__DICTIONARY_DATA)
		return "DICTIONARY_DATA";
	else if (kind == STREAM__KIND__DICTIONARY_COUNT)
		return "DICTIONARY_COUNT";
	else if (kind == STREAM__KIND__SECONDARY)
		return "SECONDARY";
	else if (kind == STREAM__KIND__ROW_INDEX)
		return "ROW_INDEX";
	else
		return "";
}

char* getEncodingName(ColumnEncoding__Kind kind)
{
	if (kind == COLUMN_ENCODING__KIND__DIRECT)
		return "DIRECT";
	else if (kind == COLUMN_ENCODING__KIND__DICTIONARY)
		return "DICTIONARY";
	else
		return "";
}

void printDataInHex(uint8_t* data, int length)
{
	int i = 0;
	for (i = 0; i < length; ++i)
	{
		printf(" %.2X", (char) data[i] & 0xFF);
	}
	printf("\n");
}

void printStripeInfo(StripeFooter* stripeFooter, unsigned long offset)
{
	int index = 0;
	Stream* stream = NULL;
	ColumnEncoding* columnEncoding = NULL;
	long streamLength = 0;

	for (index = 0; index < stripeFooter->n_streams; ++index)
	{
		stream = stripeFooter->streams[index];
		columnEncoding = stripeFooter->columns[stream->column];
		streamLength = stream->length;

		printf("Column %-2d | Stream %-2d | Offset: %ld\n", stream->column, index, offset);
		printf("    Stream kind: %-15s | Stream length: %-3ld\n", getStreamKindName(stream->kind), streamLength);
		printf("    Encoding type: %-13s | Dict. size: %d\n", getEncodingName(columnEncoding->kind),
				columnEncoding->has_dictionarysize ? columnEncoding->dictionarysize : 0);

		offset += streamLength;
	}
}

int main(int argc, const char * argv[])
{
	PostScript *postScript = NULL;
	Footer *footer = NULL;
	char* orcFileName = NULL;
	long psOffset = 0;
	long footerSize = 0;
	uint32_t *versionPointer = NULL;
	StripeInformation** stripes = NULL;
	StripeInformation* stripe = NULL;
	int noOfStripes = 0;
	int index = 0;
	Type **types;
	int noOfUserMetadataItems = 0;
	ColumnStatistics* statistics;
	StripeFooter* stripeFooter;
	CompressionParameters compressionParameters;

	if (argc != 2)
	{
		printf("Usage: readMetadata fileName\n");
		return 1;
	}

	orcFileName = (char*) argv[1];

	postScript = PostScriptInit(orcFileName, &psOffset, &compressionParameters);
	if (postScript == NULL)
	{
		fprintf(stderr, "Error while reading postscript\n");
		exit(1);
	}
	footerSize = postScript->footerlength;

	/* display the postscript's fields. */
	footerSize = postScript->footerlength;
	printf("Footer length : %ld\n", footerSize);
	printf("Compression kind : %d\n", postScript->compression);
	printf("Compression block size : %ld\n", postScript->compressionblocksize);
	if (postScript->n_version == 2)
	{
		versionPointer = postScript->version;
		printf("Version : %d.%d\n", *versionPointer, *(versionPointer + 1));
	}
	printf("Magic : %s\n", postScript->magic);

	/* read the file footer */
	footer = FileFooterInit(orcFileName, psOffset - footerSize, footerSize, &compressionParameters);
	if (footer == NULL)
	{
		fprintf(stderr, "Error while reading file footer\n");
		exit(1);
	}

	stripes = footer->stripes;
	noOfStripes = footer->n_stripes;
	types = footer->types;

	printf("========================================\n");

	/* print footer info */
	printf("Header length : %ld\n", footer->headerlength);
	printf("Content length : %ld\n", footer->contentlength);
	for (index = 0; index < noOfStripes; ++index)
	{
		stripe = stripes[index];
		printf("Stripe %d\n", index);
		printf("\tOffset : %ld\n", stripe->offset);
		printf("\tIndex length : %ld\n", stripe->indexlength);
		printf("\tData length : %ld\n", stripe->datalength);
		printf("\tFooter length : %ld\n", stripe->footerlength);
		printf("\tNumber of rows : %ld\n", stripe->numberofrows);
	}
	printf("Total # rows : %ld\n", footer->numberofrows);
	printf("Row index stride : %d\n", footer->rowindexstride);

	printf("========================================\n");

	/* print types */
	printf("No of types : %d\n", (int) footer->n_types);
	printType(types, "root", 0, 0);

	printf("========================================\n");

	/* print user metadata items */
	noOfUserMetadataItems = footer->n_metadata;
	for (index = 0; index < noOfUserMetadataItems; ++index)
	{
		/* TODO does extraction of metadata value correct? */
		printf("%s : %s", footer->metadata[index]->name, (char*) footer->metadata[index]->value.data);
	}

	printf("========================================\n");

	/* print column statistics */
	for (index = 0; index < footer->n_statistics; ++index)
	{
		statistics = footer->statistics[index];
		printf("Column %d statistics\n", index);
		if (statistics->has_numberofvalues)
		{
			printf("    # values : %ld\n", statistics->numberofvalues);
			printStatistics(statistics, types[index]);
		}
	}

	printf("========================================\n");

	for (index = 0; index < noOfStripes; ++index)
	{
		stripe = stripes[index];
		stripeFooter = StripeFooterInit(orcFileName, stripe, &compressionParameters);
		if (stripeFooter == NULL)
		{
			fprintf(stderr, "Error while reading stripe footer\n");
			return 1;
		}

		printf("== STRIPE %d ==\n", index);
		printStripeInfo(stripeFooter, stripe->offset);
	}

	/* Free the unpacked message */
	post_script__free_unpacked(postScript, NULL);
	footer__free_unpacked(footer, NULL);

	return 0;
}
