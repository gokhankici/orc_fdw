#include <zlib.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

#define isComplexType(type) (type == TYPE__KIND__LIST || type == TYPE__KIND__STRUCT || type == TYPE__KIND__MAP)

int readPostscript(FILE* orcFile, PostScript ** postScriptPtr, int* postScriptSizePtr)
{
	int isByteRead = 0;
	char c = 0;
	size_t msg_len = 0;
	uint8_t postScriptBuffer[MAX_POSTSCRIPT_SIZE];
	int psSize = 0;

	fseek(orcFile, -1, SEEK_END);
	isByteRead = fread(&c, sizeof(char), 1, orcFile);

	if (!isByteRead)
	{
		fprintf(stderr, "Error while reading the last byte\n");
		return 1;
	}

	psSize = ((int) c) & 0xFF;

	/* read postscript into the buffer */
	fseek(orcFile, -1 - psSize, SEEK_END);
	msg_len = fread(postScriptBuffer, 1, psSize, orcFile);
	if (msg_len != psSize)
	{
		fprintf(stderr, "Error while reading postscript from file\n");
		return 1;
	}

	/* unpack the message using protobuf-c. */
	*postScriptPtr = post_script__unpack(NULL, msg_len, postScriptBuffer);
	if (*postScriptPtr == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return 1;
	}

	*postScriptSizePtr = psSize;
	return 0;
}

int readFileFooter(FILE* orcFile, Footer** footer, int footerOffsetFromEnd, long footerSize)
{
	int msg_len = 0;
	uint8_t* compressedFooterBuffer = NULL;
	uInt uncompressSize = 0;
	uint8_t *uncompressedFooterBuffer = NULL;
	*footer = NULL;

	/* read the file footer */
	fseek(orcFile, -footerOffsetFromEnd, SEEK_END);
	compressedFooterBuffer = malloc(footerSize);
	msg_len = fread(compressedFooterBuffer, 1, footerSize, orcFile);
	if (msg_len != footerSize)
	{
		printf("Error while reading footer from file\n");
		return 1;
	}

//	int result = inf(compressedFooterBuffer, footerSize, &uncompressedFooterBuffer, &uncompressSize);
	uncompressedFooterBuffer = compressedFooterBuffer;
	uncompressSize = footerSize;

	/* unpack the message using protobuf-c. */
	*footer = footer__unpack(NULL, uncompressSize, uncompressedFooterBuffer);
	if (*footer == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return 1;
	}

	free(compressedFooterBuffer);

	return 0;
}

int readStripeFooter(FILE* orcFile, StripeFooter** stripeFooter, StripeInformation* stripeInfo)
{
	uint8_t *stripeFooterBuffer = malloc(stripeInfo->footerlength);
	long stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	*stripeFooter = NULL;

	fseek(orcFile, stripeFooterOffset, SEEK_SET);
	fread(stripeFooterBuffer, stripeInfo->footerlength, 1, orcFile);
	*stripeFooter = stripe_footer__unpack(NULL, stripeInfo->footerlength, stripeFooterBuffer);

	free(stripeFooterBuffer);

	return 0;
}

int initStripeReader(Footer* footer, StructReader* reader)
{
	int i = 0;
	int j = 0;
	Type** types = footer->types;
	Type* root = footer->types[0];
	Type* type = NULL;
	PrimitiveReader* primitiveReader = NULL;
	ListReader* listReader = NULL;
	Reader* field = NULL;
	Reader* listItemReader = NULL;
	reader->noOfFields = root->n_subtypes;
	reader->fields = malloc(sizeof(Reader*) * reader->noOfFields);

	for (i = 0; i < reader->noOfFields; ++i)
	{
		reader->fields[i] = malloc(sizeof(Reader));
		field = reader->fields[i];
		field->columnNo = root->subtypes[i];
		type = types[root->subtypes[i]];
		field->kind = type->kind;

		if (field->kind == TYPE__KIND__LIST)
		{
			field->fieldReader = malloc(sizeof(ListReader));
			field->hasPresentBitReader = 0;
			field->presentBitReader.stream = NULL;

			listReader = field->fieldReader;
			listReader->lengthReader.stream = NULL;

			/* initialize list item reader */
			listItemReader = &listReader->itemReader;
			listItemReader->hasPresentBitReader = 0;
			listItemReader->presentBitReader.stream = NULL;
			listItemReader->columnNo = type->subtypes[0];
			listItemReader->kind = types[listItemReader->columnNo]->kind;

			if (isComplexType(listItemReader->kind))
			{
				/* only list of primitive types is supported */
				return 1;
			}

			listItemReader->fieldReader = malloc(sizeof(PrimitiveReader));
			primitiveReader = listItemReader->fieldReader;

			for (j = 0; j < MAX_STREAM_COUNT; ++j)
			{
				primitiveReader->readers[j].stream = NULL;
				primitiveReader->readers[j].streamLength = 0;
			}
		}
		else
		{
			field->fieldReader = malloc(sizeof(PrimitiveReader));
			field->hasPresentBitReader = 0;
			field->presentBitReader.stream = NULL;

			primitiveReader = field->fieldReader;
			primitiveReader->dictionary = NULL;
			primitiveReader->dictionarySize = 0;
			primitiveReader->wordLength = NULL;

			for (j = 0; j < MAX_STREAM_COUNT; ++j)
			{
				primitiveReader->readers[j].stream = NULL;
				primitiveReader->readers[j].streamLength = 0;
			}
		}
	}
	return 0;
}

int readDataStream(StreamReader* streamReader, Type__Kind streamKind, FILE* orcFile, long offset, long length)
{
	uint8_t* buffer = NULL;

	if (streamReader->stream != NULL)
	{
		free(streamReader->stream);
	}

	buffer = malloc(length);
	fseek(orcFile, offset, SEEK_SET);
	fread(buffer, length, 1, orcFile);

	return initStreamReader(streamKind, streamReader, buffer, length);
}

int readStripeData(StripeFooter* stripeFooter, long dataOffset, StructReader* structReader, FILE* orcFile)
{
	Stream* stream = NULL;
	Reader** readers = structReader->fields;
	Reader* reader = NULL;
	PrimitiveReader* primitiveReader = NULL;
	ListReader* listReader = NULL;
	int streamNo = 0;
	int fieldNo = 0;
	int result = 0;
	long currentOffset = dataOffset;
	int noOfDataStreams = 0;
	int dataStreamIterator = 0;
	Type__Kind streamKind = 0;
	ColumnEncoding* columnEncoding = NULL;

	stream = stripeFooter->streams[streamNo];
	while (stream->kind == STREAM__KIND__ROW_INDEX)
	{
		/* skip index data for now */
		stream = stripeFooter->streams[++streamNo];
	}

	while (streamNo < stripeFooter->n_streams)
	{
		assert(stream->kind != STREAM__KIND__ROW_INDEX);

		reader = readers[fieldNo++];

		/* skip the not required columns */
		if (reader == NULL)
		{
			continue;
		}

		while (reader->columnNo > stream->column)
		{
			/* skip columns that doesn't required for reading */
			stream = stripeFooter->streams[++streamNo];
		}

		if (reader->kind == TYPE__KIND__LIST)
		{
			/* if the type is list, first check for present stream as always */
			reader->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;
			if (reader->hasPresentBitReader)
			{
				result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFile, currentOffset,
						stream->length);
				if (result)
				{
					/* cannot read the data stream correctly */
					return -1;
				}
				currentOffset += stream->length;
				stream = stripeFooter->streams[++streamNo];
			}

			/* then read the length stream into the reader */
			streamKind = getStreamKind(reader->kind, 0);
			listReader = reader->fieldReader;
			result = readDataStream(&listReader->lengthReader, streamKind, orcFile, currentOffset, stream->length);
			if (result)
			{
				return result;
			}
			currentOffset += stream->length;
			stream = stripeFooter->streams[++streamNo];

			/* and finally set the item of the list as the reader and continue */
			reader = &listReader->itemReader;
		}

		columnEncoding = stripeFooter->columns[reader->columnNo];
		reader->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;
		if (reader->hasPresentBitReader)
		{
			result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFile, currentOffset,
					stream->length);
			if (result)
			{
				/* cannot read the data stream correctly */
				return -1;
			}
			currentOffset += stream->length;
			stream = stripeFooter->streams[++streamNo];
		}

		primitiveReader = reader->fieldReader;
		freePrimitiveReader(primitiveReader);
		if (reader->kind == TYPE__KIND__STRING)
		{
			primitiveReader->dictionarySize = columnEncoding->dictionarysize;
		}

		noOfDataStreams = getStreamCount(reader->kind);
		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = getStreamKind(reader->kind, dataStreamIterator);
			result = readDataStream(&primitiveReader->readers[dataStreamIterator], streamKind, orcFile, currentOffset,
					stream->length);
			if (result)
			{
				return result;
			}

			currentOffset += stream->length;
			stream = stripeFooter->streams[++streamNo];
		}
	}

	return 0;
}
