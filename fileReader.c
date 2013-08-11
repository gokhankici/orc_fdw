#include <zlib.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "util.h"

CompressionParameters compressionParameters =
{ 0 };

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
	compressionParameters.compressionBlockSize =
			(*postScriptPtr)->has_compressionblocksize ? (*postScriptPtr)->compressionblocksize : 0;
	compressionParameters.compressionKind = (*postScriptPtr)->has_compression ? (*postScriptPtr)->compression : 0;
	return 0;
}

int readFileFooter(char* orcFileName, Footer** footer, int footerOffset, long footerSize)
{
	int msg_len = 0;
	uint8_t* compressedFooterBuffer = NULL;
	int result = 0;
	CompressedFileStream* stream = CompressedFileStream_init(orcFileName, footerOffset, footerOffset + footerSize,
			compressionParameters.compressionBlockSize, compressionParameters.compressionKind);
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

	stream->array = compressedFooterBuffer;
	stream->size = footerSize;
	result = uncompressStream(stream);
	if (result)
	{
		fprintf(stderr, "error while uncompressing file footer stream\n");
		return 1;
	}

	*footer = footer__unpack(NULL, stream->uncompressed->length, stream->uncompressed->buffer);
	/* unpack the message using protobuf-c. */
	if (*footer == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return 1;
	}

	freeCompressedStream(stream, 0);

	return 0;
}

int readStripeFooter(FILE* orcFile, StripeFooter** stripeFooter, StripeInformation* stripeInfo)
{
	CompressedStream *stream = initCompressedStream(&compressionParameters);
	uint8_t *stripeFooterBuffer = malloc(stripeInfo->footerlength);
	long stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	int result = 0;

	*stripeFooter = NULL;

	fseek(orcFile, stripeFooterOffset, SEEK_SET);
	result = fread(stripeFooterBuffer, 1, stripeInfo->footerlength, orcFile);
	if (result != stripeInfo->footerlength)
	{
		fprintf(stderr, "Error while reading stripe footer.\n");
		return 1;
	}

	stream->array = stripeFooterBuffer;
	stream->size = stripeInfo->footerlength;
	result = uncompressStream(stream);
	if (result)
	{
		fprintf(stderr, "error while uncompressing stripe footer stream\n");
		return 1;
	}

	*stripeFooter = stripe_footer__unpack(NULL, stream->uncompressed->length, stream->uncompressed->buffer);
	if (*stripeFooter == NULL)
	{
		fprintf(stderr, "error while unpacking stripe footer\n");
		return 1;
	}

	freeCompressedStream(stream, 0);

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
			}
		}
	}
	return 0;
}

int readDataStream(StreamReader* streamReader, Type__Kind streamKind, char* orcFile, long offset, long length)
{
	uint8_t* buffer = NULL;
	uint8_t* uncompressedData = NULL;
	long dataLength = 0;
	CompressedStream *stream = initCompressedStream(&compressionParameters);
	int result = 0;

	if (streamReader->stream != NULL)
	{
		free(streamReader->stream);
	}

	buffer = malloc(length);
	fseek(orcFile, offset, SEEK_SET);
	fread(buffer, length, 1, orcFile);

	stream->array = buffer;
	stream->size = length;
	result = uncompressStream(stream);
	if (result)
	{
		fprintf(stderr, "error while uncompressing data stream\n");
		return 1;
	}

	uncompressedData = stream->uncompressed->buffer;
	dataLength = stream->uncompressed->length;

	freeCompressedStream(stream, 1);

	return initStreamReader(streamKind, streamReader, "", offset, offset + length, &compressionParameters);
}

int readStripeData(StripeFooter* stripeFooter, long dataOffset, StructReader* structReader, char* orcFileName)
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
				result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName, currentOffset,
						currentOffset + stream->length, &compressionParameters);
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
			result = readDataStream(&listReader->lengthReader, streamKind, orcFileName, currentOffset,
					currentOffset + stream->length, &compressionParameters);
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
			result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName, currentOffset,
					currentOffset + stream->length, &compressionParameters);
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
			result = readDataStream(&primitiveReader->readers[dataStreamIterator], streamKind, orcFileName,
					currentOffset, currentOffset + stream->length, &compressionParameters);
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
