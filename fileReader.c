#include <zlib.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "InputStream.h"
#include "util.h"

CompressionParameters compressionParameters =
{ 0 };

int readPostscript(char* orcFileName, PostScript ** postScriptPtr, long* postScriptOffset)
{
	FILE* orcFile = fopen(orcFileName, "r");
	int isByteRead = 0;
	char c = 0;
	size_t msg_len = 0;
	uint8_t postScriptBuffer[MAX_POSTSCRIPT_SIZE];
	int psSize = 0;

	if (orcFile == NULL)
	{
		return 1;
	}

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
	*postScriptOffset = ftell(orcFile);
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

	compressionParameters.compressionBlockSize =
			(*postScriptPtr)->has_compressionblocksize ? (*postScriptPtr)->compressionblocksize : 0;
	compressionParameters.compressionKind =
			(*postScriptPtr)->has_compression ? (*postScriptPtr)->compression : 0;

	fclose(orcFile);
	return 0;
}

int readFileFooter(char* orcFileName, Footer** footer, int footerOffset, long footerSize)
{
	char* uncompressedFooter = NULL;
	int uncompressedFooterSize = 0;
	int result = 0;
	CompressedFileStream* stream = CompressedFileStream_init(orcFileName, footerOffset,
			footerOffset + footerSize, compressionParameters.compressionBlockSize,
			compressionParameters.compressionKind);
	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return 1;
	}
	*footer = NULL;

	result = CompressedFileStream_readRemaining(stream, &uncompressedFooter, &uncompressedFooterSize);
	if (result)
	{
		fprintf(stderr, "Error while uncompressing file footer\n");
		return 1;
	}

	*footer = footer__unpack(NULL, uncompressedFooterSize, (uint8_t*) uncompressedFooter);
	/* unpack the message using protobuf-c. */
	if (*footer == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return 1;
	}

	CompressedFileStream_free(stream);

	return 0;
}

int readStripeFooter(char* orcFileName, StripeFooter** stripeFooter, StripeInformation* stripeInfo)
{
	char *stripeFooterBuffer = NULL;
	int uncompressedStripeFooterSize = 0;
	long stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	int result = 0;
	CompressedFileStream *stream = CompressedFileStream_init(orcFileName, stripeFooterOffset,
			stripeFooterOffset + stripeInfo->footerlength, compressionParameters.compressionBlockSize,
			compressionParameters.compressionKind);
	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return 1;
	}
	*stripeFooter = NULL;

	result = CompressedFileStream_readRemaining(stream, &stripeFooterBuffer, &uncompressedStripeFooterSize);
	if (result)
	{
		fprintf(stderr, "Error while uncompressing file footer");
	}

	*stripeFooter = stripe_footer__unpack(NULL, uncompressedStripeFooterSize, (uint8_t*) stripeFooterBuffer);
	if (*stripeFooter == NULL)
	{
		fprintf(stderr, "error while unpacking stripe footer\n");
		return 1;
	}

	CompressedFileStream_free(stream);

	return 0;
}

int initStripeReader(Footer* footer, StructReader* reader, char* selectedFields)
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
		field->hasPresentBitReader = 0;
		field->presentBitReader.stream = NULL;

		if (!selectedFields[i])
		{
			/* if current field is not needed, continue */
			field->required = 0;
			field->fieldReader = NULL;
			continue;
		}
		field->required = 1;

		if (field->kind == TYPE__KIND__LIST)
		{
			field->fieldReader = malloc(sizeof(ListReader));

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

int readDataStream(StreamReader* streamReader, Type__Kind streamKind, char* orcFile, long offset, long length,
		CompressionParameters* parameters)
{
	if (streamReader->stream != NULL)
	{
		if (CompressedFileStream_free(streamReader->stream))
		{
			fprintf(stderr, "Error deleting previous compressed file stream\n");
			return 1;
		}
		streamReader->stream = NULL;
	}

	return initStreamReader(streamKind, streamReader, orcFile, offset, length, parameters);
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

	while (fieldNo < structReader->noOfFields)
	{
//		assert(stream->kind != STREAM__KIND__ROW_INDEX);

		reader = readers[fieldNo++];

		/* skip the not required columns */
		if (!reader->required)
		{
			continue;
		}

		while (reader->columnNo > stream->column)
		{
			/* skip columns that doesn't required for reading */
			currentOffset += stream->length;
			stream = stripeFooter->streams[++streamNo];
		}

		if (reader->kind == TYPE__KIND__LIST)
		{
			/* if the type is list, first check for present stream as always */
			reader->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;
			if (reader->hasPresentBitReader)
			{
				result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName,
						currentOffset, currentOffset + stream->length, &compressionParameters);
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
			result = readDataStream(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName,
					currentOffset, currentOffset + stream->length, &compressionParameters);
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
