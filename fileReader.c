#include <zlib.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "InputStream.h"
#include "util.h"

/* global compression parameters */
CompressionParameters compressionParameters =
{ 0 };

/**
 * Reads the postscript from the orc file and returns the postscript. Stores its offset to parameter.
 *
 * @param orcFileName name of the orc file
 * @param postScriptOffset pointer to store the size of the postscript
 *
 * @return NULL for failure, non-NULL for success
 */
PostScript* readPostscript(char* orcFileName, long* postScriptOffset)
{
	PostScript* postScript = NULL;
	FILE* orcFile = fopen(orcFileName, "r");
	int isByteRead = 0;
	char c = 0;
	size_t msg_len = 0;
	uint8_t postScriptBuffer[MAX_POSTSCRIPT_SIZE];
	int psSize = 0;

	if (orcFile == NULL)
	{
		return NULL;
	}

	fseek(orcFile, -1, SEEK_END);
	isByteRead = fread(&c, sizeof(char), 1, orcFile);

	if (!isByteRead)
	{
		fprintf(stderr, "Error while reading the last byte\n");
		return NULL;
	}

	psSize = ((int) c) & 0xFF;

	/* read postscript into the buffer */
	fseek(orcFile, -1 - psSize, SEEK_END);
	*postScriptOffset = ftell(orcFile);
	msg_len = fread(postScriptBuffer, 1, psSize, orcFile);

	if (msg_len != psSize)
	{
		fprintf(stderr, "Error while reading postscript from file\n");
		return NULL;
	}

	/* unpack the message using protobuf-c. */
	postScript = post_script__unpack(NULL, msg_len, postScriptBuffer);

	if (postScript == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return NULL;
	}

	compressionParameters.compressionBlockSize =
			postScript->has_compressionblocksize ? postScript->compressionblocksize : 0;
	compressionParameters.compressionKind = postScript->has_compression ? postScript->compression : 0;

	fclose(orcFile);

	return postScript;
}

/**
 * Reads file footer from the file at the given offset and length and returns the decoded footer to the parameter.
 *
 * @param orcFileName name of the orc file
 * @param footerOffset offset of the footer in the file
 * @param footerSize size of the footer
 *
 * @return NULL for failure, non-NULL for footer
 */
Footer* readFileFooter(char* orcFileName, int footerOffset, long footerSize)
{
	Footer* footer = NULL;
	CompressedFileStream* stream = NULL;
	char* uncompressedFooter = NULL;
	int uncompressedFooterSize = 0;
	int result = 0;

	stream = CompressedFileStream_init(orcFileName, footerOffset, footerOffset + footerSize,
			compressionParameters.compressionBlockSize, compressionParameters.compressionKind);

	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return NULL;
	}

	result = CompressedFileStream_readRemaining(stream, &uncompressedFooter, &uncompressedFooterSize);

	if (result)
	{
		fprintf(stderr, "Error while uncompressing file footer\n");
		return NULL;
	}

	/* unpack the message using protobuf-c. */
	footer = footer__unpack(NULL, uncompressedFooterSize, (uint8_t*) uncompressedFooter);

	if (footer == NULL)
	{
		fprintf(stderr, "error unpacking incoming message\n");
		return NULL;
	}

	CompressedFileStream_free(stream);

	return footer;
}

/**
 * Reads the stripe footer from the file by looking at the stripe information.
 *
 * @param orcFileName name of the orc file
 * @param stripeInfo info of the corresponding stripe
 *
 * @return NULL for failure, non-NULL for stripe footer
 */
StripeFooter* readStripeFooter(char* orcFileName, StripeInformation* stripeInfo)
{
	StripeFooter* stripeFooter = NULL;
	CompressedFileStream *stream = NULL;
	char *stripeFooterBuffer = NULL;
	int uncompressedStripeFooterSize = 0;
	long stripeFooterOffset = 0;
	int result = 0;

	stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	stream = CompressedFileStream_init(orcFileName, stripeFooterOffset, stripeFooterOffset + stripeInfo->footerlength,
			compressionParameters.compressionBlockSize, compressionParameters.compressionKind);

	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return NULL;
	}

	stripeFooter = NULL;
	result = CompressedFileStream_readRemaining(stream, &stripeFooterBuffer, &uncompressedStripeFooterSize);

	if (result)
	{
		fprintf(stderr, "Error while uncompressing file footer");
	}

	stripeFooter = stripe_footer__unpack(NULL, uncompressedStripeFooterSize, (uint8_t*) stripeFooterBuffer);

	if (stripeFooter == NULL)
	{
		fprintf(stderr, "error while unpacking stripe footer\n");
		return NULL;
	}

	CompressedFileStream_free(stream);

	return stripeFooter;
}

/**
 * Allocates space for the structure reader using the file footer and the specified fields.
 *
 * @param reader structure to store the reader information
 * @param footer orc file footer
 * @param selectedFields an array of bytes which contains either 0 or 1 to specify the needed fields
 *
 * @return 0 for success and -1 for failure
 */
int StructReader_allocate(StructReader* reader, Footer* footer, char* selectedFields)
{
	Type** types = footer->types;
	Type* root = footer->types[0];
	Type* type = NULL;
	PrimitiveReader* primitiveReader = NULL;
	ListReader* listReader = NULL;
	Reader* field = NULL;
	Reader* listItemReader = NULL;
	int readerIterator = 0;
	int streamIterator = 0;

	reader->noOfFields = root->n_subtypes;
	reader->fields = malloc(sizeof(Reader*) * reader->noOfFields);

	for (readerIterator = 0; readerIterator < reader->noOfFields; ++readerIterator)
	{

		reader->fields[readerIterator] = malloc(sizeof(Reader));
		field = reader->fields[readerIterator];
		field->columnNo = root->subtypes[readerIterator];
		type = types[root->subtypes[readerIterator]];
		field->kind = type->kind;
		field->hasPresentBitReader = 0;
		field->presentBitReader.stream = NULL;

		if (!selectedFields[readerIterator])
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
				return -1;
			}

			listItemReader->fieldReader = malloc(sizeof(PrimitiveReader));
			primitiveReader = listItemReader->fieldReader;

			for (streamIterator = 0; streamIterator < MAX_STREAM_COUNT; ++streamIterator)
			{
				primitiveReader->readers[streamIterator].stream = NULL;
			}
		}
		else
		{
			field->fieldReader = malloc(sizeof(PrimitiveReader));

			primitiveReader = field->fieldReader;
			primitiveReader->dictionary = NULL;
			primitiveReader->dictionarySize = 0;
			primitiveReader->wordLength = NULL;

			for (streamIterator = 0; streamIterator < MAX_STREAM_COUNT; ++streamIterator)
			{
				primitiveReader->readers[streamIterator].stream = NULL;
			}
		}
	}

	return 0;
}

/**
 * Initialize the structure reader for the given stripe
 *
 * @param structReader record reader for the table
 * @param orcFileName name of the orc file
 * @param dataOffset offset of the start of the data stream of the stripe in the file
 * @param stripeFooter footer of the current stripe
 *
 * @return 0 for success and -1 for failure
 */
int StructReader_init(StructReader* structReader, char* orcFileName, long dataOffset, StripeFooter* stripeFooter)
{
	Reader** readers = structReader->fields;
	Reader* reader = NULL;
	Stream* stream = NULL;
	PrimitiveReader* primitiveReader = NULL;
	ListReader* listReader = NULL;
	Type__Kind streamKind = 0;
	ColumnEncoding* columnEncoding = NULL;
	int streamNo = 0;
	int fieldNo = 0;
	long currentOffset = dataOffset;
	int noOfDataStreams = 0;
	int dataStreamIterator = 0;
	int totalNoOfStreams = stripeFooter->n_streams;
	int result = 0;

	stream = stripeFooter->streams[streamNo];

	while (stream->kind == STREAM__KIND__ROW_INDEX)
	{
		/* skip index data for now */
		stream = stripeFooter->streams[++streamNo];
	}

	while (fieldNo < structReader->noOfFields)
	{
		reader = readers[fieldNo++];

		if (!reader->required)
		{
			/* skip the not required columns */
			continue;
		}

		while (reader->columnNo > stream->column)
		{
			/* skip columns that doesn't required for reading */
			currentOffset += stream->length;

			if (++streamNo >= totalNoOfStreams)
			{
				return -1;
			}

			stream = stripeFooter->streams[streamNo];
		}

		if (reader->kind == TYPE__KIND__LIST)
		{
			/* if the type is list, first check for present stream as always */
			reader->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;

			if (reader->hasPresentBitReader)
			{
				result = StreamReader_init(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName, currentOffset,
						currentOffset + stream->length, &compressionParameters);
				if (result)
				{
					/* cannot read the data stream correctly */
					return -1;
				}
				currentOffset += stream->length;
				if (++streamNo >= totalNoOfStreams)
				{
					return -1;
				}
				stream = stripeFooter->streams[streamNo];
			}

			/* then read the length stream into the reader */
			streamKind = getStreamKind(reader->kind, 0);
			listReader = reader->fieldReader;
			result = StreamReader_init(&listReader->lengthReader, streamKind, orcFileName, currentOffset,
					currentOffset + stream->length, &compressionParameters);

			if (result)
			{
				return result;
			}

			currentOffset += stream->length;

			if (++streamNo >= totalNoOfStreams)
			{
				return -1;
			}

			stream = stripeFooter->streams[streamNo];
			/* and finally set the item of the list as the reader and continue */
			reader = &listReader->itemReader;
		}

		reader->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;

		if (reader->hasPresentBitReader)
		{
			result = StreamReader_init(&reader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName, currentOffset,
					currentOffset + stream->length, &compressionParameters);

			if (result)
			{
				/* cannot read the data stream correctly */
				return -1;
			}

			currentOffset += stream->length;

			if (++streamNo >= totalNoOfStreams)
			{
				return -1;
			}

			stream = stripeFooter->streams[streamNo];
		}

		primitiveReader = reader->fieldReader;
		freePrimitiveReader(primitiveReader);

		if (reader->kind == TYPE__KIND__STRING)
		{
			columnEncoding = stripeFooter->columns[reader->columnNo];
			primitiveReader->dictionarySize = columnEncoding->dictionarysize;
		}

		noOfDataStreams = getStreamCount(reader->kind);

		if (streamNo + noOfDataStreams > totalNoOfStreams)
		{
			return -1;
		}

		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = getStreamKind(reader->kind, dataStreamIterator);
			result = StreamReader_init(&primitiveReader->readers[dataStreamIterator], streamKind, orcFileName,
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
