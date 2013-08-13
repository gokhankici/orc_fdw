#include <zlib.h>
#include "orc_proto.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "inputStream.h"
#include "util.h"

/**
 * Reads the postscript from the orc file and returns the postscript. Stores its offset to parameter.
 *
 * @param orcFileName name of the orc file
 * @param postScriptOffset pointer to store the size of the postscript
 *
 * @return NULL for failure, non-NULL for success
 */
PostScript* PostScriptInit(char* orcFileName, long* postScriptOffset, CompressionParameters* parameters)
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

	parameters->compressionBlockSize =
			postScript->has_compressionblocksize ? postScript->compressionblocksize : 0;
	parameters->compressionKind = postScript->has_compression ? postScript->compression : 0;

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
Footer* FileFooterInit(char* orcFileName, int footerOffset, long footerSize,
		CompressionParameters* parameters)
{
	Footer* footer = NULL;
	FileStream* stream = NULL;
	char* uncompressedFooter = NULL;
	int uncompressedFooterSize = 0;
	int result = 0;

	stream = CompressedFileStreamInit(orcFileName, footerOffset, footerOffset + footerSize,
			parameters->compressionBlockSize, parameters->compressionKind);

	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return NULL;
	}

	result = CompressedFileStreamReadRemaining(stream, &uncompressedFooter, &uncompressedFooterSize);

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

	CompressedFileStreamFree(stream);

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
StripeFooter* StripeFooterInit(char* orcFileName, StripeInformation* stripeInfo,
		CompressionParameters* parameters)
{
	StripeFooter* stripeFooter = NULL;
	FileStream *stream = NULL;
	char *stripeFooterBuffer = NULL;
	int uncompressedStripeFooterSize = 0;
	long stripeFooterOffset = 0;
	int result = 0;

	stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	stream = CompressedFileStreamInit(orcFileName, stripeFooterOffset,
			stripeFooterOffset + stripeInfo->footerlength, parameters->compressionBlockSize,
			parameters->compressionKind);

	if (stream == NULL)
	{
		fprintf(stderr, "Error reading file stream\n");
		return NULL;
	}

	stripeFooter = NULL;
	result = CompressedFileStreamReadRemaining(stream, &stripeFooterBuffer, &uncompressedStripeFooterSize);

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

	CompressedFileStreamFree(stream);

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
int StructReaderAllocate(StructFieldReader* reader, Footer* footer, char* selectedFields)
{
	Type** types = footer->types;
	Type* root = footer->types[0];
	Type* type = NULL;
	PrimitiveFieldReader* primitiveReader = NULL;
	ListFieldReader* listReader = NULL;
	FieldReader* field = NULL;
	FieldReader* listItemReader = NULL;
	int readerIterator = 0;
	int streamIterator = 0;

	reader->noOfFields = root->n_subtypes;
	reader->fields = malloc(sizeof(FieldReader*) * reader->noOfFields);

	for (readerIterator = 0; readerIterator < reader->noOfFields; ++readerIterator)
	{

		reader->fields[readerIterator] = malloc(sizeof(FieldReader));
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
			field->fieldReader = malloc(sizeof(ListFieldReader));

			listReader = field->fieldReader;
			listReader->lengthReader.stream = NULL;

			/* initialize list item reader */
			listItemReader = &listReader->itemReader;
			listItemReader->hasPresentBitReader = 0;
			listItemReader->presentBitReader.stream = NULL;
			listItemReader->columnNo = type->subtypes[0];
			listItemReader->kind = types[listItemReader->columnNo]->kind;

			if (IsComplexType(listItemReader->kind))
			{
				/* only list of primitive types is supported */
				return -1;
			}

			listItemReader->fieldReader = malloc(sizeof(PrimitiveFieldReader));
			primitiveReader = listItemReader->fieldReader;

			for (streamIterator = 0; streamIterator < MAX_STREAM_COUNT; ++streamIterator)
			{
				primitiveReader->readers[streamIterator].stream = NULL;
			}
		}
		else if (field->kind == TYPE__KIND__STRUCT || field->kind == TYPE__KIND__MAP)
		{
			/* struct and map fields are not supported */
			return -1;
		}
		else
		{
			field->fieldReader = malloc(sizeof(PrimitiveFieldReader));

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
int StructReaderInit(StructFieldReader* structReader, char* orcFileName, long dataOffset,
		StripeFooter* stripeFooter, CompressionParameters* parameters)
{
	FieldReader** fieldReaders = structReader->fields;
	FieldReader* field = NULL;
	Stream* stream = NULL;
	PrimitiveFieldReader* primitiveReader = NULL;
	ListFieldReader* listReader = NULL;
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
		field = fieldReaders[fieldNo++];

		if (!field->required)
		{
			/* skip the not required columns */
			continue;
		}

		while (field->columnNo > stream->column)
		{
			/* skip columns that doesn't required for reading */
			currentOffset += stream->length;

			if (++streamNo >= totalNoOfStreams)
			{
				return -1;
			}

			stream = stripeFooter->streams[streamNo];
		}

		if (field->kind == TYPE__KIND__LIST)
		{
			/* if the type is list, first check for present stream as always */
			field->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;

			if (field->hasPresentBitReader)
			{
				result = StreamReaderInit(&field->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName,
						currentOffset, currentOffset + stream->length, parameters);
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
			streamKind = GetStreamKind(field->kind, 0);
			listReader = field->fieldReader;
			result = StreamReaderInit(&listReader->lengthReader, streamKind, orcFileName, currentOffset,
					currentOffset + stream->length, parameters);

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
			field = &listReader->itemReader;
		}

		field->hasPresentBitReader = stream->kind == STREAM__KIND__PRESENT;

		if (field->hasPresentBitReader)
		{
			result = StreamReaderInit(&field->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName,
					currentOffset, currentOffset + stream->length, parameters);

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

		primitiveReader = field->fieldReader;
		PrimitiveReaderFree(primitiveReader);

		if (field->kind == TYPE__KIND__STRING)
		{
			columnEncoding = stripeFooter->columns[field->columnNo];
			primitiveReader->dictionarySize = columnEncoding->dictionarysize;
		}

		noOfDataStreams = GetStreamCount(field->kind);

		if (streamNo + noOfDataStreams > totalNoOfStreams)
		{
			return -1;
		}

		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = GetStreamKind(field->kind, dataStreamIterator);
			result = StreamReaderInit(&primitiveReader->readers[dataStreamIterator], streamKind, orcFileName,
					currentOffset, currentOffset + stream->length, parameters);
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
