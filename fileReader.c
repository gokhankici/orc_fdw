#include <zlib.h>
#include "orc.pb-c.h"
#include "fileReader.h"
#include "recordReader.h"
#include "inputStream.h"
#include "util.h"

static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer, char* selectedFields);

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
	FILE* orcFile = AllocateFile(orcFileName, "r");
	int isByteRead = 0;
	char c = 0;
	size_t msg_len = 0;
	uint8_t postScriptBuffer[MAX_POSTSCRIPT_SIZE];
	int psSize = 0;

	if (orcFile == NULL)
	{
		LogError("Cannot open orc file");
		return NULL;
	}

	fseek(orcFile, -1, SEEK_END);
	isByteRead = fread(&c, sizeof(char), 1, orcFile);

	if (!isByteRead)
	{
		LogError("Error while reading the last byte\n");
		return NULL;
	}

	psSize = ((int) c) & 0xFF;

	/* read postscript into the buffer */
	fseek(orcFile, -1 - psSize, SEEK_END);
	*postScriptOffset = ftell(orcFile);
	msg_len = fread(postScriptBuffer, 1, psSize, orcFile);

	if (msg_len != psSize)
	{
		LogError("Error while reading postscript from file\n");
		return NULL;
	}

	/* unpack the message using protobuf-c. */
	postScript = post_script__unpack(NULL, msg_len, postScriptBuffer);

	if (postScript == NULL)
	{
		LogError("error unpacking incoming message\n");
		return NULL;
	}

	parameters->compressionBlockSize = postScript->has_compressionblocksize ? postScript->compressionblocksize : 0;
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
Footer* FileFooterInit(char* orcFileName, int footerOffset, long footerSize, CompressionParameters* parameters)
{
	Footer* footer = NULL;
	FileStream* stream = NULL;
	char* uncompressedFooter = NULL;
	int uncompressedFooterSize = 0;
	int result = 0;

	stream = FileStreamInit(orcFileName, footerOffset, footerOffset + footerSize, parameters->compressionBlockSize,
			parameters->compressionKind);

	if (stream == NULL)
	{
		LogError("Error reading file stream\n");
		return NULL;
	}

	result = FileStreamReadRemaining(stream, &uncompressedFooter, &uncompressedFooterSize);

	if (result)
	{
		LogError("Error while uncompressing file footer\n");
		return NULL;
	}

	/* unpack the message using protobuf-c. */
	footer = footer__unpack(NULL, uncompressedFooterSize, (uint8_t*) uncompressedFooter);

	if (footer == NULL)
	{
		LogError("error unpacking incoming message\n");
		return NULL;
	}

	FileStreamFree(stream);

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
StripeFooter* StripeFooterInit(char* orcFileName, StripeInformation* stripeInfo, CompressionParameters* parameters)
{
	StripeFooter* stripeFooter = NULL;
	FileStream *stream = NULL;
	char *stripeFooterBuffer = NULL;
	int uncompressedStripeFooterSize = 0;
	long stripeFooterOffset = 0;
	int result = 0;

	stripeFooterOffset = stripeInfo->offset + (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0)
			+ stripeInfo->datalength;
	stream = FileStreamInit(orcFileName, stripeFooterOffset, stripeFooterOffset + stripeInfo->footerlength,
			parameters->compressionBlockSize, parameters->compressionKind);

	if (stream == NULL)
	{
		LogError("Error reading file stream\n");
		return NULL;
	}

	stripeFooter = NULL;
	result = FileStreamReadRemaining(stream, &stripeFooterBuffer, &uncompressedStripeFooterSize);

	if (result)
	{
		LogError("Error while uncompressing file footer");
	}

	stripeFooter = stripe_footer__unpack(NULL, uncompressedStripeFooterSize, (uint8_t*) stripeFooterBuffer);

	if (stripeFooter == NULL)
	{
		LogError("error while unpacking stripe footer\n");
		return NULL;
	}

	FileStreamFree(stream);

	return stripeFooter;
}

int FieldReaderAllocate(FieldReader* reader, Footer* footer, char* selectedFields)
{
	reader->orcColumnNo = 0;
	reader->hasPresentBitReader = 0;
	reader->kind = TYPE__KIND__STRUCT;
	reader->required = 1;

	reader->presentBitReader.stream = NULL;
	reader->lengthReader.stream = NULL;

	reader->fieldReader = malloc(sizeof(StructFieldReader));
	return StructFieldReaderAllocate((StructFieldReader*) reader->fieldReader, footer, selectedFields);
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
static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer, char* selectedFields)
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
		field->orcColumnNo = root->subtypes[readerIterator];
		type = types[root->subtypes[readerIterator]];
		field->kind = type->kind;
		field->hasPresentBitReader = 0;
		field->presentBitReader.stream = NULL;
		field->lengthReader.stream = NULL;
		field->required = selectedFields[readerIterator];

		if (field->kind == TYPE__KIND__LIST)
		{
			field->fieldReader = malloc(sizeof(ListFieldReader));

			listReader = field->fieldReader;

			/* initialize list item reader */
			listItemReader = &listReader->itemReader;
			listItemReader->hasPresentBitReader = 0;
			listItemReader->presentBitReader.stream = NULL;
			listItemReader->orcColumnNo = type->subtypes[0];
			listItemReader->kind = types[listItemReader->orcColumnNo]->kind;
			listItemReader->required = selectedFields[readerIterator];

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

static int FieldReaderInitHelper(FieldReader* fieldReader, char* orcFileName, long* currentDataOffset, int* streamNo,
		StripeFooter* stripeFooter, CompressionParameters* parameters);

int FieldReaderInit(FieldReader* fieldReader, char* orcFileName, StripeInformation* stripe, StripeFooter* stripeFooter,
		CompressionParameters* parameters)
{
	long currentDataOffset = 0;
	int streamNo = 0;
	Stream* stream = NULL;

	currentDataOffset = stripe->offset + stripe->indexlength;
	stream = stripeFooter->streams[streamNo];

	while (streamNo < stripeFooter->n_streams && stream->kind == STREAM__KIND__ROW_INDEX)
	{
		streamNo++;
		stream = stripeFooter->streams[streamNo];
	}

	return FieldReaderInitHelper(fieldReader, orcFileName, &currentDataOffset, &streamNo, stripeFooter, parameters);
}

/**
 * Initialize the structure reader for the given stripe
 *
 * @param fieldReader field reader for the table
 * @param orcFileName name of the orc file
 * @param currentDataOffset pointer to store the current data stream offset in the file after reading
 * @param streamNo pointer to store the current stream no after reading
 * @param stripeFooter footer of the current stripe
 * @param parameters holds compression type and block size
 *
 * @return 0 for success and -1 for failure
 */
static int FieldReaderInitHelper(FieldReader* fieldReader, char* orcFileName, long* currentDataOffset, int* streamNo,
		StripeFooter* stripeFooter, CompressionParameters* parameters)
{
	StructFieldReader *structFieldReader = NULL;
	ListFieldReader *listFieldReader = NULL;
	PrimitiveFieldReader *primitiveFieldReader = NULL;
	FieldReader* subField = NULL;
	Stream* stream = NULL;
	Type__Kind fieldKind = 0;
	Type__Kind streamKind = 0;
	ColumnEncoding *columnEncoding = NULL;
	int fieldNo = 0;
	int noOfDataStreams = 0;
	int totalNoOfStreams = 0;
	int dataStreamIterator = 0;
	int dictionaryIterator = 0;
	int result = 0;

	totalNoOfStreams = stripeFooter->n_streams;
	stream = stripeFooter->streams[*streamNo];
	fieldKind = fieldReader->kind;

	fieldReader->hasPresentBitReader = stream->column == fieldReader->orcColumnNo && stream->kind == STREAM__KIND__PRESENT;

	if (fieldReader->hasPresentBitReader)
	{
		if (fieldReader->required)
		{
			result = StreamReaderInit(&fieldReader->presentBitReader, TYPE__KIND__BOOLEAN, orcFileName,
					*currentDataOffset, *currentDataOffset + stream->length, parameters);
		}
		else
		{
			fieldReader->hasPresentBitReader = 0;
		}

		if (result)
		{
			return -1;
		}

		*currentDataOffset += stream->length;
		(*streamNo)++;

		if (*streamNo >= totalNoOfStreams)
		{
			return -1;
		}

		stream = stripeFooter->streams[*streamNo];
	}

	switch (fieldKind)
	{
	case TYPE__KIND__LIST:
		listFieldReader = fieldReader->fieldReader;

		if (fieldReader->required)
		{
			result = StreamReaderInit(&fieldReader->lengthReader, TYPE__KIND__INT, orcFileName, *currentDataOffset,
					*currentDataOffset + stream->length, parameters);
		}

		if (result)
		{
			return -1;
		}

		*currentDataOffset += stream->length;
		(*streamNo)++;

		if (*streamNo >= totalNoOfStreams)
		{
			return -1;
		}

		stream = stripeFooter->streams[*streamNo];

		if (IsComplexType(listFieldReader->itemReader.kind))
		{
			fprintf(stderr, "List of complex types complex types are not supported\n");
			return -1;
		}

		return FieldReaderInitHelper(&listFieldReader->itemReader, orcFileName, currentDataOffset, streamNo,
				stripeFooter, parameters);
	case TYPE__KIND__MAP:
	case TYPE__KIND__DECIMAL:
	case TYPE__KIND__UNION:
		/* these are not supported yet */
		fprintf(stderr, "Use of not supported type. Type id: %d\n", fieldKind);
		return -1;
	case TYPE__KIND__STRUCT:
		structFieldReader = fieldReader->fieldReader;
		for (fieldNo = 0; fieldNo < structFieldReader->noOfFields; fieldNo++)
		{
			subField = structFieldReader->fields[fieldNo];
			result = FieldReaderInitHelper(subField, orcFileName, currentDataOffset, streamNo, stripeFooter,
					parameters);

			if (result)
			{
				return -1;
			}
		}

		return (*streamNo == totalNoOfStreams) ? 0 : -1;
	default:
		/* these are the supported types, unsupported types are declared above */
		primitiveFieldReader = fieldReader->fieldReader;

		if (fieldReader->kind == TYPE__KIND__STRING)
		{
			if (primitiveFieldReader->dictionary)
			{
				for (dictionaryIterator = 0; dictionaryIterator < primitiveFieldReader->dictionarySize;
						++dictionaryIterator)
				{
					free(primitiveFieldReader->dictionary[dictionaryIterator]);
				}
				free(primitiveFieldReader->dictionary);
				free(primitiveFieldReader->wordLength);
				primitiveFieldReader->dictionary = NULL;
				primitiveFieldReader->wordLength = NULL;
			}

			columnEncoding = stripeFooter->columns[fieldReader->orcColumnNo];

			if (fieldReader->required)
			{
				primitiveFieldReader->dictionarySize = columnEncoding->dictionarysize;
			}
			else
			{
				primitiveFieldReader->dictionarySize = 0;
			}
		}

		noOfDataStreams = GetStreamCount(fieldReader->kind);

		if (*streamNo + noOfDataStreams > totalNoOfStreams)
		{
			return -1;
		}

		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = GetStreamKind(fieldKind, dataStreamIterator);

			if (fieldReader->required)
			{
				result = StreamReaderInit(&primitiveFieldReader->readers[dataStreamIterator], streamKind, orcFileName,
						*currentDataOffset, *currentDataOffset + stream->length, parameters);
			}

			if (result)
			{
				return result;
			}

			*currentDataOffset += stream->length;
			(*streamNo)++;
			stream = stripeFooter->streams[*streamNo];
		}

		return 0;
	}
}

