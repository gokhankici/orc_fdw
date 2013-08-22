#include "postgres.h"
#include "catalog/pg_type.h"

#include "orc.pb-c.h"
#include "orcUtil.h"
#include "fileReader.h"
#include "recordReader.h"
#include "inputStream.h"
#include "storage/fd.h"

static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer,
		PostgresQueryInfo* query);
static int FieldReaderInitHelper(FieldReader* fieldReader, FILE* file, long* currentDataOffset,
		int* streamNo, StripeFooter* stripeFooter, CompressionParameters* parameters);
static bool MatchOrcWithPSQL(FieldType__Kind orcType, Oid psqlType);

/**
 * Reads the postscript from the orc file and returns the postscript. Stores its offset to parameter.
 *
 * @param file file handler of the ORC file
 * @param postScriptOffset pointer to store the size of the postscript
 *
 * @return NULL for failure, non-NULL for success
 */
PostScript* PostScriptInit(FILE* file, long* postScriptOffset, CompressionParameters* parameters)
{
	PostScript* postScript = NULL;
	int isByteRead = 0;
	char c = 0;
	size_t msg_len = 0;
	uint8_t postScriptBuffer[MAX_POSTSCRIPT_SIZE];
	int psSize = 0;

	fseek(file, -1, SEEK_END);
	isByteRead = fread(&c, sizeof(char), 1, file);

	if (!isByteRead)
	{
		LogError("Error while reading the last byte\n");
		return NULL;
	}

	psSize = ((int) c) & 0xFF;

	/* read postscript into the buffer */
	fseek(file, -1 - psSize, SEEK_END);
	*postScriptOffset = ftell(file);
	msg_len = fread(postScriptBuffer, 1, psSize, file);

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

	parameters->compressionBlockSize =
			postScript->has_compressionblocksize ? postScript->compressionblocksize : 0;
	parameters->compressionKind = postScript->has_compression ? postScript->compression : 0;

	return postScript;
}

/**
 * Reads file footer from the file at the given offset and length and returns the decoded footer to the parameter.
 *
 * @param file file handler of the ORC file
 * @param footerOffset offset of the footer in the file
 * @param footerSize size of the footer
 *
 * @return NULL for failure, non-NULL for footer
 */
Footer* FileFooterInit(FILE* file, int footerOffset, long footerSize,
		CompressionParameters* parameters)
{
	Footer* footer = NULL;
	FileStream* stream = NULL;
	char* uncompressedFooter = NULL;
	int uncompressedFooterSize = 0;
	int result = 0;

	stream = FileStreamInit(file, footerOffset, footerOffset + footerSize,
			parameters->compressionBlockSize, parameters->compressionKind);

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
 * @param file file handler of the ORC file
 * @param stripeInfo info of the corresponding stripe
 *
 * @return NULL for failure, non-NULL for stripe footer
 */
StripeFooter* StripeFooterInit(FILE* file, StripeInformation* stripeInfo,
		CompressionParameters* parameters)
{
	StripeFooter* stripeFooter = NULL;
	FileStream *stream = NULL;
	char *stripeFooterBuffer = NULL;
	int uncompressedStripeFooterSize = 0;
	long stripeFooterOffset = 0;
	int result = 0;

	stripeFooterOffset = stripeInfo->offset
			+ (stripeInfo->has_indexlength ? stripeInfo->indexlength : 0) + stripeInfo->datalength;
	stream = FileStreamInit(file, stripeFooterOffset, stripeFooterOffset + stripeInfo->footerlength,
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

	stripeFooter = stripe_footer__unpack(NULL, uncompressedStripeFooterSize,
			(uint8_t*) stripeFooterBuffer);

	if (stripeFooter == NULL)
	{
		LogError("error while unpacking stripe footer\n");
		return NULL;
	}

	FileStreamFree(stream);

	return stripeFooter;
}

/*
 * Allocates memory and sets initial types/values for the variables of a field reader
 *
 * @param query list of required columns and their propertiesss
 *
 * @return
 */
int FieldReaderAllocate(FieldReader* reader, Footer* footer, PostgresQueryInfo* query)
{
	reader->orcColumnNo = 0;
	reader->hasPresentBitReader = 0;
	reader->kind = FIELD_TYPE__KIND__STRUCT;
	reader->required = 1;

	reader->presentBitReader.stream = NULL;

	reader->fieldReader = alloc(sizeof(StructFieldReader));
	return StructFieldReaderAllocate((StructFieldReader*) reader->fieldReader, footer, query);
}

/*
 * Utility function to check whether ORC type matches with PostgreSQL type
 */
static bool MatchOrcWithPSQL(FieldType__Kind orcType, Oid psqlType)
{
	bool matches = false;

	switch (psqlType)
	{
	case INT8OID:
		matches = matches || (orcType == FIELD_TYPE__KIND__LONG);
	case INT4OID:
		matches = matches || (orcType == FIELD_TYPE__KIND__INT);
	case INT2OID:
		matches = matches || (orcType == FIELD_TYPE__KIND__SHORT);
		break;
	case FLOAT4OID:
	case FLOAT8OID:
		matches = orcType == FIELD_TYPE__KIND__FLOAT || orcType == FIELD_TYPE__KIND__DOUBLE;
		break;
	case BOOLOID:
		matches = orcType == FIELD_TYPE__KIND__BOOLEAN;
		break;
	case BPCHAROID:
	case VARCHAROID:
	case TEXTOID:
		matches = orcType == FIELD_TYPE__KIND__STRING;
		break;
	case DATEOID:
	case TIMESTAMPOID:
		matches = orcType == FIELD_TYPE__KIND__TIMESTAMP;
		break;
	case NUMERICOID:
	case TIMESTAMPTZOID:
	default:
		break;
	}

	return matches;
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
static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer,
		PostgresQueryInfo* query)
{
	FieldType** types = footer->types;
	FieldType* root = footer->types[0];
	FieldType* type = NULL;
	PrimitiveFieldReader* primitiveReader = NULL;
	ListFieldReader* listReader = NULL;
	FieldReader* field = NULL;
	FieldReader* listItemReader = NULL;
	int readerIterator = 0;
	int streamIterator = 0;
	PostgresColumnInfo* requestedColumns = query->selectedColumns;
	PostgresColumnInfo* currentColumnInfo = NULL;
	int queryColumnIterator = 0;
	int arrayItemPSQLKind = 0;
	bool typesMatch = false;

	reader->noOfFields = root->n_subtypes;
	reader->fields = alloc(sizeof(FieldReader*) * reader->noOfFields);

	for (readerIterator = 0; readerIterator < reader->noOfFields; ++readerIterator)
	{
		/* create field reader definitions for all fields */
		reader->fields[readerIterator] = alloc(sizeof(FieldReader));
		field = reader->fields[readerIterator];
		field->orcColumnNo = root->subtypes[readerIterator];
		type = types[root->subtypes[readerIterator]];
		field->kind = type->kind;
		field->hasPresentBitReader = 0;
		field->presentBitReader.stream = NULL;
		field->rowIndex = NULL;

		/* requested columns are sorted according to their index, we can trust its order */
		if (queryColumnIterator < query->noOfSelectedColumns
				&& requestedColumns[queryColumnIterator].columnIndex == readerIterator)
		{
			field->required = 1;
			currentColumnInfo = &requestedColumns[queryColumnIterator];
			field->psqlKind = currentColumnInfo->columnTypeId;

			field->columnTypeMod = currentColumnInfo->columnTypeMod;
			arrayItemPSQLKind = currentColumnInfo->columnArrayTypeId;
			queryColumnIterator++;
		}
		else
		{
			field->required = 0;
			field->psqlKind = 0;
			field->columnTypeMod = 0;
			arrayItemPSQLKind = 0;
		}

		if (field->kind == FIELD_TYPE__KIND__LIST)
		{
			field->fieldReader = alloc(sizeof(ListFieldReader));

			listReader = field->fieldReader;
			listReader->lengthReader.stream = NULL;

			/* initialize list item reader */
			listItemReader = &listReader->itemReader;
			listItemReader->required = field->required;
			listItemReader->psqlKind = arrayItemPSQLKind;
			listItemReader->hasPresentBitReader = 0;
			listItemReader->presentBitReader.stream = NULL;
			listItemReader->orcColumnNo = type->subtypes[0];
			listItemReader->kind = types[listItemReader->orcColumnNo]->kind;

			if (field->required)
			{
				typesMatch = MatchOrcWithPSQL(listItemReader->kind, listItemReader->psqlKind);
				if (arrayItemPSQLKind == InvalidOid || !typesMatch)
				{
					LogError3(
							"Error while reading column %d: ORC and PSQL types do not match, ORC type is %s[]",
							field->orcColumnNo, getTypeKindName(listItemReader->kind));
				}
			}

			if (IsComplexType(listItemReader->kind))
			{
				/* only list of primitive types is supported */
				return -1;
			}

			listItemReader->fieldReader = alloc(sizeof(PrimitiveFieldReader));
			primitiveReader = listItemReader->fieldReader;

			primitiveReader->dictionary = NULL;
			primitiveReader->dictionarySize = 0;
			primitiveReader->wordLength = NULL;

			for (streamIterator = 0; streamIterator < MAX_STREAM_COUNT; ++streamIterator)
			{
				primitiveReader->readers[streamIterator].stream = NULL;
			}
		}
		else if (field->kind == FIELD_TYPE__KIND__STRUCT || field->kind == FIELD_TYPE__KIND__MAP)
		{
			/* struct and map fields are not supported */
			return -1;
		}
		else
		{
			if (field->required)
			{
				typesMatch = MatchOrcWithPSQL(field->kind, field->psqlKind);
				if (arrayItemPSQLKind != InvalidOid || !typesMatch)
				{
					LogError3(
							"Error while reading column %d: ORC and PSQL types do not match, ORC type is %s",
							field->orcColumnNo, getTypeKindName(field->kind));
				}
			}

			field->fieldReader = alloc(sizeof(PrimitiveFieldReader));

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

/*
 * Initializes a reader for the given stripe. Uses helper function FieldReaderInitHelper
 * to recursively initialize its fields.
 */
int FieldReaderInit(FieldReader* fieldReader, FILE* file, StripeInformation* stripe,
		StripeFooter* stripeFooter, CompressionParameters* parameters)
{
	StructFieldReader structReader = (StructFieldReader*) fieldReader->fieldReader;
	FieldReader* subField = NULL;
	FileStream* indexStream = NULL;
	char* indexBuffer = NULL;
	int indexBufferLength = 0;
	Stream* stream = NULL;
	long currentDataOffset = 0;
	int streamNo = 0;
	int result = 0;
	int iterator = 0;

	currentDataOffset = stripe->offset + stripe->indexlength;
	stream = stripeFooter->streams[streamNo];

	if (stream->kind == STREAM__KIND__ROW_INDEX)
	{
		/* first index stream is for the struct field, skip it */
		streamNo++;
		stream = stripeFooter->streams[streamNo];
	}

	while (streamNo < stripeFooter->n_streams && stream->kind == STREAM__KIND__ROW_INDEX)
	{
		subField = structReader->fields[streamNo - 1];

		if (subField->required)
		{
			/* if row is required, read its index information */
			if (subField->rowIndex)
			{
				row_index__free_unpacked(subField->rowIndex, NULL);
				subField->rowIndex = NULL;
			}

			indexStream = FileStreamInit(file, currentDataOffset,
					currentDataOffset + stream->length,
					DEFAULT_ROW_INDEX_SIZE, parameters->compressionKind);
			FileStreamReadRemaining(indexStream, &indexBuffer, &indexBufferLength);

			subField->rowIndex = row_index__unpack(NULL, indexBufferLength, (uint8_t*) indexBuffer);
			if (!subField->rowIndex)
			{
				LogError("Error while unpacking row index message");
				return -1;
			}

			FileStreamFree(indexStream);
		}

		streamNo++;
		stream = stripeFooter->streams[streamNo];
	}

	/* set offset for data reading */
	currentDataOffset = stripe->offset + stripe->indexlength;

	return FieldReaderInitHelper(fieldReader, file, &currentDataOffset, &streamNo, stripeFooter,
			parameters);
}

/**
 * Helper function to initialize the reader for the given stripe
 *
 * @param fieldReader field reader for the table
 * @param file ORC file
 * @param currentDataOffset pointer to store the current data stream offset in the file after reading
 * @param streamNo pointer to store the current stream no after reading
 * @param stripeFooter footer of the current stripe
 * @param parameters holds compression type and block size
 *
 * @return 0 for success and -1 for failure
 */
static int FieldReaderInitHelper(FieldReader* fieldReader, FILE* file, long* currentDataOffset,
		int* streamNo, StripeFooter* stripeFooter, CompressionParameters* parameters)
{
	StructFieldReader *structFieldReader = NULL;
	ListFieldReader *listFieldReader = NULL;
	PrimitiveFieldReader *primitiveFieldReader = NULL;
	FieldReader* subField = NULL;
	Stream* stream = NULL;
	FieldType__Kind fieldKind = 0;
	FieldType__Kind streamKind = 0;
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

	fieldReader->hasPresentBitReader = stream->column == fieldReader->orcColumnNo
			&& stream->kind == STREAM__KIND__PRESENT;

	/* first stream is always present stream, check for that */
	if (fieldReader->hasPresentBitReader)
	{
		if (fieldReader->required)
		{
			result = StreamReaderInit(&fieldReader->presentBitReader, FIELD_TYPE__KIND__BOOLEAN,
					file, *currentDataOffset, *currentDataOffset + stream->length, parameters);
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
	case FIELD_TYPE__KIND__LIST:
	{
		listFieldReader = fieldReader->fieldReader;

		if (fieldReader->required)
		{
			result = StreamReaderInit(&listFieldReader->lengthReader, FIELD_TYPE__KIND__INT, file,
					*currentDataOffset, *currentDataOffset + stream->length, parameters);
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
			LogError("List of complex types complex types are not supported\n");
			return -1;
		}

		return FieldReaderInitHelper(&listFieldReader->itemReader, file, currentDataOffset,
				streamNo, stripeFooter, parameters);
	}
	case FIELD_TYPE__KIND__MAP:
	case FIELD_TYPE__KIND__DECIMAL:
	case FIELD_TYPE__KIND__UNION:
	{
		/* these are not supported yet */
		LogError2("Use of not supported type. Type id: %d\n", fieldKind);
		return -1;
	}
	case FIELD_TYPE__KIND__STRUCT:
	{
		/* check for nested types is done at FieldReaderAllocate function */
		structFieldReader = fieldReader->fieldReader;
		for (fieldNo = 0; fieldNo < structFieldReader->noOfFields; fieldNo++)
		{
			subField = structFieldReader->fields[fieldNo];
			result = FieldReaderInitHelper(subField, file, currentDataOffset, streamNo,
					stripeFooter, parameters);

			if (result)
			{
				return -1;
			}
		}

		return (*streamNo == totalNoOfStreams) ? 0 : -1;
	}
	default:
	{
		/* these are the supported types, unsupported types are declared above */
		primitiveFieldReader = fieldReader->fieldReader;
		columnEncoding = stripeFooter->columns[fieldReader->orcColumnNo];

		if (fieldReader->kind == FIELD_TYPE__KIND__STRING && primitiveFieldReader)
		{
			if (columnEncoding->kind != COLUMN_ENCODING__KIND__DICTIONARY)
			{
				LogError("Only dictionary encoded strings are supported.");
				return -1;
			}

			/* if field's type is string, (re)initialize dictionary */
			if (primitiveFieldReader->dictionary)
			{
				for (dictionaryIterator = 0;
						dictionaryIterator < primitiveFieldReader->dictionarySize;
						++dictionaryIterator)
				{
					freeMemory(primitiveFieldReader->dictionary[dictionaryIterator]);
				}
				freeMemory(primitiveFieldReader->dictionary);
				freeMemory(primitiveFieldReader->wordLength);
				primitiveFieldReader->dictionary = NULL;
				primitiveFieldReader->wordLength = NULL;
			}

			if (fieldReader->required)
			{
				primitiveFieldReader->dictionarySize = columnEncoding->dictionarysize;
			}
			else if (primitiveFieldReader)
			{
				primitiveFieldReader->dictionarySize = 0;
			}
		}
		else if (columnEncoding->kind != COLUMN_ENCODING__KIND__DIRECT)
		{
			LogError2("Only direct encoding is supported for %s types.",
					getTypeKindName(fieldReader->kind));
			return -1;
		}

		noOfDataStreams = GetStreamCount(fieldReader->kind);

		/* check if there exists enough stream for the current field */
		if (*streamNo + noOfDataStreams > totalNoOfStreams)
		{
			return -1;
		}

		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = GetStreamKind(fieldKind, dataStreamIterator);

			if (fieldReader->required)
			{
				result = StreamReaderInit(&primitiveFieldReader->readers[dataStreamIterator],
						streamKind, file, *currentDataOffset, *currentDataOffset + stream->length,
						parameters);
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
}

