#include "postgres.h"
#include "catalog/pg_type.h"
#include "storage/fd.h"
#include "utils/lsyscache.h"

#include "orc_fdw.h"
#include "orc.pb-c.h"
#include "orcUtil.h"
#include "fileReader.h"
#include "recordReader.h"
#include "inputStream.h"

/* forward declarations of static functions */
static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer, List* columns);
static int FieldReaderInitHelper(FieldReader* fieldReader, FILE* file, long* currentDataOffset,
		int* streamNo, StripeFooter* stripeFooter, CompressionParameters* parameters);
static bool MatchOrcWithPSQL(FieldType__Kind orcType, Oid psqlType);

static void PrimitiveFieldReaderFree(PrimitiveFieldReader* reader);
static void StructFieldReaderFree(StructFieldReader* structReader);

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
int FieldReaderAllocate(FieldReader* reader, Footer* footer, List* columns)
{
	reader->orcColumnNo = 0;
	reader->hasPresentBitReader = 0;
	reader->kind = FIELD_TYPE__KIND__STRUCT;
	reader->required = 1;
	reader->psqlVariable = NULL;
	reader->rowIndex = NULL;
	reader->presentBitReader.stream = NULL;

	reader->fieldReader = alloc(sizeof(StructFieldReader));

	/* allocate memory for the row as a structure reader */
	return StructFieldReaderAllocate((StructFieldReader*) reader->fieldReader, footer, columns);
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
		/* floating point type difference isn't made in the program */
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
		matches = orcType == FIELD_TYPE__KIND__DATE;
		break;
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
//static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer,
//		PostgresQueryInfo* query)
static int StructFieldReaderAllocate(StructFieldReader* reader, Footer* footer, List* columns)
{
	FieldType** types = footer->types;
	FieldType* root = footer->types[0];
	FieldType* type = NULL;
	PrimitiveFieldReader* primitiveReader = NULL;
	ListFieldReader* listReader = NULL;
	FieldReader* field = NULL;
	FieldReader* listItemReader = NULL;
	ListCell* listCell = NULL;
	Var* variable = NULL;
	int readerIterator = 0;
	int streamIterator = 0;
	int queryColumnIterator = 0;
	int arrayItemPSQLKind = 0;
	bool typesMatch = false;

	reader->noOfFields = root->n_subtypes;
	reader->fields = alloc(sizeof(FieldReader*) * reader->noOfFields);
	listCell = list_head(columns);

	/* list may be empty, like in the case of "select count(*) from table_name" */
	if (listCell)
	{
		variable = lfirst(listCell);
	}

	/* for each column in the row, create readers if they are required in the query */
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
		if (listCell != NULL && (variable->varattno - 1) == readerIterator)
		{
			field->required = 1;
			field->psqlVariable = variable;

			listCell = lnext(listCell);
			if (listCell)
			{
				variable = (Var*) lfirst(listCell);
			}
			queryColumnIterator++;
		}
		else
		{
			field->required = 0;
			field->psqlVariable = NULL;
		}

		if (field->kind == FIELD_TYPE__KIND__LIST)
		{
			field->fieldReader = alloc(sizeof(ListFieldReader));

			listReader = field->fieldReader;
			listReader->lengthReader.stream = NULL;

			/* initialize list item reader */
			listItemReader = &listReader->itemReader;
			listItemReader->required = field->required;
			listItemReader->hasPresentBitReader = 0;
			listItemReader->presentBitReader.stream = NULL;
			listItemReader->orcColumnNo = type->subtypes[0];
			listItemReader->kind = types[listItemReader->orcColumnNo]->kind;
			listItemReader->rowIndex = NULL;

			if (listItemReader->required)
			{
				listItemReader->psqlVariable = alloc(sizeof(Var));
				memset(listItemReader->psqlVariable, 0, sizeof(Var));
				listItemReader->psqlVariable->vartype = get_element_type(
						field->psqlVariable->vartype);

				typesMatch = MatchOrcWithPSQL(listItemReader->kind, OrcGetPSQLType(listItemReader));
				if (listItemReader->psqlVariable->vartype == InvalidOid || !typesMatch)
				{
					LogError3(
							"Error while reading column %d: ORC and PSQL types do not match, ORC type is %s[]",
							field->orcColumnNo, GetTypeKindName(listItemReader->kind));
				}
			}
			else
			{
				listItemReader->psqlVariable = NULL;
			}

			if (IsComplexType(listItemReader->kind))
			{
				/* only list of primitive types is supported */
				LogError("Only lists of primitive types are supported currently");
				return -1;
			}

			listItemReader->fieldReader = alloc(sizeof(PrimitiveFieldReader));
			primitiveReader = listItemReader->fieldReader;

			primitiveReader->hasDictionary = 0;
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
			LogError2("%s kind in ORC files aren't supported", GetTypeKindName(field->kind));
			return -1;
		}
		else
		{
			/* initializers for primitive column types */
			if (field->required)
			{
				typesMatch = MatchOrcWithPSQL(field->kind, OrcGetPSQLType(field));
				if (arrayItemPSQLKind != InvalidOid || !typesMatch)
				{
					LogError3(
							"Error while reading column %d: ORC and PSQL types do not match, ORC type is %s",
							field->orcColumnNo, GetTypeKindName(field->kind));
				}
			}

			field->fieldReader = alloc(sizeof(PrimitiveFieldReader));

			primitiveReader = field->fieldReader;
			primitiveReader->hasDictionary = 0;
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
	StructFieldReader* structReader = (StructFieldReader*) fieldReader->fieldReader;
	FieldReader** fields = structReader->fields;
	FieldReader* subField = NULL;
	FileStream* indexStream = NULL;
	char* indexBuffer = NULL;
	int indexBufferLength = 0;
	Stream* stream = NULL;
	long currentDataOffset = 0;
	long currentIndexOffset = 0;
	int streamNo = 0;

	currentIndexOffset = stripe->offset;
	currentDataOffset = stripe->offset + stripe->indexlength;
	stream = stripeFooter->streams[streamNo];

	if (stream->kind == STREAM__KIND__ROW_INDEX)
	{
		/* first index stream is for the struct field, skip it */
		currentIndexOffset += stream->length;
		streamNo++;
		stream = stripeFooter->streams[streamNo];
	}

	/* read the row index information from the file for the required columns */
	while (streamNo < stripeFooter->n_streams && stream->kind == STREAM__KIND__ROW_INDEX)
	{
		subField = *fields;
		fields++;

		if (ENABLE_ROW_SKIPPING && subField->required)
		{
			/* if row is required, read its index information */
			if (subField->rowIndex)
			{
				row_index__free_unpacked(subField->rowIndex, NULL);
				subField->rowIndex = NULL;
			}

			indexStream = FileStreamInit(file, currentIndexOffset,
					currentIndexOffset + stream->length,
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

		/* if column type is list we need to take into account the index stream for the child */
		if (subField->kind == FIELD_TYPE__KIND__LIST)
		{
			subField = &((ListFieldReader*) subField->fieldReader)->itemReader;

			/* get the child's index stream */
			currentIndexOffset += stream->length;
			streamNo++;
			stream = stripeFooter->streams[streamNo];

			if (ENABLE_ROW_SKIPPING && subField->required)
			{
				if (subField->rowIndex)
				{
					row_index__free_unpacked(subField->rowIndex, NULL);
					subField->rowIndex = NULL;
				}

				indexStream = FileStreamInit(file, currentIndexOffset,
						currentIndexOffset + stream->length,
						DEFAULT_ROW_INDEX_SIZE, parameters->compressionKind);
				FileStreamReadRemaining(indexStream, &indexBuffer, &indexBufferLength);

				subField->rowIndex = row_index__unpack(NULL, indexBufferLength,
						(uint8_t*) indexBuffer);
				if (!subField->rowIndex)
				{
					LogError("Error while unpacking row index message");
					return -1;
				}

				FileStreamFree(indexStream);
			}

		}

		currentIndexOffset += stream->length;
		streamNo++;
		stream = stripeFooter->streams[streamNo];
	}

	/* set offset for data reading */
	currentDataOffset = stripe->offset + stripe->indexlength;

	/* initialize data stream readers now */
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

	/* first stream is always present stream, check for that first */
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
			/* get the length stream of the list field */
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

		/* set the readers for the child of the list */
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
		primitiveFieldReader->encoding = columnEncoding->kind;

		if (columnEncoding->kind == COLUMN_ENCODING__KIND__DIRECT_V2
				|| columnEncoding->kind == COLUMN_ENCODING__KIND__DICTIONARY_V2)
		{
			LogError("Encoding V2 is not supported");
			return -1;
		}

		if (fieldReader->kind == FIELD_TYPE__KIND__STRING && primitiveFieldReader)
		{
			primitiveFieldReader->hasDictionary = (columnEncoding->kind
					== COLUMN_ENCODING__KIND__DICTIONARY);

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
					GetTypeKindName(fieldReader->kind));
			return -1;
		}

		noOfDataStreams = GetStreamCount(fieldReader->kind, columnEncoding->kind);

		/* check if there exists enough stream for the current field */
		if (*streamNo + noOfDataStreams > totalNoOfStreams)
		{
			return -1;
		}

		for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams; ++dataStreamIterator)
		{
			streamKind = GetStreamKind(fieldKind, columnEncoding->kind, dataStreamIterator);

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

void FieldReaderSeek(FieldReader* rowReader, int strideNo)
{
	StructFieldReader* structReader = (StructFieldReader*) rowReader->fieldReader;
	FieldReader* subfield = NULL;
	PrimitiveFieldReader* primitiveFieldReader = NULL;
	RowIndex* rowIndex = NULL;
	RowIndexEntry* rowIndexEntry = NULL;
	OrcStack* stack = NULL;
	FieldType__Kind streamKind = 0;
	int columnId = 0;
	int noOfDataStreams = 0;
	int dataStreamIterator = 0;

	for (columnId = 0; columnId < structReader->noOfFields; ++columnId)
	{
		subfield = structReader->fields[columnId];

		if (subfield->required)
		{
			rowIndex = subfield->rowIndex;
			rowIndexEntry = rowIndex->entry[strideNo];
			stack = OrcStackInit(rowIndexEntry->positions, sizeof(uint64_t),
					rowIndexEntry->n_positions);

			if (subfield->hasPresentBitReader)
			{
				StreamReaderSeek(&subfield->presentBitReader, subfield->kind,
						FIELD_TYPE__KIND__BOOLEAN, stack);
			}

			switch (subfield->kind)
			{
			case FIELD_TYPE__KIND__LIST:
			{
				/* set the length reader of the list column reader */
				StreamReaderSeek(&((ListFieldReader*) subfield->fieldReader)->lengthReader,
						subfield->kind, FIELD_TYPE__KIND__INT, stack);

				/* set the subfield as the list item reader and skip its content */
				subfield = &((ListFieldReader*) subfield->fieldReader)->itemReader;

				rowIndex = subfield->rowIndex;
				rowIndexEntry = rowIndex->entry[strideNo];
				stack = OrcStackInit(rowIndexEntry->positions, sizeof(uint64_t),
						rowIndexEntry->n_positions);

				if (subfield->hasPresentBitReader)
				{
					StreamReaderSeek(&subfield->presentBitReader, subfield->kind,
							FIELD_TYPE__KIND__BOOLEAN, stack);
				}

				/* continue to the default case to jump in the child (which should be of primitive type) streams */
			}
			default:
			{
				primitiveFieldReader = (PrimitiveFieldReader*) subfield->fieldReader;

				noOfDataStreams = GetStreamCount(subfield->kind, primitiveFieldReader->encoding);

				for (dataStreamIterator = 0; dataStreamIterator < noOfDataStreams;
						++dataStreamIterator)
				{
					/*
					 * When dictionary encoding is used for strings, we only make a jump in the
					 * data stream which is the integer stream for the dictionary item position.
					 */
					if ((subfield->kind == FIELD_TYPE__KIND__STRING)
							&& (primitiveFieldReader->encoding == COLUMN_ENCODING__KIND__DICTIONARY)
							&& (dataStreamIterator != DATA_STREAM))
					{
						continue;
					}

					streamKind = GetStreamKind(subfield->kind, primitiveFieldReader->encoding,
							dataStreamIterator);
					StreamReaderSeek(&primitiveFieldReader->readers[dataStreamIterator],
							subfield->kind, streamKind, stack);
				}
			}
			}

			OrcStackFree(stack);
		}
	}
}

/**
 * static function to free up the streams of a primitive field reader
 */
static void PrimitiveFieldReaderFree(PrimitiveFieldReader* reader)
{
	int iterator = 0;
	if (reader->dictionary)
	{
		for (iterator = 0; iterator < reader->dictionarySize; ++iterator)
		{
			freeMemory(reader->dictionary[iterator]);
		}
		freeMemory(reader->dictionary);
		freeMemory(reader->wordLength);

		reader->dictionary = NULL;
		reader->wordLength = NULL;
	}
	for (iterator = 0; iterator < MAX_STREAM_COUNT; ++iterator)
	{
		if (reader->readers[iterator].stream != NULL)
		{
			FileStreamFree(reader->readers[iterator].stream);
			reader->readers[iterator].stream = NULL;
		}
	}
	freeMemory(reader);
}

/**
 * static function to free up the fields of a structure field reader
 */
static void StructFieldReaderFree(StructFieldReader* structReader)
{
	FieldReader* subField = NULL;
	int iterator = 0;

	for (iterator = 0; iterator < structReader->noOfFields; ++iterator)
	{
		subField = structReader->fields[iterator];
		if (subField->required)
		{
			FieldReaderFree(subField);
		}
		freeMemory(subField);
	}
	freeMemory(structReader->fields);
	freeMemory(structReader);
}

/**
 * Frees up a field reader
 *
 * @return 0 for success, -1 for failure
 */
int FieldReaderFree(FieldReader* reader)
{
	ListFieldReader* listReader = NULL;

	if (reader == NULL)
	{
		return 0;
	}

	StreamReaderFree(&reader->presentBitReader);

	if (reader->fieldReader == NULL)
	{
		return 0;
	}

	if (reader->rowIndex)
	{
		row_index__free_unpacked(reader->rowIndex, NULL);
		reader->rowIndex = NULL;
	}

	switch (reader->kind)
	{
	case FIELD_TYPE__KIND__STRUCT:
		StructFieldReaderFree((StructFieldReader*) reader->fieldReader);
		break;
	case FIELD_TYPE__KIND__LIST:
		listReader = (ListFieldReader*) reader->fieldReader;
		StreamReaderFree(&listReader->lengthReader);
		FieldReaderFree(&listReader->itemReader);
		freeMemory(listReader);
		break;
	case FIELD_TYPE__KIND__DECIMAL:
	case FIELD_TYPE__KIND__UNION:
	case FIELD_TYPE__KIND__MAP:
		return -1;
	default:
		PrimitiveFieldReaderFree((PrimitiveFieldReader*) reader->fieldReader);
		break;
	}

	return 0;
}
