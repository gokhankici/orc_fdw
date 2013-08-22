#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"

#include "orc.pb-c.h"
#include "orcUtil.h"
#include "recordReader.h"

static void PrimitiveFieldReaderFree(PrimitiveFieldReader* reader);
static void StructFieldReaderFree(StructFieldReader* reader);

/**
 * Decode the nano seconds stored in the file
 *
 * @param serializedData data stored in the file as a long
 *
 * @return nanoseconds in the timestamp after the seconds
 */
static int ParseNanos(long serializedData)
{
	int zeros = 7 & (int) serializedData;
	int result = (int) serializedData >> 3;
	int iterator = 0;
	if (zeros != 0)
	{
		for (iterator = 0; iterator <= zeros; ++iterator)
		{
			result *= 10;
		}
	}
	return result;
}

/**
 * Read variable-length integer from the stream
 *
 * @param stream stream to read
 * @param data place to read into the integer
 *
 * @return length of the integer after variable-length encoding
 */
static int ReadVarLenInteger(FileStream* stream, uint64_t *data)
{
	int shift = 0;
	int bytesRead = 0;
	char byte = 0;
	int result = 0;
	*data = 0;

	result = FileStreamReadByte(stream, &byte);

	if (result)
	{
		return -1;
	}

	while ((byte & 0x80) != 0)
	{
		*data |= (uint64_t) (byte & 0x7F) << shift;
		shift += 7;
		bytesRead++;

		result = FileStreamReadByte(stream, &byte);

		if (result)
		{
			return -1;
		}
	}
	*data |= (uint64_t) byte << shift;

	return ++bytesRead;
}

/**
 * Initialize a boolean reader for reading.
 *
 * @param boolState a boolean stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int BooleanReaderInit(StreamReader* boolState)
{
	char type = 0;

	if (FileStreamReadByte(boolState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		boolState->currentEncodingType = VARIABLE_LENGTH;
		boolState->noOfLeftItems = -type - 1;
	}
	else
	{
		/* run-length contains type + 3 elements */
		boolState->currentEncodingType = RLE;
		boolState->noOfLeftItems = type + 3 - 1;
	}

	if (FileStreamReadByte(boolState->stream, &type))
	{
		return -1;
	}

	boolState->data = (uint8_t) type;
	boolState->mask = 0x80;

	return 0;
}

/**
 * Initialize a byte reader for reading.
 *
 * @param byteState a boolean stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int ByteReaderInit(StreamReader* byteState)
{
	char type = 0;

	if (FileStreamReadByte(byteState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		byteState->currentEncodingType = VARIABLE_LENGTH;
		byteState->noOfLeftItems = -type;
		byteState->data = 0;
		byteState->step = 0;
	}
	else
	{
		/* run-length contains type + 3 elements */
		byteState->currentEncodingType = RLE;
		byteState->noOfLeftItems = type + 3;

		/* step is always 0 for byte streams */
		byteState->step = 0;

		if (FileStreamReadByte(byteState->stream, &type))
		{
			return -1;
		}
		byteState->data = (uint8_t) type;
	}

	return 0;
}

/**
 * Initialize a integer reader for reading.
 * For short,int and long signed integers are used.
 * For others unsigned integers are used.
 *
 * @param kind type of the reader, used for detecting sign
 * @param intState an integer stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int IntegerReaderInit(FieldType__Kind kind, StreamReader* intState)
{
	char type = 0;
	int bytesRead = 0;

	/* read first byte to determine the encoding */
	if (FileStreamReadByte(intState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		intState->currentEncodingType = VARIABLE_LENGTH;
		intState->noOfLeftItems = -type;
		intState->step = 0;
		intState->data = 0;
	}
	else
	{
		/* run-length contains type + 3 elements */
		intState->currentEncodingType = RLE;
		intState->noOfLeftItems = type + 3;

		/* read the step used in the run */
		if (FileStreamReadByte(intState->stream, &intState->step))
		{
			return -1;
		}

		bytesRead = ReadVarLenInteger(intState->stream, &intState->data);
		if (bytesRead < 0)
		{
			/* stream is too short */
			return -1;
		}
	}
	return 0;
}

/**
 * Frees up a stream reader
 */
int StreamReaderFree(StreamReader* streamReader)
{
	if (streamReader == NULL)
	{
		return 0;
	}

	if (streamReader->stream != NULL)
	{
		if (FileStreamFree(streamReader->stream))
		{
			LogError("Error deleting previous compressed file stream\n");
			return -1;
		}
		streamReader->stream = NULL;
	}

	return 0;
}

/**
 * Initialize a stream reader
 *
 * @param streamReader reader to initialize
 * @param streamKind is the type of the field
 * @param offset is the offset of the data stream in the file
 * @param limit is the offset of the end of the stream
 * @param parameters contains compression format and compression block size
 *
 * @return 0 for success, 1 for failure
 */
int StreamReaderInit(StreamReader* streamReader, FieldType__Kind streamKind, FILE* file,
		long offset, long limit, CompressionParameters* parameters)
{

	if (streamReader->stream != NULL)
	{
		if (FileStreamFree(streamReader->stream))
		{
			LogError("Error deleting previous compressed file stream\n");
			return -1;
		}
		streamReader->stream = NULL;
	}

	streamReader->stream = FileStreamInit(file, offset, limit, parameters->compressionBlockSize,
			parameters->compressionKind);

	switch (streamKind)
	{
	case FIELD_TYPE__KIND__BOOLEAN:
		return BooleanReaderInit(streamReader);
	case FIELD_TYPE__KIND__BYTE:
		return ByteReaderInit(streamReader);
	case FIELD_TYPE__KIND__SHORT:
	case FIELD_TYPE__KIND__INT:
	case FIELD_TYPE__KIND__LONG:
		return IntegerReaderInit(streamKind, streamReader);
	case FIELD_TYPE__KIND__FLOAT:
	case FIELD_TYPE__KIND__DOUBLE:
	case FIELD_TYPE__KIND__BINARY:
		/* no need for initializer */
		return 0;
	default:
		return -1;
	}
}

/**
 * Reads a boolean value from the stream.
 *
 * @param booleanReaderState boolean reader
 *
 * @return 0 for false, 1 for true, -1 for error
 */
static char ReadBoolean(StreamReader* booleanReaderState)
{
	char result = 0;

	if (booleanReaderState->mask == 0)
	{
		if (booleanReaderState->noOfLeftItems == 0)
		{
			/* try to re-initialize the reader */
			int initResult = BooleanReaderInit(booleanReaderState);
			if (initResult)
			{
				/* error while reading from input stream */
				return -1;
			}
		}
		else
		{
			if (booleanReaderState->currentEncodingType == VARIABLE_LENGTH)
			{
				/* read next byte from the stream */
				if (FileStreamReadByte(booleanReaderState->stream, &result))
				{
					return -1;
				}
				booleanReaderState->data = (uint8_t) result;
			}

			booleanReaderState->mask = 0x80;
			booleanReaderState->noOfLeftItems--;
		}
	}

	result = (booleanReaderState->data & booleanReaderState->mask) ? 1 : 0;
	booleanReaderState->mask >>= 1;

	return result;
}

/**
 * Reads a byte from the stream.
 *
 * @param byteReaderState byte reader
 * @param result used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int ReadByte(StreamReader* byteReaderState, uint8_t *result)
{
	*result = 0;

	if (byteReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = ByteReaderInit(byteReaderState);

		if (initResult)
		{
			/* error while reading from input stream */
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (byteReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		if (FileStreamReadByte(byteReaderState->stream, (char*) result))
		{
			return -1;
		}
	}
	else
	{
		*result = byteReaderState->data;
	}

	byteReaderState->noOfLeftItems--;
	return 0;
}

/**
 * Reads an integer from the stream.
 *
 * @param kind to detect the sign
 * @param intReaderState integer reader
 * @param result used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int ReadInteger(FieldType__Kind kind, StreamReader* intReaderState, uint64_t *result)
{
	int bytesRead = 0;
	char step = 0;
	uint64_t data = 0;
	*result = 0;

	if (intReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = IntegerReaderInit(kind, intReaderState);
		if (initResult)
		{
			/* error while reading from input stream */
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (intReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		bytesRead = ReadVarLenInteger(intReaderState->stream, &intReaderState->data);
		if (bytesRead <= 0)
		{
			/* there have to be bytes left on the stream */
			return -1;
		}
		*result = intReaderState->data;
	}
	else
	{
		step = intReaderState->step;
		data = intReaderState->data;
		*result = data;
		switch (kind)
		{
		case FIELD_TYPE__KIND__SHORT:
		case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG:
			intReaderState->data = ToUnsignedInteger(ToSignedInteger(data) + step);
			break;
		default:
			intReaderState->data += step;
			break;
		}
	}
	intReaderState->noOfLeftItems--;

	return 0;
}

/**
 * Reads a float from the stream.
 *
 * @param fpState float reader
 * @param data used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int ReadFloat(StreamReader* fpState, float *data)
{
	int floatLength = sizeof(float);
	char* floatBytes = FileStreamRead(fpState->stream, &floatLength);
	if (floatBytes == NULL || floatLength != sizeof(float))
	{
		return -1;
	}
	memcpy(data, floatBytes, floatLength);
	fpState->floatData = *data;

	return 0;
}

/**
 * Reads a double from the stream.
 *
 * @param fpState double reader
 * @param data used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int ReadDouble(StreamReader* fpState, double *data)
{
	int doubleLength = sizeof(double);
	char* doubleBytes = FileStreamRead(fpState->stream, &doubleLength);
	if (doubleBytes == NULL || doubleLength != sizeof(double))
	{
		return -1;
	}
	memcpy(data, doubleBytes, doubleLength);
	fpState->floatData = *data;

	return 0;
}

/**
 * Reads an array of bytes from the input stream.
 *
 * @param binaryReaderState binary stream reader
 * @param data to put the value
 * @param length no of bytes to read
 *
 * @return 0 for success, -1 for error
 */
static int ReadBinary(StreamReader* binaryReaderState, uint8_t* data, int length)
{
	int requiredLength = length;
	char* bytes = FileStreamRead(binaryReaderState->stream, &length);

	if (bytes == NULL || requiredLength != length)
	{
		return -1;
	}

	memcpy(data, bytes, length);

	return 0;
}

/**
 * Helper function to read a primitive type from the reader
 *
 * @param reader
 * @param value to store the data
 * @param length to store the length of the data (used when necessary)
 *
 * @return 0 for success, 1 for NULL, -1 for failure
 */
static int ReadPrimitiveType(FieldReader* fieldReader, FieldValue* value, int* length)
{
	PrimitiveFieldReader* primitiveReader = (PrimitiveFieldReader*) fieldReader->fieldReader;

	StreamReader* presentStreamReader = &fieldReader->presentBitReader;
	StreamReader* booleanStreamReader = NULL;
	StreamReader* byteStreamReader = NULL;
	StreamReader* integerStreamReader = NULL;
	StreamReader* fpStreamReader = NULL;
	StreamReader* binaryReader = NULL;
	StreamReader *nanoSecondsReader = NULL;

	time_t millis = 0;
	uint64_t wordLength = 0;
	uint64_t index = 0;
	int64_t seconds = 0;
	uint64_t data64 = 0;
	float floatData = 0;
	double doubleData = 0;
	int dictionaryIterator = 0;
	int newNanos = 0;
	char isPresent = 0;
	int result = 0;

	if (fieldReader->hasPresentBitReader)
	{
		isPresent = ReadBoolean(presentStreamReader);
		if (isPresent == 0)
		{
			return 1;
		}
		else if (isPresent < 0)
		{
			return -1;
		}
	}

	switch (fieldReader->kind)
	{
	case FIELD_TYPE__KIND__BOOLEAN:
		booleanStreamReader = &primitiveReader->readers[DATA_STREAM];
		value->value8 = ReadBoolean(booleanStreamReader);
		result = value->value8 < 0 ? -1 : 0;
		break;
	case FIELD_TYPE__KIND__SHORT:
	case FIELD_TYPE__KIND__INT:
	case FIELD_TYPE__KIND__LONG:
		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(fieldReader->kind, integerStreamReader, &data64);
		value->value64 = ToSignedInteger(data64);
		break;
	case FIELD_TYPE__KIND__FLOAT:
		fpStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadFloat(fpStreamReader, &floatData);
		value->doubleValue = floatData;
		break;
	case FIELD_TYPE__KIND__DOUBLE:
		fpStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadDouble(fpStreamReader, &doubleData);
		value->doubleValue = doubleData;
		break;
	case FIELD_TYPE__KIND__STRING:
		if (primitiveReader->dictionary == NULL)
		{
			/* if dictionary is NULL, read the whole dictionary to the memory */
			primitiveReader->dictionary =
			alloc(sizeof(char*) * primitiveReader->dictionarySize);
			primitiveReader->wordLength =
			alloc(sizeof(int) * primitiveReader->dictionarySize);

			integerStreamReader = &primitiveReader->readers[LENGTH_STREAM];
			binaryReader = &primitiveReader->readers[DICTIONARY_DATA_STREAM];

			/* read the dictionary */
			for (dictionaryIterator = 0; dictionaryIterator < primitiveReader->dictionarySize;
					++dictionaryIterator)
			{
				result = ReadInteger(fieldReader->kind, integerStreamReader, &wordLength);

				if (result < 0)
				{
					return -1;
				}

				primitiveReader->wordLength[dictionaryIterator] = (int) wordLength;
				primitiveReader->dictionary[dictionaryIterator] = alloc(wordLength + 1);
				result = ReadBinary(binaryReader,
						(uint8_t*) primitiveReader->dictionary[dictionaryIterator],
						(int) wordLength);
				primitiveReader->dictionary[dictionaryIterator][wordLength] = '\0';

				if (result < 0)
				{
					return -1;
				}
			}
		}

		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(fieldReader->kind, integerStreamReader, &index);
		value->binary = primitiveReader->dictionary[index];
		break;
	case FIELD_TYPE__KIND__BYTE:
		byteStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadByte(byteStreamReader, &value->value8);
		break;
	case FIELD_TYPE__KIND__BINARY:
		integerStreamReader = &primitiveReader->readers[LENGTH_STREAM];
		binaryReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(fieldReader->kind, integerStreamReader, &wordLength);

		if (result < 0)
		{
			return -1;
		}

		*length = (int) wordLength;
		value->binary = alloc(wordLength);
		result = ReadBinary(binaryReader, (uint8_t*) value->binary, (int) wordLength);
		break;
	case FIELD_TYPE__KIND__TIMESTAMP:
		/* seconds primitiveReader */
		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(FIELD_TYPE__KIND__LONG, integerStreamReader, &data64);
		seconds = ToSignedInteger(data64);

		/* nano seconds primitiveReader */
		nanoSecondsReader = &primitiveReader->readers[SECONDARY_STREAM];
		result |= ReadInteger(FIELD_TYPE__KIND__INT, nanoSecondsReader, &data64);
		newNanos = ParseNanos((long) data64);

		millis = (ORC_EPOCH_IN_SECONDS + seconds) * 1000;

		if (millis >= 0)
		{
			millis += newNanos / 1000000;
		}
		else
		{
			millis -= newNanos / 1000000;
		}

		value->time.tv_sec = millis / 1000;
		value->time.tv_nsec = newNanos;
		break;
	case FIELD_TYPE__KIND__DECIMAL:
	case FIELD_TYPE__KIND__UNION:
		result = -1;
		break;
	default:
		result = -1;
		break;
	}

	return result;
}

/*
 * Helper function to read list from the stream.
 *
 * @param fieldReader field to read
 * @param field structure to put the values
 * @param length pointer to store the length of the list
 *
 * @return -1 for error, 0 for not-null and 1 for null
 */
static int ReadListItem(FieldReader* fieldReader, Field* field, int* length)
{
	ListFieldReader* listReader = fieldReader->fieldReader;
	FieldReader* itemReader = &listReader->itemReader;
	StreamReader* presentStreamReader = &fieldReader->presentBitReader;
	uint64_t listSize = 0;
	int result = 0;
	int iterator = 0;
	char isPresent = 0;
	FieldValue* value = NULL;

	if (fieldReader->hasPresentBitReader && (isPresent = ReadBoolean(presentStreamReader)) == 0)
	{
		/* not present, return 1 as null */
		return 1;
	}
	else if (isPresent == -1)
	{
		/* error occured while reading bit, return error code */
		return -1;
	}

	result = ReadInteger(fieldReader->kind, &listReader->lengthReader, &listSize);
	if (result)
	{
		/* error while reading the list size */
		return -1;
	}

	field->list = alloc(sizeof(FieldValue) * listSize);
	*length = (int) listSize;
	field->isItemNull = alloc(sizeof(char) * listSize);

	if (itemReader->kind == FIELD_TYPE__KIND__BINARY)
	{
		field->listItemSizes = alloc(sizeof(int) * listSize);
	}

	for (iterator = 0; iterator < listSize; ++iterator)
	{
		value = &field->list[iterator];
		result = ReadPrimitiveType(itemReader, value, &field->listItemSizes[iterator]);
		if (result < 0)
		{
			/* error while reading the list item */
			return -1;
		}
		field->isItemNull[iterator] = result;
	}

	return 0;
}

/*
 * Reads one field from the file
 *
 * @param fieldReader field to read
 * @param field structure to put the values
 * @param length length of the list or the size of the binary field
 *
 * @return -1 for error, 0 for not-null, 1 for null
 */
int FieldReaderRead(FieldReader* fieldReader, Field* field, int* length)
{
	field->list = NULL;
	field->listItemSizes = NULL;
	field->isItemNull = NULL;

	if (fieldReader->kind == FIELD_TYPE__KIND__LIST)
	{
		return ReadListItem(fieldReader, field, length);
	}
	else if (!IsComplexType(fieldReader->kind))
	{
		return ReadPrimitiveType(fieldReader, &field->value, length);
	}
	else
	{
		return -1;
	}
}

/**
 * Get the stream type of each data stream of a data type
 */
FieldType__Kind GetStreamKind(FieldType__Kind type, int streamIndex)
{
	switch (type)
	{
	case FIELD_TYPE__KIND__BINARY:
		switch (streamIndex)
		{
		case 0:
			return FIELD_TYPE__KIND__BINARY;
		case 1:
			return FIELD_TYPE__KIND__INT;
		default:
			return -1;
		}
		break;
	case FIELD_TYPE__KIND__BOOLEAN:
	case FIELD_TYPE__KIND__BYTE:
	case FIELD_TYPE__KIND__DOUBLE:
	case FIELD_TYPE__KIND__FLOAT:
	case FIELD_TYPE__KIND__SHORT:
	case FIELD_TYPE__KIND__INT:
	case FIELD_TYPE__KIND__LONG:
		return (streamIndex == 0) ? type : -1;
	case FIELD_TYPE__KIND__STRING:
		switch (streamIndex)
		{
		case 0:
		case 1:
			return FIELD_TYPE__KIND__INT;
		case 2:
			return FIELD_TYPE__KIND__BINARY;
		default:
			return -1;
		}
		break;
	case FIELD_TYPE__KIND__TIMESTAMP:
		switch (streamIndex)
		{
		case 0:
			return FIELD_TYPE__KIND__LONG;
		case 1:
			return FIELD_TYPE__KIND__INT;
		default:
			return -1;
		}
		break;
	case FIELD_TYPE__KIND__LIST:
		return streamIndex ? -1 : FIELD_TYPE__KIND__INT;
	default:
		return -1;
	}
}

/**
 * Get # data streams of a data type (excluding the present bit stream)
 */
int GetStreamCount(FieldType__Kind type)
{
	switch (type)
	{
	case FIELD_TYPE__KIND__BINARY:
		return BINARY_STREAM_COUNT;
	case FIELD_TYPE__KIND__BOOLEAN:
	case FIELD_TYPE__KIND__BYTE:
	case FIELD_TYPE__KIND__DOUBLE:
	case FIELD_TYPE__KIND__FLOAT:
	case FIELD_TYPE__KIND__SHORT:
	case FIELD_TYPE__KIND__INT:
	case FIELD_TYPE__KIND__LONG:
		return COMMON_STREAM_COUNT;
	case FIELD_TYPE__KIND__STRING:
		return STRING_STREAM_COUNT;
	case FIELD_TYPE__KIND__TIMESTAMP:
		return TIMESTAMP_STREAM_COUNT;
	case FIELD_TYPE__KIND__LIST:
		/* for length */
		return 1;
	case FIELD_TYPE__KIND__STRUCT:
		return 0;
	default:
		return -1;
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

/*
 * Reads a primitive field from the reader and returns it as a Datum.
 *
 * @param isNull pointer to store whether the field is null or not
 */
Datum ReadPrimitiveFieldAsDatum(FieldReader* fieldReader, bool *isNull)
{
	Datum columnValue = 0;
	PrimitiveFieldReader* primitiveReader = (PrimitiveFieldReader*) fieldReader->fieldReader;

	StreamReader* presentStreamReader = &fieldReader->presentBitReader;
	StreamReader* booleanStreamReader = NULL;
	StreamReader* integerStreamReader = NULL;
	StreamReader* fpStreamReader = NULL;
	StreamReader* binaryReader = NULL;
	StreamReader *nanoSecondsReader = NULL;

	char isPresent = 0;
	int64_t data64 = 0;
	uint64_t udata64 = 0;
	float floatData = 0;
	double doubleData = 0;
	char* dictionaryItem = NULL;
	int dictionaryIterator = 0;
	uint64_t wordLength = 0;
	int64_t seconds = 0;
	int newNanos = 0;
	int result = 0;

	if (fieldReader->hasPresentBitReader)
	{
		isPresent = ReadBoolean(presentStreamReader);
		if (isPresent == 0)
		{
			*isNull = true;
			return 0;
		}
		else if (isPresent < 0)
		{
			LogError("Error while reading present bit stream");
			return -1;
		}
	}

	switch (fieldReader->psqlKind)
	{
	case BOOLOID:
	{
		booleanStreamReader = &primitiveReader->readers[DATA_STREAM];
		columnValue = BoolGetDatum(ReadBoolean(booleanStreamReader));
		break;
	}
	case INT2OID:
	case INT4OID:
	case INT8OID:
	{
		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(fieldReader->kind, integerStreamReader, &udata64);
		data64 = ToSignedInteger(udata64);
		switch (fieldReader->psqlKind)
		{
		case INT2OID:
			columnValue = Int16GetDatum(data64);
			break;
		case INT4OID:
			columnValue = Int32GetDatum(data64);
			break;
		case INT8OID:
			columnValue = Int64GetDatum(data64);
			break;
		default:
			break;
		}
		break;
	}
	case FLOAT4OID:
	case FLOAT8OID:
	{
		fpStreamReader = &primitiveReader->readers[DATA_STREAM];
		if (fieldReader->kind == FIELD_TYPE__KIND__FLOAT)
		{
			result = ReadFloat(fpStreamReader, &floatData);
			doubleData = floatData;
		}
		else
		{
			result = ReadDouble(fpStreamReader, &doubleData);
		}
		columnValue = Float8GetDatum(doubleData);
		break;
	}
	case BPCHAROID:
	case VARCHAROID:
	case TEXTOID:
	{
		if (primitiveReader->dictionary == NULL)
		{
			/* if dictionary is NULL, read the whole dictionary to the memory */
			primitiveReader->dictionary =
			alloc(sizeof(char*) * primitiveReader->dictionarySize);
			primitiveReader->wordLength =
			alloc(sizeof(int) * primitiveReader->dictionarySize);

			integerStreamReader = &primitiveReader->readers[LENGTH_STREAM];
			binaryReader = &primitiveReader->readers[DICTIONARY_DATA_STREAM];

			/* read the dictionary */
			for (dictionaryIterator = 0; dictionaryIterator < primitiveReader->dictionarySize;
					++dictionaryIterator)
			{
				result = ReadInteger(fieldReader->kind, integerStreamReader, &wordLength);
				if (result < 0)
				{
					LogError("Error while reading dictionary item length");
					return -1;
				}

				primitiveReader->wordLength[dictionaryIterator] = (int) wordLength;
				primitiveReader->dictionary[dictionaryIterator] = alloc(wordLength + 1);
				result = ReadBinary(binaryReader,
						(uint8_t*) primitiveReader->dictionary[dictionaryIterator],
						(int) wordLength);
				primitiveReader->dictionary[dictionaryIterator][wordLength] = '\0';
				if (result < 0)
				{
					LogError("Error while reading dictionary item");
					return -1;
				}
			}
		}

		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(fieldReader->kind, integerStreamReader, &udata64);
		dictionaryItem = primitiveReader->dictionary[udata64];

		switch (fieldReader->psqlKind)
		{
		case BPCHAROID:
		{
			columnValue = DirectFunctionCall3(bpcharin, CStringGetDatum(dictionaryItem),
					ObjectIdGetDatum(InvalidOid),
					Int32GetDatum(fieldReader->columnTypeMod));
			break;
		}
		case VARCHAROID:
		{
			columnValue = DirectFunctionCall3(varcharin, CStringGetDatum(dictionaryItem),
					ObjectIdGetDatum(InvalidOid),
					Int32GetDatum(fieldReader->columnTypeMod));
			break;
		}
		case TEXTOID:
		{
			columnValue = CStringGetTextDatum(dictionaryItem);
			break;
		}
		default:
			break;
		}
		break;
	}
	case DATEOID:
	case TIMESTAMPOID:
	{
		/* seconds primitiveReader */
		integerStreamReader = &primitiveReader->readers[DATA_STREAM];
		result = ReadInteger(FIELD_TYPE__KIND__LONG, integerStreamReader, &udata64);
		seconds = ToSignedInteger(udata64);
		seconds += ORC_DIFF_POSTGRESQL;

		if (fieldReader->psqlKind == DATEOID)
		{
			/* if this is a date type, don't read nsec stream at all */
			/* TODO PostgreSQL adds one day, why ? */
			columnValue = DateADTGetDatum(seconds / SECONDS_PER_DAY - 1);
		}
		else
		{
			/* nano seconds primitiveReader */
			nanoSecondsReader = &primitiveReader->readers[SECONDARY_STREAM];
			result |= ReadInteger(FIELD_TYPE__KIND__INT, nanoSecondsReader, &udata64);
			newNanos = ParseNanos((long) udata64);

			columnValue = TimestampGetDatum(seconds * MICROSECONDS_PER_SECOND +
					newNanos / NANOSECONDS_PER_MICROSECOND);
		}
		break;
	}
	case NUMERICOID:
	default:
		result = -1;
		break;
	}

	if (result)
	{
		LogError("Error while reading column");
	}

	*isNull = false;
	return columnValue;
}

/*
 * Reads a list field from the reader and returns it as a Datum.
 *
 * @param isNull pointer to store whether the list is null or not
 */
Datum ReadListFieldAsDatum(FieldReader* fieldReader, bool *isNull)
{
	Datum columnValue = 0;
	Datum *datumArray = NULL;
	int datumArraySize = 0;
	uint64_t listSize = 0;
	ArrayType *columnValueObject = NULL;
	ListFieldReader* listReader = fieldReader->fieldReader;
	FieldReader* itemReader = &listReader->itemReader;
	StreamReader* presentStreamReader = &fieldReader->presentBitReader;
	int result = 0;
	int iterator = 0;
	char isPresent = 0;
	bool isItemNull = 0;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	if (fieldReader->hasPresentBitReader && (isPresent = ReadBoolean(presentStreamReader)) == 0)
	{
		/* list not present, return null */
		*isNull = true;
		return 0;
	}
	else if (isPresent == -1)
	{
		/* error occured while reading bit, return error code */
		LogError("Error while reading pressent bit");
		return -1;
	}

	result = ReadInteger(fieldReader->kind, &listReader->lengthReader, &listSize);
	if (result)
	{
		/* error while reading the list size */
		LogError("Error while reading the length of the list");
		return -1;
	}

	/* allocate enough room for datum array's maximum possible size */
	datumArray = palloc0(listSize * sizeof(Datum));
	memset(datumArray, 0, listSize * sizeof(Datum));

	for (iterator = 0; iterator < listSize; ++iterator)
	{
		columnValue = ReadPrimitiveFieldAsDatum(itemReader, &isItemNull);
		if (isItemNull)
		{
			/* TODO currently null values in the list are skipped, is this OK? */
			continue;
		}
		datumArray[datumArraySize] = columnValue;
		datumArraySize++;
	}

	get_typlenbyvalalign(itemReader->psqlKind, &typeLength, &typeByValue, &typeAlignment);
	columnValueObject = construct_array(datumArray, datumArraySize, itemReader->psqlKind,
			typeLength, typeByValue, typeAlignment);
	columnValue = PointerGetDatum(columnValueObject);

	*isNull = false;
	return columnValue;
}
