#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "orc_proto.pb-c.h"
#include "recordReader.h"

struct tm BASE_TIMESTAMP =
{ .tm_sec = 0, .tm_min = 0, .tm_hour = 0, .tm_mday = 1, .tm_wday = 3, .tm_mon = 0, .tm_year = 115 };

static int parseNanos(long serialized)
{
	int zeros = 7 & (int) serialized;
	int result = (int) serialized >> 3;
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
 * Reads the variable-length integer from the stream and puts it into data.
 * Return -1 for error or positive for no of bytes read.
 */
static long readVarLenInteger(CompressedFileStream* stream, uint64_t *data)
{
	*data = 0;
	int shift = 0;
	long bytesRead = 0;
	char byte = 0;

	if (CompressedFileStream_readByte(stream, &byte))
	{
		return -1;
	}

	while ((byte & 0x80) != 0)
	{
		*data |= (uint64_t) (byte & 0x7F) << shift;
		shift += 7;
		bytesRead++;

		if (CompressedFileStream_readByte(stream, &byte))
		{
			return -1;
		}
	}
	*data |= (uint64_t) byte << shift;

	return ++bytesRead;
}

/**
 * Initialize a boolean reader for reading.
 */
static int initBooleanReader(StreamReader* boolState)
{
	char type = 0;

	if (CompressedFileStream_readByte(boolState->stream, &type))
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

	if (CompressedFileStream_readByte(boolState->stream, &type))
	{
		return -1;
	}

	boolState->data = (uint8_t) type;
	boolState->mask = 0x80;

	return 0;
}

/**
 * Initialize an var-length integer reader for reading.
 */
static int initByteReader(StreamReader* byteState)
{
	char type = 0;

	if (CompressedFileStream_readByte(byteState->stream, &type))
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

		if (CompressedFileStream_readByte(byteState->stream, &type))
		{
			return -1;
		}
		byteState->data = (uint8_t) type;
	}

	return 0;
}

/**
 * Initialize an var-length integer reader for reading.
 */
static int initIntegerReader(Type__Kind kind, StreamReader* intState)
{
	char type = 0;
	long bytesRead = 0;

	if (CompressedFileStream_readByte(intState->stream, &type))
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

		if (CompressedFileStream_readByte(intState->stream, &intState->step))
		{
			return -1;
		}

		bytesRead = readVarLenInteger(intState->stream, &intState->data);
		if (bytesRead < 0)
		{
			/* stream is too short */
			fprintf(stderr, "Error while reading var-len int from stream!\n");
			return -1;
		}
	}
	return 0;
}

int initStreamReader(Type__Kind streamKind, StreamReader* streamReader, char* fileName, long offset, long limit,
		CompressionParameters* parameters)
{
	streamReader->stream = CompressedFileStream_init(fileName, offset, limit, parameters->compressionBlockSize,
			parameters->compressionKind);

	switch (streamKind)
	{
	case TYPE__KIND__BOOLEAN:
		return initBooleanReader(streamReader);
	case TYPE__KIND__BYTE:
		return initByteReader(streamReader);
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		return initIntegerReader(streamKind, streamReader);
	case TYPE__KIND__FLOAT:
	case TYPE__KIND__DOUBLE:
	case TYPE__KIND__BINARY:
		/* no need for initializer */
		return 0;
	default:
		return -1;
	}
}

/**
 * Reads a boolean value from the stream.
 * 0,1 : false or true value
 *  -1 : error
 */
static char readBoolean(StreamReader* booleanReaderState)
{
	char result = 0;

	if (booleanReaderState->mask == 0)
	{
		if (booleanReaderState->noOfLeftItems == 0)
		{
			/* try to re-initialize the reader */
			int initResult = initBooleanReader(booleanReaderState);
			if (initResult)
			{
				/* error while reading from input stream */
				fprintf(stderr, "Error while re-initializing the boolean reader");
				return -1;
			}
		}
		else
		{
			if (booleanReaderState->currentEncodingType == VARIABLE_LENGTH)
			{
				/* read next byte from the stream */
				if (CompressedFileStream_readByte(booleanReaderState->stream, &result))
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
 * Reads an integer from the stream.
 * Conversion from mixed sign to actual sign isn't done.
 */
static int readByte(StreamReader* byteReaderState, uint8_t *result)
{
	*result = 0;

	if (byteReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = initByteReader(byteReaderState);

		if (initResult)
		{
			/* error while reading from input stream */
			fprintf(stderr, "Error while re-initializing the byte reader");
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (byteReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		if (CompressedFileStream_readByte(byteReaderState->stream, (char*) result))
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
 * Conversion from mixed sign to actual sign isn't done.
 */
static int readInteger(Type__Kind kind, StreamReader* intReaderState, uint64_t *result)
{
	*result = 0;
	long bytesRead = 0;
	char step = 0;
	uint64_t data = 0;

	if (intReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = initIntegerReader(kind, intReaderState);
		if (initResult)
		{
			/* error while reading from input stream */
			fprintf(stderr, "Error while re-initializing the integer reader");
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (intReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		bytesRead = readVarLenInteger(intReaderState->stream, &intReaderState->data);
		if (bytesRead <= 0)
		{
			/* there have to be bytes left on the stream */
			fprintf(stderr, "Byte stream ended prematurely!");
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
		case TYPE__KIND__SHORT:
		case TYPE__KIND__INT:
		case TYPE__KIND__LONG:
			intReaderState->data = toUnsignedInteger(
					toSignedInteger(data) + step);
			break;
		default:
			intReaderState->data += step;
			break;
		}
	}
	intReaderState->noOfLeftItems--;

	return 0;
}

static int readFloat(StreamReader* fpState, float *data)
{
	int floatLength = sizeof(float);
	char* floatBytes = CompressedFileStream_read(fpState->stream, &floatLength);
	if (floatBytes == NULL || floatLength != sizeof(float))
	{
		return 1;
	}
	memcpy(data, floatBytes, floatLength);
	fpState->floatData = *data;

	return 0;
}

static int readDouble(StreamReader* fpState, double *data)
{
	int doubleLength = sizeof(double);
	char* doubleBytes = CompressedFileStream_read(fpState->stream, &doubleLength);
	if (doubleBytes == NULL || doubleLength != sizeof(double))
	{
		return 1;
	}
	memcpy(data, doubleBytes, doubleLength);
	fpState->floatData = *data;

	return 0;
}

/**
 * Reads a list of bytes from the input stream.
 */
static int readBinary(StreamReader* binaryReaderState, uint8_t* data, int length)
{
	int requiredLength = length;
	char* bytes = CompressedFileStream_read(binaryReaderState->stream, &length);
	if (bytes == NULL || requiredLength != length)
	{
		return 1;
	}

	memcpy(data, bytes, length);

	return 0;
}

static int readPrimitiveType(Reader* reader, FieldValue* value, int* length)
{
	PrimitiveReader* primitiveReader = (PrimitiveReader*) reader->fieldReader;

	StreamReader* presentStreamReader = &reader->presentBitReader;
	StreamReader* booleanStreamReader = NULL;
	StreamReader* byteStreamReader = NULL;
	StreamReader* integerStreamReader = NULL;
	StreamReader* fpStreamReader;
	StreamReader* binaryReader = NULL;
	StreamReader *nanoSecondsReader = NULL;

	time_t millis;
	uint64_t wordLength = 0;
	uint64_t index = 0;
	uint64_t seconds = 0;
	uint64_t nanoSeconds = 0;
	uint64_t data64 = 0;
	int dictionaryIterator = 0;
	char isPresent = 0;
	int result = 0;

	if (reader->hasPresentBitReader && (isPresent = readBoolean(presentStreamReader)) == 0)
	{
		/* not present, return 1 as null */
		return 1;
	}
	else if (isPresent == -1)
	{
		/* error occured while reading bit, return error code */
		return -1;
	}

	switch (reader->kind)
	{
	case TYPE__KIND__BOOLEAN:
		booleanStreamReader = &primitiveReader->readers[DATA];
		value->value8 = readBoolean(booleanStreamReader);
		result = value->value8 < 0 ? -1 : 0;
		break;
	case TYPE__KIND__BYTE:
		byteStreamReader = &primitiveReader->readers[DATA];
		result = readByte(byteStreamReader, &value->value8);
		break;
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		integerStreamReader = &primitiveReader->readers[DATA];
		result = readInteger(reader->kind, integerStreamReader, &data64);
		value->value64 = toSignedInteger(data64);
		break;
	case TYPE__KIND__FLOAT:
		fpStreamReader = &primitiveReader->readers[DATA];
		result = readFloat(fpStreamReader, &value->floatValue);
		break;
	case TYPE__KIND__DOUBLE:
		fpStreamReader = &primitiveReader->readers[DATA];
		result = readDouble(fpStreamReader, &value->doubleValue);
		break;
	case TYPE__KIND__STRING:
		if (primitiveReader->dictionary == NULL)
		{
			primitiveReader->dictionary = malloc(sizeof(char*) * primitiveReader->dictionarySize);
			primitiveReader->wordLength = malloc(sizeof(int) * primitiveReader->dictionarySize);

			integerStreamReader = &primitiveReader->readers[LENGTH];
			binaryReader = &primitiveReader->readers[DICTIONARY_DATA];

			/* read the dictionary */
			for (dictionaryIterator = 0; dictionaryIterator < primitiveReader->dictionarySize; ++dictionaryIterator)
			{
				result = readInteger(reader->kind, integerStreamReader, &wordLength);
				primitiveReader->wordLength[dictionaryIterator] = (int) wordLength;
				primitiveReader->dictionary[dictionaryIterator] = malloc(wordLength + 1);
				result |= readBinary(binaryReader, (uint8_t*) primitiveReader->dictionary[dictionaryIterator],
						(int) wordLength);
				primitiveReader->dictionary[dictionaryIterator][wordLength] = '\0';

				if (result < 0)
				{
					return -1;
				}
			}
		}

		integerStreamReader = &primitiveReader->readers[DATA];
		result = readInteger(reader->kind, integerStreamReader, &index);
		value->binary = primitiveReader->dictionary[index];
		break;
	case TYPE__KIND__BINARY:
		integerStreamReader = &primitiveReader->readers[LENGTH];
		binaryReader = &primitiveReader->readers[DATA];

		result = readInteger(reader->kind, integerStreamReader, &wordLength);
		*length = (int) wordLength;
		value->binary = malloc(wordLength);
		readBinary(binaryReader, (uint8_t*) value->binary, (int) wordLength);
		break;
	case TYPE__KIND__TIMESTAMP:

		/* seconds primitiveReader */
		integerStreamReader = &primitiveReader->readers[DATA];
		result = readInteger(TYPE__KIND__LONG, integerStreamReader, &seconds);
		seconds = toSignedInteger(seconds);

		/* nano seconds primitiveReader */
		nanoSecondsReader = &primitiveReader->readers[SECONDARY];
		result |= readInteger(TYPE__KIND__INT, nanoSecondsReader, &nanoSeconds);
		int newNanos = parseNanos((long) nanoSeconds);

		millis = (mktime(&BASE_TIMESTAMP) + seconds) * 1000;
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
	default:
		result = -1;
		break;
	}

	return result;
}

static int readListElement(Reader* reader, Field* field, int* length)
{
	ListReader* listReader = reader->fieldReader;
	Reader* itemReader = &listReader->itemReader;
	StreamReader* presentStreamReader = &reader->presentBitReader;
	uint64_t listSize = 0;
	int result = 0;
	int iterator = 0;
	char isPresent = 0;
	FieldValue* value = NULL;

	if (reader->hasPresentBitReader && (isPresent = readBoolean(presentStreamReader)) == 0)
	{
		/* not present, return 1 as null */
		return 1;
	}
	else if (isPresent == -1)
	{
		/* error occured while reading bit, return error code */
		return -1;
	}

	result = readInteger(reader->kind, &listReader->lengthReader, &listSize);
	if (result)
	{
		/* error while reading the list size */
		return 1;
	}

	field->list = malloc(sizeof(FieldValue) * listSize);
	*length = (int) listSize;
	field->isItemNull = malloc(sizeof(char) * listSize);

	if (itemReader->kind == TYPE__KIND__BINARY)
	{
		field->listItemSizes = malloc(sizeof(int) * listSize);
	}

	for (iterator = 0; iterator < listSize; ++iterator)
	{
		value = &field->list[iterator];
		result = readPrimitiveType(itemReader, value, &field->listItemSizes[iterator]);
		if (result < 0)
		{
			/* error while reading the list item */
			return 1;
		}
		field->isItemNull[iterator] = result;
	}

	return 0;
}

int readField(Reader* reader, Field* field, int* length)
{
	field->list = NULL;
	field->listItemSizes = NULL;
	field->isItemNull = NULL;

	switch (reader->kind)
	{
	case TYPE__KIND__LIST:
		return readListElement(reader, field, length);
	default:
		return readPrimitiveType(reader, &field->value, length);
	}
}

/**
 * Get the stream type of each data stream of a data type
 */
Type__Kind getStreamKind(Type__Kind type, int streamIndex)
{
	switch (type)
	{
	case TYPE__KIND__BINARY:
		switch (streamIndex)
		{
		case 0:
			return TYPE__KIND__BINARY;
		case 1:
			return TYPE__KIND__INT;
		default:
			return -1;
		}
		break;
	case TYPE__KIND__BOOLEAN:
	case TYPE__KIND__BYTE:
	case TYPE__KIND__DOUBLE:
	case TYPE__KIND__FLOAT:
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		return (streamIndex == 0) ? type : -1;
	case TYPE__KIND__STRING:
		switch (streamIndex)
		{
		case 0:
		case 1:
			return TYPE__KIND__INT;
		case 2:
			return TYPE__KIND__BINARY;
		default:
			return -1;
		}
		break;
	case TYPE__KIND__TIMESTAMP:
		switch (streamIndex)
		{
		case 0:
			return TYPE__KIND__LONG;
		case 1:
			return TYPE__KIND__INT;
		default:
			return -1;
		}
		break;
	case TYPE__KIND__LIST:
		return streamIndex ? -1 : TYPE__KIND__INT;
	default:
		return -1;
	}
}

/**
 * Get # data streams of a data type (excluding the present bit stream)
 */
int getStreamCount(Type__Kind type)
{
	switch (type)
	{
	case TYPE__KIND__BINARY:
		return BINARY_STREAM_COUNT;
	case TYPE__KIND__BOOLEAN:
	case TYPE__KIND__BYTE:
	case TYPE__KIND__DOUBLE:
	case TYPE__KIND__FLOAT:
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		return COMMON_STREAM_COUNT;
	case TYPE__KIND__STRING:
		return STRING_STREAM_COUNT;
	case TYPE__KIND__TIMESTAMP:
		return TIMESTAMP_STREAM_COUNT;
	case TYPE__KIND__LIST:
		/* for length */
		return 1;
	default:
		return -1;
	}
}

void freePrimitiveReader(PrimitiveReader* reader)
{
	int iterator = 0;
	if (reader->dictionary)
	{
		for (iterator = 0; iterator < reader->dictionarySize; ++iterator)
		{
			free(reader->dictionary[iterator]);
		}
		free(reader->dictionary);
		free(reader->wordLength);

		reader->dictionary = NULL;
		reader->wordLength = NULL;
	}
	for (iterator = 0; iterator < MAX_STREAM_COUNT; ++iterator)
	{
		if (reader->readers[iterator].stream != NULL)
		{
			free(reader->readers[iterator].stream);
			reader->readers[iterator].stream = NULL;
		}
	}
}

void freeStructReader(StructReader* structReader)
{
	Reader* reader = NULL;
	int iterator = 0;

	for (iterator = 0; iterator < structReader->noOfFields; ++iterator)
	{
		reader = structReader->fields[iterator];
		assert(reader->kind != TYPE__KIND__STRUCT || reader->kind != TYPE__KIND__LIST);
		freePrimitiveReader(reader->fieldReader);
		free(reader);
	}
	free(structReader->fields);
}
