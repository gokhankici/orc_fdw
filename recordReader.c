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
static long readVarLenInteger(uint8_t* stream, long streamLength, uint64_t *data)
{
	*data = 0;
	int shift = 0;
	long bytesRead = 0;

	if (streamLength <= 0)
	{
		/* not enough bytes in the stream */
		return -1;
	}

	while (((*stream) & 0x80) != 0)
	{
		if (--streamLength < 1)
		{
			/* not enough bytes in the stream */
			return -1;
		}

		*data |= (uint64_t) (*(stream++) & 0x7F) << shift;
		shift += 7;
		bytesRead++;
	}
	*data |= (uint64_t) *stream << shift;

	return ++bytesRead;
}

/**
 * Initialize a boolean reader for reading.
 */
static int initBooleanReader(StreamReader* boolState, uint8_t* stream, long streamLength)
{
	char type = 0;

	if (streamLength < 2)
	{
		/* stream is too short */
		fprintf(stderr, "Stream is too short!\n");
		return -1;
	}

	type = (char) *(stream++);

	if (type < 0)
	{
		/* -type var-len integers follow */
		boolState->currentEncodingType = VARIABLE_LENGTH;
		boolState->noOfLeftItems = -type - 1;

		if (streamLength - 1 < boolState->noOfLeftItems)
		{
			/* there have to more bytes left on the stream */
			fprintf(stderr, "Byte stream ended prematurely!");
			return -1;
		}
	}
	else
	{
		/* run-length contains type + 3 elements */
		boolState->currentEncodingType = RLE;
		boolState->noOfLeftItems = type + 3 - 1;
	}

	boolState->data = *(stream++);
	boolState->mask = 0x80;
	boolState->streamPointer = stream;
	/* for both cases we read 2 bytes */
	boolState->streamLength = streamLength - 2;

	return 0;
}

/**
 * Initialize an var-length integer reader for reading.
 */
static int initByteReader(StreamReader* byteState, uint8_t* stream, long streamLength)
{
	char type = 0;

	if (streamLength < 2)
	{
		/* stream is too short */
		fprintf(stderr, "Stream is too short!\n");
		return -1;
	}

	type = (char) *(stream++);

	if (type < 0)
	{
		/* -type var-len integers follow */
		byteState->currentEncodingType = VARIABLE_LENGTH;
		byteState->noOfLeftItems = -type;
		byteState->streamPointer = stream;
		byteState->streamLength = streamLength - 1;
		byteState->data = 0;
		byteState->step = 0;

		if (byteState->streamLength < byteState->noOfLeftItems)
		{
			/* there have to be bytes left on the stream */
			fprintf(stderr, "Byte stream ended prematurely!");
			return -1;
		}
	}
	else
	{
		/* run-length contains type + 3 elements */
		byteState->currentEncodingType = RLE;
		byteState->noOfLeftItems = type + 3;

		/* step is always 0 for byte streams */
		byteState->step = 0;
		byteState->data = *stream++;
		byteState->streamPointer = stream;
		byteState->streamLength = streamLength - 2;
	}

	return 0;
}

/**
 * Initialize an var-length integer reader for reading.
 */
static int initIntegerReader(Type__Kind kind, StreamReader* intState, uint8_t* stream, long streamLength)
{
	char type = 0;
	long bytesRead = 0;

	if (streamLength < 2)
	{
		/* stream is too short */
		fprintf(stderr, "Stream is too short!\n");
		return -1;
	}

	type = (char) *(stream++);

	if (type < 0)
	{
		/* -type var-len integers follow */
		intState->currentEncodingType = VARIABLE_LENGTH;
		intState->noOfLeftItems = -type;
		intState->streamPointer = stream;
		intState->streamLength = streamLength - 1;
		intState->step = 0;
		intState->data = 0;
	}
	else
	{
		/* run-length contains type + 3 elements */
		intState->currentEncodingType = RLE;
		intState->noOfLeftItems = type + 3;

		intState->step = *stream++;
		bytesRead = readVarLenInteger(stream, streamLength - 2, &intState->data);
		if (bytesRead < 0)
		{
			/* stream is too short */
			fprintf(stderr, "Error while reading var-len int from stream!\n");
			return -1;
		}

		intState->streamPointer = stream + bytesRead;
		intState->streamLength = streamLength - bytesRead - 2;
	}

	return 0;
}

/**
 * Initialize an floating point reader for reading.
 */
static int initFPReader(StreamReader* fpState, uint8_t* stream, long streamLength)
{
	fpState->streamPointer = stream;
	fpState->streamLength = streamLength;
	if (streamLength % 4)
	{
		/* length has to be a multiple of 4 */
		return -1;
	}
	return 0;
}

static int initBinaryReader(StreamReader* binaryState, uint8_t* stream, long streamLength)
{
	binaryState->streamPointer = stream;
	binaryState->streamLength = streamLength;
	return 0;
}

int initStreamReader(Type__Kind streamKind, StreamReader* streamReader, uint8_t* stream, long streamLength)
{
	streamReader->stream = stream;

	switch (streamKind)
	{
	case TYPE__KIND__BOOLEAN:
		return initBooleanReader(streamReader, stream, streamLength);
	case TYPE__KIND__BYTE:
		return initByteReader(streamReader, stream, streamLength);
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		return initIntegerReader(streamKind, streamReader, stream, streamLength);
	case TYPE__KIND__FLOAT:
	case TYPE__KIND__DOUBLE:
		return initFPReader(streamReader, stream, streamLength);
	case TYPE__KIND__BINARY:
		return initBinaryReader(streamReader, stream, streamLength);
	default:
		return -1;
	}
}

/**
 * Reads a boolean value from the stream.
 * 0,1 : false or true value
 *  -1 : error
 */
char readBoolean(StreamReader* booleanReaderState)
{
	char result = 0;

	if (booleanReaderState->mask == 0)
	{
		if (booleanReaderState->noOfLeftItems == 0)
		{
			/* try to re-initialize the reader */
			int initResult = initBooleanReader(booleanReaderState, booleanReaderState->streamPointer,
					booleanReaderState->streamLength);
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
				booleanReaderState->data = *(booleanReaderState->streamPointer++);
				booleanReaderState->streamLength--;
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
int readByte(StreamReader* intReaderState, uint8_t *result)
{
	*result = 0;

	if (intReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = initByteReader(intReaderState, intReaderState->streamPointer, intReaderState->streamLength);

		if (initResult)
		{
			/* error while reading from input stream */
			fprintf(stderr, "Error while re-initializing the byte reader");
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (intReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		*result = *intReaderState->streamPointer++;
		intReaderState->streamLength--;
	}
	else
	{
		*result = intReaderState->data;
	}

	intReaderState->noOfLeftItems--;
	return 0;
}

/**
 * Reads an integer from the stream.
 * Conversion from mixed sign to actual sign isn't done.
 */
int readInteger(Type__Kind kind, StreamReader* intReaderState, int64_t *result)
{
	*result = 0;
	long bytesRead = 0;
	char step = 0;
	uint64_t data = 0;

	if (intReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = initIntegerReader(kind, intReaderState, intReaderState->streamPointer,
				intReaderState->streamLength);
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
		bytesRead = readVarLenInteger(intReaderState->streamPointer, intReaderState->streamLength,
				&intReaderState->data);
		if (bytesRead <= 0)
		{
			/* there have to be bytes left on the stream */
			fprintf(stderr, "Byte stream ended prematurely!");
			return -1;
		}
		*result = intReaderState->data;
		intReaderState->streamPointer += bytesRead;
		intReaderState->streamLength -= bytesRead;
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

int readFloat(StreamReader* fpState, float *data)
{
	if (fpState->streamLength < 4)
	{
		/* not enough byte in the stream */
		return -1;
	}
	memcpy(data, fpState->streamPointer, 4);
	fpState->floatData = *data;
	fpState->streamPointer += 4;

	return 0;
}

int readDouble(StreamReader* fpState, double *data)
{
	if (fpState->streamLength < 8)
	{
		/* not enough byte in the stream */
		return -1;
	}
	memcpy(data, fpState->streamPointer, 8);
	fpState->doubleData = *data;
	fpState->streamPointer += 8;

	return 0;
}

/**
 * Reads a list of bytes from the input stream.
 */
int readBinary(StreamReader* binaryReaderState, uint8_t* data, int length)
{
	int bytePosition = 0;

	if (length > binaryReaderState->streamLength)
	{
		/* stream doesn't have enough bytes */
		fprintf(stderr, "Stream doesn't have enough bytes!\n");
		return -1;
	}

	for (bytePosition = 0; bytePosition < length; bytePosition++)
	{
		data[bytePosition] = *binaryReaderState->streamPointer++;
	}
	binaryReaderState->streamLength -= length;

	return 0;
}

int readPrimitiveType(Reader* reader, FieldValue* value, int* length)
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
	int64_t wordLength = 0;
	int64_t index = 0;
	int64_t seconds = 0;
	int64_t nanoSeconds = 0;
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
		result = 1;
		break;
	case TYPE__KIND__BYTE:
		byteStreamReader = &primitiveReader->readers[DATA];
		result = readByte(byteStreamReader, &value->value8);
		break;
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		integerStreamReader = &primitiveReader->readers[DATA];
		result = readInteger(reader->kind, integerStreamReader, &value->value64);
		value->value64 = toSignedInteger(value->value64);
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
		*length = primitiveReader->wordLength[index];
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

int readListElement(Reader* reader, FieldValue* value, int* length)
{
	ListReader* listReader = reader->fieldReader;
	StreamReader* presentStreamReader = &reader->presentBitReader;
	char isPresent = 0;

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

	return readPrimitiveType(&(listReader->itemReader), value, length);
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
