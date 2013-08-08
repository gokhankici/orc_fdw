/*
 * recordReader.h
 *
 *  Created on: Aug 2, 2013
 *      Author: gokhan
 */

#ifndef RECORDREADER_H_
#define RECORDREADER_H_

#include <time.h>
#include "orc_proto.pb-c.h"

#define DATA 			0
#define LENGTH 			1
#define SECONDARY 		1
#define DICTIONARY_DATA	2

#define MAX_STREAM_COUNT		3
#define STRING_STREAM_COUNT		3
#define TIMESTAMP_STREAM_COUNT	2
#define BINARY_STREAM_COUNT		2
#define COMMON_STREAM_COUNT		1

#define MAX_POSTSCRIPT_SIZE 255
#define STREAM_BUFFER_SIZE 1024

#define toUnsignedInteger(x) (((x) < 0) ? -(2 * (x) + 1) : (2 * (x)))
#define toSignedInteger(x) (((x) % 2) ? (-(x + 1) / 2) : ((x) / 2))

typedef enum
{
	VARIABLE_LENGTH, RLE
} EncodingType;

typedef union
{
	uint8_t value8;
	int64_t value64;
	float floatValue;
	double doubleValue;
	char* binary;
	struct timespec time;
} FieldValue;

typedef struct
{
	/* pointer to the next byte to read */
	/* TODO: Is fixed size buffer better ? */
	uint8_t *streamPointer;
	uint8_t *stream;

//	uint8_t stream[STREAM_BUFFER_SIZE];

	/* no of bytes left in the stream */
	long streamLength;

	/* type of the encoding */
	EncodingType currentEncodingType;

	/* byte that is used currently */
	uint64_t data;

	/* no of bytes left in the current run */
	short noOfLeftItems;

	/* mask is for boolean, step is for int, float&double is for floating point readers */
	union
	{
		uint8_t mask;
		char step;
		float floatData;
		double doubleData;
	};

} StreamReader;
typedef struct
{
	Type__Kind kind;
	StreamReader presentBitReader;
	char hasPresentBitReader;

	/* Actual field reader, can be struct, list, or primitive */
	void* fieldReader;
} Reader;

/**
 * For primitive types
 * (boolean, byte, binary, short, integer, long, float, double, string)
 */
typedef struct
{
	StreamReader readers[MAX_STREAM_COUNT];

	/* for string type to store the dictionary */
	int dictionarySize;
	int* wordLength;
	char** dictionary;
} PrimitiveReader;

typedef struct
{
	StreamReader lengthReader;
	Reader itemReader;
} ListReader;

typedef struct
{
	int noOfFields;
	Reader* fields;
} StructReader;

extern struct tm BASE_TIMESTAMP;

void freePrimitiveReader(PrimitiveReader* reader);

int initStreamReader(Type__Kind streamKind, StreamReader* streamReader, uint8_t* stream, long streamLength);

char readBoolean(StreamReader* booleanReaderState);
int readByte(StreamReader* byteReaderState, uint8_t *result);
int readInteger(Type__Kind kind, StreamReader* intReaderState, int64_t* result);
int readFloat(StreamReader* fpState, float *data);
int readDouble(StreamReader* fpState, double *data);
int readBinary(StreamReader* intReaderState, uint8_t* data, int length);

/**
 * Reads one element from the type.
 * The returned value is <0 for error, 1 for null, 0 for not-null value
 */
int readPrimitiveType(Reader* reader, FieldValue* value, int* length);
int readListElement(Reader* reader, void* value, int* length);
int readStruct(StructReader* reader, void* value);

Type__Kind getStreamKind(Type__Kind type, int streamIndex);
int getStreamCount(Type__Kind type);

#endif /* RECORDREADER_H_ */
