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
#define toSignedInteger(x) (((x) % 2) ? (-(int64_t)((x - 1) / 2) - 1) : ((x) / 2))

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
	union
	{
		FieldValue value;
		FieldValue* list;
	};
	int* listItemSizes;
	char* isItemNull;
} Field;

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
	int columnNo;
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
	Reader** fields;
} StructReader;

extern struct tm BASE_TIMESTAMP;

void freeStructReader(StructReader* reader);
void freePrimitiveReader(PrimitiveReader* reader);

int readStruct(StructReader* reader, void* value);
int initStreamReader(Type__Kind streamKind, StreamReader* streamReader, uint8_t* stream, long streamLength);

/**
 * Reads one element from the type.
 * The returned value is <0 for error, 1 for null, 0 for not-null value
 */
int readField(Reader* reader, Field* field, int* length);

Type__Kind getStreamKind(Type__Kind type, int streamIndex);
int getStreamCount(Type__Kind type);

#endif /* RECORDREADER_H_ */
