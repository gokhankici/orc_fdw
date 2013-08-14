/*
 * recordReader.h
 *
 *  Created on: Aug 2, 2013
 *      Author: gokhan
 */

#ifndef RECORDREADER_H_
#define RECORDREADER_H_

#include "postgres.h"
#include <time.h>
#include "orc.pb-c.h"
#include "inputStream.h"

#define DATA_STREAM 			0
#define LENGTH_STREAM 			1
#define SECONDARY_STREAM 		1
#define DICTIONARY_DATA_STREAM	2

#define MAX_STREAM_COUNT		3
#define STRING_STREAM_COUNT		3
#define TIMESTAMP_STREAM_COUNT	2
#define BINARY_STREAM_COUNT		2
#define COMMON_STREAM_COUNT		1

#define MAX_POSTSCRIPT_SIZE 255
#define STREAM_BUFFER_SIZE 1024

#define ToUnsignedInteger(x) (((x) < 0) ? ((uint64_t)(-(x+1)) * 2 + 1) : (2 * (uint64_t)(x)))
#define ToSignedInteger(x) (((x) % 2) ? (-(int64_t)((x - 1) / 2) - 1) : ((x) / 2))

#define IsComplexType(type) (type == FIELD_TYPE__KIND__LIST || type == FIELD_TYPE__KIND__STRUCT || type == FIELD_TYPE__KIND__MAP)

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
	/* stream to read from the file */
	FileStream* stream;

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
	FieldType__Kind kind;
	int psqlKind;
	int orcColumnNo;
	StreamReader presentBitReader;
	/* Length reader for list & map complex types */
	StreamReader lengthReader;
	char hasPresentBitReader;
	char required;

	/* Actual field reader, can be struct, list, or primitive */
	void* fieldReader;
} FieldReader;

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
} PrimitiveFieldReader;

typedef struct
{
	FieldReader itemReader;
} ListFieldReader;

typedef struct
{
	int noOfFields;
	FieldReader** fields;
} StructFieldReader;

extern struct tm BASE_TIMESTAMP;

int FieldReaderFree(FieldReader* reader);

int StreamReaderFree(StreamReader* streamReader);
int StreamReaderInit(StreamReader* streamReader, FieldType__Kind streamKind, char* fileName, long offset, long limit,
		CompressionParameters* parameters);

/**
 * Reads one element from the type.
 * The returned value is <0 for error, 1 for null, 0 for not-null value
 */
int FieldReaderRead(FieldReader* fieldReader, Field* field, int* length);

/**
 * Helper functions to get the kth stream and its type
 */
FieldType__Kind GetStreamKind(FieldType__Kind type, int streamIndex);
int GetStreamCount(FieldType__Kind type);

#endif /* RECORDREADER_H_ */
