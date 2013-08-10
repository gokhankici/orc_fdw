/*
 * util.h
 *
 *  Created on: Aug 7, 2013
 *      Author: gokhan
 */

#ifndef UTIL_H_
#define UTIL_H_

#include "orc_proto.pb-c.h"
#include "recordReader.h"

#define COMPRESSED_HEADER_SIZE 3

#define min(x,y) (((x) < (y)) ? (x) : (y))
#define max(x,y) (((x) < (y)) ? (y) : (x))

typedef struct
{
	CompressionKind compressionKind;
	long compressionBlockSize;
} CompressionParameters;

extern CompressionParameters compressionParameters;

typedef struct
{
	uint8_t *buffer;
	int offset;
	int length;
} ByteBuffer;

typedef struct
{
	uint8_t *array;
	int offset;
	int size;

	ByteBuffer* uncompressed;
	CompressionKind compressionKind;
	long compressionBlockSize;

	char isUncompressedOriginal;
} CompressedStream;

CompressedStream* initCompressedStream(CompressionParameters* parameters);
void freeCompressedStream(CompressedStream* stream,int retainUncompressedBuffer);

int uncompressStream(CompressedStream* stream);

void printFieldValue(FieldValue* value, Type__Kind kind, int length);

int timespecToStr(char* timespecBuffer, struct timespec *ts);

#endif /* UTIL_H_ */
