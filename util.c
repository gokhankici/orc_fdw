#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include "util.h"

#define TIMESPEC_BUFFER_LENGTH 30

int timespecToStr(char* timespecBuffer, struct timespec *ts)
{
	int ret = 0;
	int len = TIMESPEC_BUFFER_LENGTH;
	struct tm t;

	tzset();
	if (localtime_r(&(ts->tv_sec), &t) == NULL)
		return 1;

	ret = strftime(timespecBuffer, len, "%F %T", &t);
	if (ret == 0)
		return 2;
	len -= ret - 1;

	ret = snprintf(&timespecBuffer[strlen(timespecBuffer)], len, ".%09ld", ts->tv_nsec);
	if (ret >= len)
		return 3;

	return 0;
}

int inf(uint8_t *input, unsigned int inputSize, uint8_t **output, unsigned int *outputSize)
{
	int ret = 0;
	z_stream strm;

	/* allocate inflate state */
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	strm.avail_in = 0;
	strm.next_in = Z_NULL;
	ret = inflateInit(&strm);
	if (ret != Z_OK)
		return ret;

	*output = malloc(compressionBlockSize);

	strm.avail_in = inputSize;
	if (strm.avail_in == 0)
		return Z_DATA_ERROR;
	strm.next_in = input;

	strm.avail_out = compressionBlockSize;
	strm.next_out = *output;
	ret = inflate(&strm, Z_NO_FLUSH);
	assert(ret != Z_STREAM_ERROR); /* state not clobbered */
	switch (ret)
	{
	case Z_NEED_DICT:
		ret = Z_DATA_ERROR; /* and fall through */
	case Z_DATA_ERROR:
	case Z_MEM_ERROR:
		(void) inflateEnd(&strm);
		return ret;
	}
	if (strm.avail_out == 0)
	{
		/* we should have used more space*/
		return -10;
	}
	*outputSize = compressionBlockSize - strm.avail_out;

	/* clean up and return */
	(void) inflateEnd(&strm);
	return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

void printFieldValue(FieldValue* value, Type__Kind kind, int length)
{
	char* timespecBuffer = NULL;
	uint8_t *binaryValues = NULL;
	int iterator = 0;

	switch (kind)
	{
	case TYPE__KIND__BOOLEAN:
		printf("%d", (int)value->value8);
		break;
	case TYPE__KIND__BYTE:
		printf("%.2X", value->value8);
		break;
	case TYPE__KIND__SHORT:
	case TYPE__KIND__INT:
	case TYPE__KIND__LONG:
		printf("%ld", value->value64);
		break;
	case TYPE__KIND__FLOAT:
		printf("%.2f", value->floatValue);
		break;
	case TYPE__KIND__DOUBLE:
		printf("%.2lf", value->doubleValue);
		break;
	case TYPE__KIND__STRING:
		printf("%s", value->binary);
		break;
	case TYPE__KIND__TIMESTAMP:
		timespecBuffer = malloc(TIMESPEC_BUFFER_LENGTH);
		timespecToStr(timespecBuffer, &value->time);
		printf("%s", timespecBuffer);
		break;
	case TYPE__KIND__BINARY:
		binaryValues = (uint8_t*) value->binary;
		for (iterator = 0; iterator < length; ++iterator)
		{
			printf("%.2X", binaryValues[iterator]);
		}
		break;
	default:
		break;
	}
}
