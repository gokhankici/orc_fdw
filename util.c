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

CompressedStream* initCompressedStream(CompressionParameters* parameters)
{
	CompressedStream* stream = malloc(sizeof(CompressedStream));
	stream->array = NULL;
	stream->offset = 0;
	stream->size = 0;
	stream->compressionBlockSize = parameters->compressionBlockSize;
	stream->compressionKind = parameters->compressionKind;
	stream->isUncompressedOriginal = 0;
	stream->uncompressed = NULL;

	return stream;
}

void freeCompressedStream(CompressedStream* stream, int retainUncompressedBuffer)
{
	if (stream->array && !(stream->isUncompressedOriginal && retainUncompressedBuffer))
	{
		/* free compressed array if uncompressed is new or we don't want to retain the uncompressed buffer */
		free(stream->array);
	}

	if (stream->uncompressed)
	{
		if (!stream->isUncompressedOriginal && !retainUncompressedBuffer)
		{
			free(stream->uncompressed->buffer);
		}
		free(stream->uncompressed);
	}
}

static int inf(uint8_t *input, int inputSize, uint8_t *output, int *outputSize)
{
	int ret = 0;
	z_stream strm;

	/* allocate inflate state */
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	strm.avail_in = 0;
	strm.next_in = Z_NULL;
	ret = inflateInit2(&strm,-15);
	if (ret != Z_OK)
		return ret;

	strm.avail_in = inputSize;
	if (strm.avail_in == 0)
		return Z_DATA_ERROR;
	strm.next_in = input;

	strm.avail_out = *outputSize;
	strm.next_out = output;
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

	*outputSize = *outputSize - strm.avail_out;

	/* clean up and return */
	(void) inflateEnd(&strm);
	return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

static int readCompressedStreamHeader(CompressedStream* stream)
{
	uint8_t *array = stream->array;
	int bufferSize = stream->compressionBlockSize;
	char isOriginal = 0;
	int result = 0;

	if (stream->size - stream->offset > COMPRESSED_HEADER_SIZE)
	{
		/* first 3 bytes are the chunk contains the chunk length, last bit is for "original" */
		int chunkLength = ((0xff & array[stream->offset + 2]) << 15) | ((0xff & array[stream->offset + 1]) << 7)
				| ((0xff & array[stream->offset]) >> 1);
		if (chunkLength > bufferSize)
		{
			fprintf(stderr, "Buffer size too small. size = %d needed = %d\n", bufferSize, chunkLength);
			return 1;
		}

		isOriginal = array[stream->offset] & 0x01;
		stream->offset += COMPRESSED_HEADER_SIZE;
		if (isOriginal)
		{
			if (!stream->isUncompressedOriginal && stream->uncompressed)
			{
				/* free the allocated buffer if compressed is not original before */
//				TODO Am I doing this right?
				free(stream->uncompressed->buffer);
			}

			stream->isUncompressedOriginal = 1;
			if (!stream->uncompressed)
			{
				stream->uncompressed = malloc(sizeof(ByteBuffer));
			}

			stream->uncompressed->buffer = array + stream->offset;
			stream->uncompressed->offset = 0;
			stream->uncompressed->length = chunkLength;
		}
		else
		{
			if (stream->isUncompressedOriginal || stream->uncompressed == NULL)
			{
				if (!stream->uncompressed)
				{
					stream->uncompressed = malloc(sizeof(ByteBuffer));
				}
				stream->uncompressed->buffer = malloc(bufferSize);
				stream->uncompressed->offset = 0;
				stream->uncompressed->length = bufferSize;
				stream->isUncompressedOriginal = 0;
			}
			else
			{
				stream->uncompressed->offset = 0;
			}

//			result = uncompress(stream->uncompressed->buffer, &stream->uncompressed->length, array + stream->offset,
//					chunkLength);
			result = inf(array + stream->offset, chunkLength, stream->uncompressed->buffer,
					&stream->uncompressed->length);
			if (result != Z_OK)
			{
				fprintf(stderr, "Error while decompressing with zlib inflator\n");
				return 1;
			}
		}
		stream->offset += chunkLength;
	}
	else
	{
		/* stream is shorter than the header size */
		fprintf(stderr, "Can't read header\n");
		return 1;
	}

	return 0;
}

int uncompressStream(CompressedStream* stream)
{
	int result = 0;
	switch (stream->compressionKind)
	{
	case COMPRESSION_KIND__NONE:
		if (!stream->uncompressed)
		{
			stream->uncompressed = malloc(sizeof(ByteBuffer));
		}
		stream->uncompressed->buffer = stream->array + stream->offset;
		stream->uncompressed->offset = 0;
		stream->uncompressed->length = stream->size - stream->offset;
		stream->isUncompressedOriginal = 1;
		result = 0;
		break;
	case COMPRESSION_KIND__ZLIB:
		result = readCompressedStreamHeader(stream);
		if (result != 0)
		{
			fprintf(stderr, "error while uncompressing footer with zlib\n");
			return 1;
		}
		result = 0;
		break;
	default:
		fprintf(stderr, "unsupported compression kind\n");
		result = 1;
	}
	return result;
}

void printFieldValue(FieldValue* value, Type__Kind kind, int length)
{
	char* timespecBuffer = NULL;
	uint8_t *binaryValues = NULL;
	int iterator = 0;

	switch (kind)
	{
	case TYPE__KIND__BOOLEAN:
		printf("%d", (int) value->value8);
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
