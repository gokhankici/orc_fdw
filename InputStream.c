/*
 * InputStream.c
 *
 *  Created on: Aug 10, 2013
 *      Author: gokhan
 */
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include "InputStream.h"
#include "util.h"
#include "snappy-c/snappy.h"

/**
 * Initialize a FileStream, from starting at an offset and with a limit.
 * Buffer size gives the maximum size that can be read at a time.
 */
FileStream* FileStream_init(char* filePath, long offset, long limit, int bufferSize)
{
	FileStream* fileStream = malloc(sizeof(FileStream));
	int result = 0;
	if (filePath == NULL)
	{
		free(fileStream);
		return NULL;
	}
	fileStream->file = fopen(filePath, "r");
	if (fileStream->file == NULL)
	{
		free(fileStream);
		return NULL;
	}

	fileStream->offset = offset;
	result = fseek(fileStream->file, offset, SEEK_SET);
	if (result)
	{
		fclose(fileStream->file);
		free(fileStream);
		return NULL;
	}

	fileStream->limit = limit;
	fileStream->bufferSize = bufferSize;
	fileStream->buffer = malloc(bufferSize);
	if (fileStream->buffer == NULL)
	{
		fclose(fileStream->file);
		free(fileStream);
		return NULL;
	}
	fileStream->position = 0;
	fileStream->length = 0;

	return fileStream;
}

int FileStream_free(FileStream* fileStream)
{
	int result = 0;

	if (fileStream == NULL)
	{
		return 1;
	}
	result = fclose(fileStream->file);
	if (result)
	{
		return 1;
	}
	if (fileStream->buffer)
	{
		free(fileStream->buffer);
	}
	free(fileStream);

	return 0;
}

/**
 * Static function to fill the buffer. Returns no of bytes read from the file when the buffer is filled.
 */
static int FileStream_fill(FileStream* fileStream)
{
	int bytesRead = 0;
	int result = 0;

	if (fileStream == NULL)
	{
		return -1;
	}

	if (fileStream->offset + fileStream->length == fileStream->limit)
	{
		/* already reached end of file stream */
		return 0;
	}

	if (fileStream->position > 0)
	{

		memmove(fileStream->buffer, fileStream->buffer + fileStream->position,
				fileStream->length - fileStream->position);
		fileStream->offset += fileStream->position;
		fileStream->length -= fileStream->position;
		fileStream->position = 0;
	}

	bytesRead =
	min(fileStream->limit - (fileStream->offset + fileStream->length), fileStream->bufferSize - fileStream->length);

	if (bytesRead < 0)
	{
		/* this shouldn't happen */
		return -1;
	}
	else if (bytesRead == 0)
	{
		/* buffer is already filled */
		return 0;
	}

	result = fread(fileStream->buffer + fileStream->length, 1, bytesRead, fileStream->file);
	if (result != bytesRead)
	{
		fprintf(stderr, "Error while reading file\n");
		return -1;
	}

	fileStream->length += bytesRead;

	return bytesRead;
}

/**
 * Reads bytes from the file of specified length and returns the start of the data;
 * WARNING! This function returns the pointer from the internal buffer,
 * so copy this to another memory location when necessary!
 */
char* FileStream_read(FileStream* fileStream, int *length)
{
	char* data = NULL;
	int result = 0;
	if (fileStream == NULL)
	{
		return NULL;
	}

	if (*length > fileStream->length - fileStream->position)
	{
		result = FileStream_fill(fileStream);
		if (result < 0)
		{
			return NULL;
		}

		if (*length > fileStream->length - fileStream->position)
		{
			/* cannot read that many bytes from the file */
			*length = fileStream->length - fileStream->position;
		}
	}

	data = fileStream->buffer + fileStream->position;
	fileStream->position += *length;

	return data;
}

int FileStream_readRemaining(FileStream* fileStream, char** data, int* dataLength)
{
	char* newBuffer = NULL;
	int remainingLength = fileStream->limit - (fileStream->offset + fileStream->position);

	if (remainingLength <= fileStream->bufferSize)
	{
		FileStream_fill(fileStream);
		*data = fileStream->buffer + fileStream->position;
		*dataLength = fileStream->length - fileStream->position;
		return 0;
	}
	else
	{
		newBuffer = malloc(remainingLength);
		fileStream->bufferSize = remainingLength;
		memcpy(newBuffer, fileStream->buffer + fileStream->position, fileStream->length - fileStream->position);
		fileStream->offset += fileStream->position;
		fileStream->length -= fileStream->position;
		fileStream->position = 0;
		free(fileStream->buffer);

		if (FileStream_fill(fileStream))
		{
			return 1;
		}
		*data = fileStream->buffer + fileStream->position;
		*dataLength = fileStream->length - fileStream->position;
		return 0;
	}
}

/**
 * Skip that many bytes from the stream
 */
int FileStream_skip(FileStream* fileStream, int skip)
{
	int skippedBytes = 0;

	if (skip > fileStream->length - fileStream->position)
	{
		skippedBytes = fileStream->length - fileStream->position;
		fileStream->position = fileStream->length;
	}
	else
	{
		skippedBytes = skip;
		fileStream->position += skip;
	}

	return skippedBytes;
}

/**
 * Returns the # bytes left in the file stream
 */
long FileStream_bytesLeft(FileStream* fileStream)
{
	return fileStream->limit - (fileStream->offset + fileStream->position);
}

/**
 * Initialize a file stream with compression
 */
CompressedFileStream* CompressedFileStream_init(char* filePath, long offset, long limit, int bufferSize,
		CompressionKind kind)
{
	CompressedFileStream *stream = malloc(sizeof(CompressedFileStream));
	if (kind == COMPRESSION_KIND__NONE)
	{
		stream->bufferSize = DEFAULT_BUFFER_SIZE;
	}
	else
	{
		stream->bufferSize = bufferSize;
	}

	stream->fileStream = FileStream_init(filePath, offset, limit, stream->bufferSize);
	if (stream->fileStream == NULL)
	{
		free(stream);
		return NULL;
	}

	stream->bufferSize = bufferSize;
	stream->compressionKind = kind;

	stream->position = 0;
	stream->length = 0;
	stream->uncompressedBuffer = NULL;
	stream->isOriginal = 0;
	stream->tempBuffer = NULL;

	return stream;
}

int CompressedFileStream_free(CompressedFileStream* stream)
{
	if (stream == NULL)
	{
		return 1;
	}

	if (!stream->isOriginal && stream->uncompressedBuffer != NULL)
	{
		free(stream->uncompressedBuffer);
	}

	if (stream->tempBuffer)
	{
		free(stream->tempBuffer);
	}

	if (FileStream_free(stream->fileStream))
	{
		return 1;
	}

	free(stream);

	return 0;
}

static int readCompressedStreamHeader(CompressedFileStream* stream)
{
	char *header = NULL;
	int headerLength = COMPRESSED_HEADER_SIZE;
	char *compressed = NULL;
	int bufferSize = stream->bufferSize;
	char isOriginal = 0;
	int chunkLength = 0;
	int result = 0;
	size_t snappyUncompressedSize = 0;

	header = FileStream_read(stream->fileStream, &headerLength);
	if (header == NULL || headerLength != COMPRESSED_HEADER_SIZE)
	{
		/* couldn't read compressed header */
		return 1;
	}

	/* first 3 bytes are the chunk contains the chunk length, last bit is for "original" */
	chunkLength = ((0xff & header[2]) << 15) | ((0xff & header[1]) << 7) | ((0xff & header[0]) >> 1);
	if (chunkLength > bufferSize)
	{
		fprintf(stderr, "Buffer size too small. size = %d needed = %d\n", bufferSize, chunkLength);
		return 1;
	}

	isOriginal = header[0] & 0x01;
	if (isOriginal)
	{
		if (!stream->isOriginal && stream->uncompressedBuffer)
		{
			/* free the allocated buffer if compressed is not original before */
			free(stream->uncompressedBuffer);
		}

		stream->isOriginal = 1;

		/* result is used for temporary storage */
		result = chunkLength;
		stream->uncompressedBuffer = FileStream_read(stream->fileStream, &chunkLength);
		if (result != chunkLength)
		{
			fprintf(stderr, "chunk of given length couldn't read from the file\n");
			return 1;
		}
		stream->position = 0;
		stream->length = chunkLength;
	}
	else
	{
		stream->isOriginal = 0;
		if (stream->uncompressedBuffer == NULL)
		{
			stream->uncompressedBuffer = malloc(bufferSize);
		}
		stream->position = 0;
		stream->length = bufferSize;

		result = chunkLength;
		compressed = FileStream_read(stream->fileStream, &chunkLength);
		if (compressed == NULL || result != chunkLength)
		{
			fprintf(stderr, "chunk of given length couldn't read from the file\n");
			return 1;
		}

		switch (stream->compressionKind)
		{
		case COMPRESSION_KIND__ZLIB:
			result = inf((uint8_t*) compressed, chunkLength, (uint8_t*) stream->uncompressedBuffer, &stream->length);
			if (result != Z_OK)
			{
				fprintf(stderr, "Error while decompressing with zlib inflator\n");
				return 1;
			}
			break;
		case COMPRESSION_KIND__SNAPPY:
			result = snappy_uncompress((const char*) compressed, (size_t) chunkLength,
					(char*) stream->uncompressedBuffer);
			if (result)
			{
				fprintf(stderr, "Error while uncompressing with snappy. Error code %d\n", result);
				return 1;
			}
			result = snappy_uncompressed_length((const char*) compressed, (size_t) chunkLength,
					&snappyUncompressedSize);

			if (result != 1)
			{
				fprintf(stderr, "Error while calculating uncompressed size of snappy block.\n");
				return 1;
			}

			stream->length = (int) snappyUncompressedSize;
			if (stream->length > stream->bufferSize)
			{
				fprintf(stderr, "Uncompressed stream size (%d) exceeds buffer size (%d\n", stream->length,
						stream->bufferSize);
				return 1;
			}
			break;
		default:
			/* compression kind not supported */
			return 1;
		}
	}
	return 0;
}

char* CompressedFileStream_read(CompressedFileStream* stream, int *length)
{
	int requestedLength = *length;
	int result = 0;
	char* data = NULL;
	char* newBuffer = NULL;
	int bytesCurrentlyRead = 0;

	if (stream->compressionKind == COMPRESSION_KIND__NONE)
	{
		/* if there is no compression, read directly from FileStream */
		return FileStream_read(stream->fileStream, length);
	}

	if (stream->length == 0 || stream->position == stream->length)
	{
		result = readCompressedStreamHeader(stream);
		if (result)
		{
			fprintf(stderr, "Error reading compressed stream header\n");
			return NULL;
		}
	}

	if (stream->position + requestedLength <= stream->length)
	{
		data = stream->uncompressedBuffer + stream->position;
		stream->position += requestedLength;
	}
	else
	{
		if (newBuffer)
		{
			free(newBuffer);
		}

		/* stash available data */
		newBuffer = malloc(requestedLength);
		bytesCurrentlyRead = stream->length - stream->position;
		memcpy(newBuffer, stream->uncompressedBuffer + stream->position, bytesCurrentlyRead);

		/* set the stream in order to read next compressed block */
		stream->position = stream->length;

		result = readCompressedStreamHeader(stream);
		if (result)
		{
			fprintf(stderr, "Error while initializing the next block\n");
			return NULL;
		}

		if (stream->length < requestedLength - bytesCurrentlyRead)
		{
			fprintf(stderr, "Couldn't get enough bytes from the next block\n");
			return NULL;
		}

		memcpy(newBuffer + bytesCurrentlyRead, stream->uncompressedBuffer, requestedLength - bytesCurrentlyRead);
		stream->position += requestedLength - bytesCurrentlyRead;

		data = stream->tempBuffer;
	}

	return data;
}

int CompressedFileStream_readByte(CompressedFileStream* stream, char* value)
{
	int readLength = 1;
	char* readBytes = CompressedFileStream_read(stream, &readLength);
	if (readBytes == NULL || readLength != 1)
	{
		return -1;
	}

	*value = *readBytes;
	return 0;
}

int CompressedFileStream_readRemaining(CompressedFileStream* stream, char** data, int* dataLength)
{
	int result = 0;
	char* newBuffer = NULL;
	char* tempBuffer = NULL;
	int newBufferPosition = 0;
	int newBufferSize = 0;

	if (stream->compressionKind == COMPRESSION_KIND__NONE)
	{
		/* if there is no compression, read directly from FileStream */
		return FileStream_readRemaining(stream->fileStream, data, dataLength);
	}

	if (stream->length == 0 || stream->position == stream->length)
	{
		/* if not any bytes is read from the stream before, init it first */
		result = readCompressedStreamHeader(stream);
		if (result)
		{
			fprintf(stderr, "Error reading compressed stream header\n");
			return 1;
		}
	}

	if (FileStream_bytesLeft(stream->fileStream))
	{
		/* allocate space for the uncompressed data*/
		newBufferSize = stream->bufferSize * 2;
		newBuffer = malloc(newBufferSize);
		newBufferPosition = stream->length - stream->position;
		memcpy(newBuffer, stream->uncompressedBuffer + stream->position, newBufferPosition);

		/* while file has still data, decompress the block and add it to new buffer */
		while (FileStream_bytesLeft(stream->fileStream))
		{
			result = readCompressedStreamHeader(stream);
			if (result)
			{
				fprintf(stderr, "Error reading compressed stream header\n");
				return 1;
			}

			if (newBufferSize - newBufferPosition < stream->length - stream->position)
			{
				/* if there is not space in the new buffer, copy buffer to another one with increased size */
				newBufferSize += stream->bufferSize;
				tempBuffer = malloc(newBufferSize);
				memcpy(tempBuffer, newBuffer, newBufferPosition);
				newBufferPosition += stream->length - stream->position;

				/* free the previous buffer and cleanup */
				free(newBuffer);
				newBuffer = tempBuffer;
				tempBuffer = NULL;
			}
			else
			{
				/* there is enough space so copy directly */
				memcpy(newBuffer, stream->uncompressedBuffer + stream->position, stream->length - stream->position);
				newBufferPosition += stream->length - stream->position;
			}
		}

		*data = newBuffer;
		*dataLength = newBufferPosition;

		/* also put the new buffer to stream in order to free it later */
		stream->tempBuffer = newBuffer;
	}
	else
	{
		*data = stream->uncompressedBuffer + stream->position;
		*dataLength = stream->length - stream->position;
	}

	return 0;
}
