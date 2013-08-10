/*
 * InputStream.c
 *
 *  Created on: Aug 10, 2013
 *      Author: gokhan
 */
#include <stdlib.h>
#include <string.h>
#include "InputStream.h"

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
 * Returns no of bytes read from the file when the buffer is filled.
 */
int FileStream_fill(FileStream* fileStream)
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

long FileStream_bytesLeft(FileStream* fileStream)
{
	return fileStream->limit - (fileStream->offset + fileStream->position);
}
