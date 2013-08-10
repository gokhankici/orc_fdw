/*
 * InputStream.h
 *
 *  Created on: Aug 10, 2013
 *      Author: gokhan
 */

#ifndef INPUTSTREAM_H_
#define INPUTSTREAM_H_

#include <stdio.h>
#include "util.h"

typedef struct
{
	/* Input stream for reading from the file */
	FILE* file;

	/* offset of the start of the buffer in the file */
	long offset;
	/* end of the file stream in the file */
	long limit;

	/* current position in the buffer */
	int position;
	/* current length of the buffer */
	int length;

	/* allocated buffer size */
	int bufferSize;
	/* buffer to store the file data */
	char* buffer;
} FileStream;

FileStream* FileStream_init(char* filePath, long offset, long limit, int bufferSize);
int FileStream_free(FileStream*);
int FileStream_fill(FileStream*);
char* FileStream_read(FileStream*, int *length);
int FileStream_skip(FileStream*, int skip);
long FileStream_bytesLeft(FileStream*);

typedef struct
{
	/* Stream for decompression */
	FileStream* fileStream;

	/* current length in the uncompressed buffer */
	int offset;
	/* current length of the uncompressed buffer */
	int length;
	/* buffer to store uncompressed data */
	char* uncompressedBuffer;
} CompressedFileStream;

CompressedFileStream* CompressedFileStream_init(char* filePath, long offset, long limit, int bufferSize);
int CompressedFileStream_free(CompressedFileStream*);
char* CompressedFileStream_read(CompressedFileStream*, int *length);
int CompressedFileStream_skip(CompressedFileStream*, int skip);

#endif /* INPUTSTREAM_H_ */
