/*
 * testInputStream.c
 *
 *  Created on: Aug 10, 2013
 *      Author: gokhan
 */

#include "InputStream.h"
#include <string.h>

#define BUFFER_SIZE 262144
#define TEST_FILE_SIZE 171535926

int compareBytes(char* array1, char* array2, int length)
{
	int i = 0;
	for (i = 0; i < length; ++i)
	{
		if (array1[i] != array2[i])
		{
			return 1;
		}
	}

	return 0;
}

int testFileStream(char* fileName, long fileSize, int bufferSize)
{
	FileStream* fileStream = FileStream_init(fileName, 0, fileSize, bufferSize);
	FILE* fileToTest = fopen(fileName, "r");
	int bytesToRead = BUFFER_SIZE;
	char* result = NULL;
	char testBuffer[bufferSize];
	long totalBytes = 0;

	if (fileStream == NULL)
	{
		return 1;
	}
	while (FileStream_bytesLeft(fileStream) > 0)
	{
		result = FileStream_read(fileStream, &bytesToRead);
		if (result == NULL)
		{
			return 1;
		}
		totalBytes += bytesToRead;
		fread(testBuffer, 1, bytesToRead, fileToTest);
		bytesToRead = BUFFER_SIZE;

		if (compareBytes(result, testBuffer, bytesToRead))
		{
			/* bytes differ */
			return 1;
		}
	}

	if (ftell(fileToTest) != fileSize)
	{
		/* there is still bytes left at the original file */
		return 1;
	}

	return 0;
}

int main(int argc, char **argv)
{
	int result = 0;

	result = testFileStream("/home/gokhan/orc-files/output_gzip_lcomment.orc", TEST_FILE_SIZE, BUFFER_SIZE);

	if (result)
	{
		printf("Error at testFileStream\n");
		return 1;
	}
	else
	{
		printf("testFileStream: OK\n");
	}

	return 0;
}
