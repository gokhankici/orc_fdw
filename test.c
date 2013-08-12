/*
 * recordReader.c
 *
 *  Created on: Aug 2, 2013
 *      Author: gokhan
 */

#include <stdio.h>
#include <stdlib.h>
#include "recordReader.h"
#include "util.h"

//void testByteReader()
//{
//	StreamReader byteReaderState;
//	int streamSize = 131;
//	int noOfData = 129;
//	int i = 0;
//	uint8_t data = 0;
//	uint8_t stream[] =
//	{ 0x80, 0x01, 0x03, 0x05, 0x07, 0x09, 0x0B, 0x0D, 0x0F, 0x11, 0x13, 0x15, 0x17, 0x19, 0x1B, 0x1D, 0x1F,
//			0x21, 0x23, 0x25, 0x27, 0x29, 0x2B, 0x2D, 0x2F, 0x31, 0x33, 0x35, 0x37, 0x39, 0x3B, 0x3D, 0x3F,
//			0x41, 0x43, 0x45, 0x47, 0x49, 0x4B, 0x4D, 0x4F, 0x51, 0x53, 0x55, 0x57, 0x59, 0x5B, 0x5D, 0x5F,
//			0x61, 0x63, 0x65, 0x67, 0x69, 0x6B, 0x6D, 0x6F, 0x71, 0x73, 0x75, 0x77, 0x79, 0x7B, 0x7D, 0x7F,
//			0x01, 0x03, 0x05, 0x07, 0x09, 0x0B, 0x0D, 0x0F, 0x11, 0x13, 0x15, 0x17, 0x19, 0x1B, 0x1D, 0x1F,
//			0x21, 0x23, 0x25, 0x27, 0x29, 0x2B, 0x2D, 0x2F, 0x31, 0x33, 0x35, 0x37, 0x39, 0x3B, 0x3D, 0x3F,
//			0x41, 0x43, 0x45, 0x47, 0x49, 0x4B, 0x4D, 0x4F, 0x51, 0x53, 0x55, 0x57, 0x59, 0x5B, 0x5D, 0x5F,
//			0x61, 0x63, 0x65, 0x67, 0x69, 0x6B, 0x6D, 0x6F, 0x71, 0x73, 0x75, 0x77, 0x79, 0x7B, 0x7D, 0x7F,
//			0xFF, 0x01 };
//
//	initStreamReader(TYPE__KIND__BYTE, &byteReaderState, stream, streamSize);
//
//	for (i = 0; i < noOfData; ++i)
//	{
//		readByte(&byteReaderState, &data);
//		printf("%d\n", data);
//	}
//}
//
//void testIntegerReader()
//{
//	StreamReader intReaderState;
//	int streamSize = 15;
//	int noOfData = 9;
//	int i = 0;
//	int64_t data = 0;
//	uint8_t stream[] =
//	{ 0xFB, 6, 8, 0x80, 0x84, 0xaf, 0x5f, 0xbf, 0x9a, 0x0c, 0xa4, 0x13, 1, 2, 3 };
//
//	initStreamReader(TYPE__KIND__INT, &intReaderState, stream, streamSize);
//
//	for (i = 0; i < noOfData; ++i)
//	{
//		readInteger(TYPE__KIND__INT, &intReaderState, &data);
//		printf("%ld\n", data);
//	}
//}
//
//void testFloat()
//{
//	StreamReader floatReaderState;
//	int streamSize = 8;
//	int noOfData = 2;
//	int i = 0;
//	float data = 0;
//	uint8_t stream[] =
//	{ 0, 0, 0x80, 0x3f, 0, 0, 0, 0x40 };
//
//	initStreamReader(TYPE__KIND__FLOAT, &floatReaderState, stream, streamSize);
//
//	for (i = 0; i < noOfData; ++i)
//	{
//		readFloat(&floatReaderState, &data);
//		printf("%f\n", data);
//	}
//}
//
//void testDouble()
//{
//	StreamReader doubleReaderState;
//	int streamSize = 8;
//	int noOfData = 1;
//	int i = 0;
//	double data = 0;
//
//	uint8_t stream[] =
//	{ 0, 0, 0, 0, 0, 0, 0x2E, 0xC0 };
//
//	initStreamReader(TYPE__KIND__FLOAT, &doubleReaderState, stream, streamSize);
//
//	for (i = 0; i < noOfData; ++i)
//	{
//		readDouble(&doubleReaderState, &data);
//		printf("%lf\n", data);
//	}
//}

void testStringReader()
{
	Reader reader;
	PrimitiveReader stringReader;

	uint8_t present[2] =
	{ 0xFF, 0xB0 };
	uint8_t data[4] =
	{ 0xFD, 0, 1, 0 };
	uint8_t length[3] =
	{ 0xFE, 0x06, 0x06 };
	uint8_t dictionary[12] =
	{ 0x67, 0x6F, 0x6B, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x72, 0x67, 0x69, 0x73 };

	int result = 0;
	Field field;
	int noOfWords = 4;
	int iterator = 0;

	reader.fieldReader = &stringReader;
	reader.hasPresentBitReader = 1;
	reader.kind = TYPE__KIND__STRING;
	stringReader.dictionary = NULL;
	stringReader.dictionarySize = 2;

	StreamReader_init(TYPE__KIND__BOOLEAN, &reader.presentBitReader, present, (long) 2);
	StreamReader_init(TYPE__KIND__INT, &stringReader.readers[DATA], data, (long) 4);
	StreamReader_init(TYPE__KIND__INT, &stringReader.readers[LENGTH], length, (long) 3);
	StreamReader_init(TYPE__KIND__BINARY, &stringReader.readers[DICTIONARY_DATA], dictionary, (long) 12);

	for (iterator = 0; iterator < noOfWords; ++iterator)
	{
		result = readField(&reader, &field, NULL);
		switch (result)
		{
		case 0:
			printf("Word %d: %s\n", iterator, stringReader.dictionary[(int) field.value.value64]);
			break;
		case 1:
			printf("Word %d: %s\n", iterator, "(NULL)");
			break;
		case -1:
			printf("Error while reading element %d", iterator);
			break;
		}
	}

	/* free the resources */
	for (iterator = 0; iterator < stringReader.dictionarySize; ++iterator)
	{
		free(stringReader.dictionary[iterator]);
	}
	free(stringReader.dictionary);
	free(stringReader.wordLength);
}

int main(int argc, char **argv)
{
//	testByteReader();
//	testIntegerReader();
//	testFloat();
//	testDouble();
//	testString();

	testStringReader();

	return 0;
}
