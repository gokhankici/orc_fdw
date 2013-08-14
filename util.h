/*
 * util.h
 *
 *  Created on: Aug 7, 2013
 *      Author: gokhan
 */

#ifndef UTIL_H_
#define UTIL_H_

#include "orc.pb-c.h"
#include "recordReader.h"

#define LogError(x) fprintf(stderr, x)
#define LogError2(x,y) fprintf(stderr, x,y)
#define LogError3(x,y,z) fprintf(stderr, x,y,z)

#define COMPRESSED_HEADER_SIZE 3

#define min(x,y) (((x) < (y)) ? (x) : (y))
#define max(x,y) (((x) < (y)) ? (y) : (x))

void PrintFieldValue(FILE* file, FieldValue* value, FieldType__Kind kind, int length);

int TimespecToStr(char* timespecBuffer, struct timespec *ts);

int InflateZLIB(uint8_t *input, int inputSize, uint8_t *output, int *outputSize);

#endif /* UTIL_H_ */
