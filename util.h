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

void printFieldValue(FILE* file, FieldValue* value, Type__Kind kind, int length);

int timespecToStr(char* timespecBuffer, struct timespec *ts);

int inflateZLIB(uint8_t *input, int inputSize, uint8_t *output, int *outputSize);

void* lower_bound(void* key, void* base, size_t noOfElements, size_t size, int (*comp)(const void*, const void*));
void* upper_bound(void* key, void* base, size_t noOfElements, size_t size, int (*comp)(const void*, const void*));
int compareInt(const void*,const void*);
int compareLong(const void*,const void*);

#endif /* UTIL_H_ */
