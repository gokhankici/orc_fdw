/*
 * util.h
 *
 *  Created on: Aug 7, 2013
 *      Author: gokhan
 */

#ifndef UTIL_H_
#define UTIL_H_

#include "postgres.h"
#include "storage/fd.h"

#include "orc.pb-c.h"
#include "recordReader.h"

//#define LogError(x) fprintf(stderr, x)
//#define LogError2(x,y) fprintf(stderr, x,y)
//#define LogError3(x,y,z) fprintf(stderr, x,y,z)

#define LogError(x) elog(ERROR, x)
#define LogError2(x,y) elog(ERROR, x,y)
#define LogError3(x,y,z) elog(ERROR, x,y,z)

#define alloc(memoryPointer) palloc(memoryPointer)
#define freeMemory(memoryPointer) pfree(memoryPointer)
#define reAllocateMemory(memoryPointer,newSize) repalloc(memoryPointer,newSize)

#define MyOpenFile(filePath, mode) AllocateFile(filePath, mode)
#define MyCloseFile(filePath) FreeFile(filePath)

#define COMPRESSED_HEADER_SIZE 3

#define min(x,y) (((x) < (y)) ? (x) : (y))
#define max(x,y) (((x) < (y)) ? (y) : (x))

void PrintFieldValue(FILE* file, FieldValue* value, FieldType__Kind kind, int length);
void PrintFieldValueAsWarning(FieldValue* value, FieldType__Kind kind, int length);

int TimespecToStr(char* timespecBuffer, struct timespec *ts);

int InflateZLIB(uint8_t *input, int inputSize, uint8_t *output, int *outputSize);

#endif /* UTIL_H_ */
