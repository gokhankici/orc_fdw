/*
 * orcUtil.h
 *
 *  Created on: Aug 7, 2013
 *      Author: gokhan
 */

#ifndef ORC_UTIL_H_
#define ORC_UTIL_H_

#include "postgres.h"
#include "storage/fd.h"

#include "orc.pb-c.h"

typedef struct
{
	void* list;

	int elementSize;
	int length;
	int position;
} OrcStack;

//#define LogError(x) fprintf(stderr, x)
//#define LogError2(x,y) fprintf(stderr, x,y)
//#define LogError3(x,y,z) fprintf(stderr, x,y,z)

#define LogError(message) elog(ERROR, message)
#define LogError2(message,arg1) elog(ERROR, message,arg1)
#define LogError3(message,arg1,arg2) elog(ERROR,message,arg1,arg2)

#define alloc(memoryPointer) palloc(memoryPointer)
#define freeMemory(memoryPointer) pfree(memoryPointer)
#define reAllocateMemory(memoryPointer,newSize) repalloc(memoryPointer,newSize)

#define MyOpenFile(filePath, mode) AllocateFile(filePath, mode)
#define MyCloseFile(filePath) FreeFile(filePath)

#define COMPRESSED_HEADER_SIZE 3

#define min(x,y) (((x) < (y)) ? (x) : (y))
#define max(x,y) (((x) < (y)) ? (y) : (x))

int TimespecToStr(char* timespecBuffer, struct timespec *ts);

int InflateZLIB(uint8_t *input, int inputSize, uint8_t *output, int *outputSize);

char* getTypeKindName(FieldType__Kind kind);

OrcStack* OrcStackInit(void* list, int elementSize, int length);
void* OrcStackPop(OrcStack* stack);
void OrcStackFree(OrcStack* stack);

#endif /* ORC_UTIL_H_ */
