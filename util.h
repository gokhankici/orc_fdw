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

void printFieldValue(FieldValue* value, Type__Kind kind, int length);

int timespecToStr(char* timespecBuffer, struct timespec *ts);

int inf(uint8_t *input, unsigned int inputSize, uint8_t **output, unsigned int *outputSize);

#endif /* UTIL_H_ */
