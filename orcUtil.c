#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#include "orcUtil.h"
#include "snappy.h"


/*
 * Inflates ZLIB compressed buffer.
 *
 * @param input data to decompress
 * @param inputSize length of input in bytes
 * @param output buffer to write the output of the decompression
 * @param outputSize length of the input when it is decompressed
 */
int
InflateZLIB(uint8_t *input, int inputSize, uint8_t *output, int *outputSize)
{
	int returnCode = 0;
	z_stream stream;

	/* allocate inflate state */
	stream.zalloc = Z_NULL;
	stream.zfree = Z_NULL;
	stream.opaque = Z_NULL;
	stream.avail_in = 0;
	stream.next_in = Z_NULL;
	returnCode = inflateInit2(&stream,-15);
	if (returnCode != Z_OK)
		return returnCode;

	stream.avail_in = inputSize;
	if (stream.avail_in == 0)
		return Z_DATA_ERROR;
	stream.next_in = input;

	stream.avail_out = *outputSize;
	stream.next_out = output;
	returnCode = inflate(&stream, Z_NO_FLUSH);
	assert(returnCode != Z_STREAM_ERROR); /* state not clobbered */

	switch (returnCode)
	{
		case Z_NEED_DICT:
		{
			returnCode = Z_DATA_ERROR; /* and fall through */
		}
		case Z_DATA_ERROR:
		case Z_MEM_ERROR:
		{
			(void) inflateEnd(&stream);
			return returnCode;
		}
	}

	*outputSize = *outputSize - stream.avail_out;

	/* clean up and return */
	(void) inflateEnd(&stream);

	return returnCode == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}


char *
GetTypeKindName(FieldType__Kind kind)
{
	switch (kind)
	{
	case FIELD_TYPE__KIND__BOOLEAN:
		return "BOOLEAN";
	case FIELD_TYPE__KIND__BYTE:
		return "BYTE";
	case FIELD_TYPE__KIND__SHORT:
		return "SHORT";
	case FIELD_TYPE__KIND__INT:
		return "INT";
	case FIELD_TYPE__KIND__LONG:
		return "LONG";
	case FIELD_TYPE__KIND__FLOAT:
		return "FLOAT";
	case FIELD_TYPE__KIND__DOUBLE:
		return "DOUBLE";
	case FIELD_TYPE__KIND__STRING:
		return "STRING";
	case FIELD_TYPE__KIND__BINARY:
		return "BINARY";
	case FIELD_TYPE__KIND__TIMESTAMP:
		return "TIMESTAMP";
	case FIELD_TYPE__KIND__DATE:
		return "DATE";
	case FIELD_TYPE__KIND__LIST:
		return "LIST";
	case FIELD_TYPE__KIND__MAP:
		return "MAP";
	case FIELD_TYPE__KIND__STRUCT:
		return "STRUCT";
	case FIELD_TYPE__KIND__UNION:
		return "UNION";
	case FIELD_TYPE__KIND__DECIMAL:
		return "DECIMAL";
	default:
		return "";
	}
}


/*
 * Creates a stack with the given parameters.
 *
 * @param list array of values
 * @param elementSize size of each element in the list
 * @param length # elements in the list
 *
 * @return
 */
OrcStack *
OrcStackInit(void* list, int elementSize, int length)
{
	OrcStack* stack = alloc(sizeof(OrcStack));
	stack->list = list;
	stack->elementSize = elementSize;
	stack->length = length;
	stack->position = 0;

	return stack;
}


/*
 * Free the memory that stack uses
 */
void OrcStackFree(OrcStack* stack)
{
	freeMemory(stack);
}


/*
 * Pop the first element in the stack and return it
 */
void* OrcStackPop(OrcStack* stack)
{
	if (stack->position < stack->length)
	{
		return ((char*) stack->list) + (stack->position++) * stack->elementSize;
	}
	else
	{
		return NULL;
	}
}
