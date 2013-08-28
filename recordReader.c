#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"

#include "orc.pb-c.h"
#include "recordReader.h"
#include "fileReader.h"
#include "orcUtil.h"

/* forward declarations of static functions */
static int ParseNanos(long serializedData);
static int ReadVarLenInteger(FileStream *stream, uint64_t *data);
static int BooleanReaderInit(StreamReader *boolState);
static int ByteReaderInit(StreamReader *byteState);
static int IntegerReaderInit(FieldType__Kind kind, StreamReader *intState);

static char ReadBoolean(StreamReader *booleanReaderState);
static int ReadByte(StreamReader *byteReaderState, uint8_t *result);
static int ReadInteger(FieldType__Kind kind, StreamReader *intReaderState, uint64_t *result);
static int ReadFloat(StreamReader *fpState, float *data);
static int ReadDouble(StreamReader *fpState, double *data);
static int ReadBinary(StreamReader *binaryReaderState, uint8_t *data, int length);


/*
 * Decode the nano seconds stored in the file
 *
 * @param serializedData data stored in the file as a long
 *
 * @return nanoseconds in the timestamp after the seconds
 */
static int 
ParseNanos(long serializedData)
{
	/* last 3 bits encode the # 0s in the number */
	int zeros = 7 & (int) serializedData;
	int result = (int) serializedData >> 3;
	int zeroIterator = 0;

	if (zeros != 0)
	{
		/* if last 3 bits (xyz) are not 0, this means we have to multiply the value with ((xyz) + 1) * 10 */
		for (zeroIterator = 0; zeroIterator <= zeros; ++zeroIterator)
		{
			result *= 10;
		}
	}

	return result;
}


/**
 * Read variable-length integer from the stream
 *
 * @param stream stream to read
 * @param data place to read into the integer
 *
 * @return length of the integer after variable-length encoding
 */
static int 
ReadVarLenInteger(FileStream *stream, uint64_t *data)
{
	int shift = 0;
	int bytesRead = 0;
	char byte = 0;
	int result = 0;
	*data = 0;

	result = FileStreamReadByte(stream, &byte);

	if (result)
	{
		return -1;
	}

	while ((byte & 0x80) != 0)
	{
		*data |= (uint64_t) (byte & 0x7F) << shift;
		shift += 7;
		bytesRead++;

		result = FileStreamReadByte(stream, &byte);

		if (result)
		{
			return -1;
		}
	}

	*data |= (uint64_t) byte << shift;

	return ++bytesRead;
}


/**
 * Initialize a boolean reader for reading.
 *
 * @param boolState a boolean stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int
BooleanReaderInit(StreamReader *boolState)
{
	char type = 0;

	if (FileStreamReadByte(boolState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		boolState->currentEncodingType = VARIABLE_LENGTH;
		boolState->noOfLeftItems = -type - 1;
	}
	else
	{
		/* run-length contains type + 3 elements */
		boolState->currentEncodingType = RLE;
		boolState->noOfLeftItems = type + 3 - 1;
	}

	if (FileStreamReadByte(boolState->stream, &type))
	{
		return -1;
	}

	boolState->data = (uint8_t) type;
	boolState->mask = 0x80;

	return 0;
}


/**
 * Initialize a byte reader for reading.
 *
 * @param byteState a boolean stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int 
ByteReaderInit(StreamReader *byteState)
{
	char type = 0;

	if (FileStreamReadByte(byteState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		byteState->currentEncodingType = VARIABLE_LENGTH;
		byteState->noOfLeftItems = -type;
		byteState->data = 0;
		byteState->step = 0;
	}
	else
	{
		/* run-length contains type + 3 elements */
		byteState->currentEncodingType = RLE;
		byteState->noOfLeftItems = type + 3;

		/* step is always 0 for byte streams */
		byteState->step = 0;

		if (FileStreamReadByte(byteState->stream, &type))
		{
			return -1;
		}
		byteState->data = (uint8_t) type;
	}

	return 0;
}


/**
 * Initialize a integer reader for reading.
 * For short,int and long signed integers are used.
 * For others unsigned integers are used.
 *
 * @param kind type of the reader, used for detecting sign
 * @param intState an integer stream reader
 *
 * @return 0 for success, -1 for failure
 */
static int 
IntegerReaderInit(FieldType__Kind kind, StreamReader *intState)
{
	char type = 0;

	/* read first byte to determine the encoding */
	if (FileStreamReadByte(intState->stream, &type))
	{
		return -1;
	}

	if (type < 0)
	{
		/* -type var-len integers follow */
		intState->currentEncodingType = VARIABLE_LENGTH;
		intState->noOfLeftItems = -type;
		intState->step = 0;
		intState->data = 0;
	}
	else
	{
		int bytesRead = 0;
		
		/* run-length contains type + 3 elements */
		intState->currentEncodingType = RLE;
		intState->noOfLeftItems = type + 3;

		/* read the step used in the run */
		if (FileStreamReadByte(intState->stream, &intState->step))
		{
			return -1;
		}

		bytesRead = ReadVarLenInteger(intState->stream, &intState->data);
		if (bytesRead < 0)
		{
			/* stream is too short */
			return -1;
		}
	}
	return 0;
}


/**
 * Frees up a stream reader
 */
int
StreamReaderFree(StreamReader *streamReader)
{
	if (streamReader == NULL)
	{
		return 0;
	}

	if (streamReader->stream != NULL)
	{
		if (FileStreamFree(streamReader->stream))
		{
			LogError("Error deleting previous compressed file stream\n");
			return -1;
		}
		streamReader->stream = NULL;
	}

	return 0;
}


/**
 * Initialize a stream reader
 *
 * @param streamReader reader to initialize
 * @param streamKind is the type of the field
 * @param offset is the offset of the data stream in the file
 * @param limit is the offset of the end of the stream
 * @param parameters contains compression format and compression block size
 *
 * @return 0 for success, 1 for failure
 */
int 
StreamReaderInit(StreamReader *streamReader, FieldType__Kind streamKind, FILE *file,
		long offset, long limit, CompressionParameters *parameters)
{

	if (streamReader->stream != NULL)
	{
		FileStreamReset(streamReader->stream, offset, limit, parameters->compressionBlockSize,
			parameters->compressionKind);
	}
	else
	{
		streamReader->stream = FileStreamInit(file, offset, limit, parameters->compressionBlockSize,
				parameters->compressionKind);
	}

	switch (streamKind)
	{
		case FIELD_TYPE__KIND__BOOLEAN:
		{
			return BooleanReaderInit(streamReader);
		}
		case FIELD_TYPE__KIND__BYTE:
		{
			return ByteReaderInit(streamReader);
		}	
		case FIELD_TYPE__KIND__SHORT:
		case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG:
		{
			return IntegerReaderInit(streamKind, streamReader);
		}	
		case FIELD_TYPE__KIND__FLOAT:
		case FIELD_TYPE__KIND__DOUBLE:
		case FIELD_TYPE__KIND__BINARY:
		{
			/* no need for initializer */
			return 0;
		}	
		default:
		{
			return -1;
		}
	}
}


/*
 * Seek in the stream reader using the offsets at offset stack.
 * It is assumed we are seeking to a forward location (not the same or previous).
 * 
 * @param fieldType orc type of the column
 * @param streamKind kind of the stream
 */
void 
StreamReaderSeek(StreamReader *streamReader, FieldType__Kind fieldType,
		FieldType__Kind streamKind, OrcStack *stack)
{
	int dataIndex = 0;
	long* positionInRun = NULL;


	/* first jump to the given location in the stream */
	FileStreamSkip(streamReader->stream, stack);

	/*
	 * Then switch by looking at the stream kind to initialize the stream readers.
	 * Some streams use run-length encoding to encode the values, so also get
	 * the current position in the run and jump to that position.
	 */
	switch (streamKind)
	{
		case FIELD_TYPE__KIND__BOOLEAN:
		{
			long* bitsRead = NULL;
			int booleanPosition = 0;

			BooleanReaderInit(streamReader);

			positionInRun = OrcStackPop(stack);
			if (positionInRun == NULL)
			{
				LogError("Error occurred while getting position in the run");
			}

			bitsRead = OrcStackPop(stack);
			if (bitsRead == NULL)
			{
				LogError("Error occurred while getting position in the run");
			}

			/* 
			 * positionInRun gives the current byte in the run. bitsRead gives the # of
			 * bits read in that byte. So we have to read the following number of bits
			 * from the stream
			 */
			booleanPosition = *positionInRun * 8 + *bitsRead;

			for (dataIndex = 0; dataIndex < booleanPosition; ++dataIndex)
			{
				ReadBoolean(streamReader);
			}

			break;
		}
		case FIELD_TYPE__KIND__BYTE:
		{
			uint8_t data8 = 0;

			ByteReaderInit(streamReader);
			positionInRun = OrcStackPop(stack);

			if (positionInRun == NULL)
			{
				LogError("Error occurred while getting position in the run");
			}

			for (dataIndex = 0; dataIndex < *positionInRun; ++dataIndex)
			{
				ReadByte(streamReader, &data8);
			}

			break;
		}
		case FIELD_TYPE__KIND__SHORT:
		case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG:
		{
			uint64_t data64 = 0;

			IntegerReaderInit(streamKind, streamReader);

			positionInRun = OrcStackPop(stack);
			if (positionInRun == NULL)
			{
				LogError("Error occurred while getting position in the run");
			}

			for (dataIndex = 0; dataIndex < *positionInRun; ++dataIndex)
			{
				ReadInteger(fieldType, streamReader, &data64);
			}

			break;
		}
		case FIELD_TYPE__KIND__BINARY:
		case FIELD_TYPE__KIND__FLOAT:
		case FIELD_TYPE__KIND__DOUBLE:
		{
			/* no need for initializer or jumping since RLE is not used in these */
			break;
		}
		default:
		{
			break;
		}
	}
}


/**
 * Reads a boolean value from the stream.
 *
 * @param booleanReaderState boolean reader
 *
 * @return 0 for false, 1 for true, -1 for error
 */
static char
ReadBoolean(StreamReader *booleanReaderState)
{
	char result = 0;

	if (booleanReaderState->mask == 0)
	{
		if (booleanReaderState->noOfLeftItems == 0)
		{
			/* try to re-initialize the reader */
			int initResult = BooleanReaderInit(booleanReaderState);
			if (initResult)
			{
				/* Error occurred while reading from input stream */
				return -1;
			}
		}
		else
		{
			if (booleanReaderState->currentEncodingType == VARIABLE_LENGTH)
			{
				/* read next byte from the stream */
				if (FileStreamReadByte(booleanReaderState->stream, &result))
				{
					return -1;
				}
				booleanReaderState->data = (uint8_t) result;
			}

			booleanReaderState->mask = 0x80;
			booleanReaderState->noOfLeftItems--;
		}
	}

	result = (booleanReaderState->data & booleanReaderState->mask) ? 1 : 0;
	booleanReaderState->mask >>= 1;

	return result;
}


/**
 * Reads a byte from the stream.
 *
 * @param byteReaderState byte reader
 * @param result used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int
ReadByte(StreamReader *byteReaderState, uint8_t *result)
{
	*result = 0;

	if (byteReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = ByteReaderInit(byteReaderState);

		if (initResult)
		{
			/* Error occurred while reading from input stream */
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (byteReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		if (FileStreamReadByte(byteReaderState->stream, (char*) result))
		{
			return -1;
		}
	}
	else
	{
		*result = byteReaderState->data;
	}

	byteReaderState->noOfLeftItems--;
	return 0;
}


/**
 * Reads an integer from the stream.
 *
 * @param kind to detect the sign
 * @param intReaderState integer reader
 * @param result used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int 
ReadInteger(FieldType__Kind kind, StreamReader *intReaderState, uint64_t *result)
{
	int bytesRead = 0;
	char step = 0;
	uint64_t data = 0;
	*result = 0;

	if (intReaderState->noOfLeftItems == 0)
	{
		/* try to re-initialize the reader */
		int initResult = IntegerReaderInit(kind, intReaderState);
		if (initResult)
		{
			/* Error occurred while reading from input stream */
			return -1;
		}
	}

	/* try to read from the stream or generate an item from run */
	if (intReaderState->currentEncodingType == VARIABLE_LENGTH)
	{
		bytesRead = ReadVarLenInteger(intReaderState->stream, &intReaderState->data);
		if (bytesRead <= 0)
		{
			/* there have to be bytes left on the stream */
			return -1;
		}
		*result = intReaderState->data;
	}
	else
	{
		step = intReaderState->step;
		data = intReaderState->data;
		*result = data;
		switch (kind)
		{
		case FIELD_TYPE__KIND__SHORT:
		case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG:
			intReaderState->data = ToUnsignedInteger(ToSignedInteger(data) + step);
			break;
		default:
			intReaderState->data += step;
			break;
		}
	}
	intReaderState->noOfLeftItems--;

	return 0;
}


/**
 * Reads a float from the stream.
 *
 * @param fpState float reader
 * @param data used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int 
ReadFloat(StreamReader *fpState, float *data)
{
	int floatLength = sizeof(float);
	char *floatBytes = FileStreamRead(fpState->stream, &floatLength);
	
	if (floatBytes == NULL || floatLength != sizeof(float))
	{
		return -1;
	}

	memcpy(data, floatBytes, floatLength);
	fpState->floatData = *data;

	return 0;
}


/**
 * Reads a double from the stream.
 *
 * @param fpState double reader
 * @param data used to store the value
 *
 * @return 0 for success, -1 for failure
 */
static int 
ReadDouble(StreamReader *fpState, double *data)
{
	int doubleLength = sizeof(double);
	char *doubleBytes = FileStreamRead(fpState->stream, &doubleLength);
	
	if (doubleBytes == NULL || doubleLength != sizeof(double))
	{
		return -1;
	}

	memcpy(data, doubleBytes, doubleLength);
	fpState->floatData = *data;

	return 0;
}


/**
 * Reads an array of bytes from the input stream.
 *
 * @param binaryReaderState binary stream reader
 * @param data to put the value
 * @param length no of bytes to read
 *
 * @return 0 for success, -1 for error
 */
static int 
ReadBinary(StreamReader *binaryReaderState, uint8_t *data, int length)
{
	int requiredLength = length;
	char *bytes = FileStreamRead(binaryReaderState->stream, &length);

	if (bytes == NULL || requiredLength != length)
	{
		return -1;
	}

	memcpy(data, bytes, length);
	return 0;
}


/**
 * Get the stream type of each data stream of a data type
 */
FieldType__Kind 
GetStreamKind(FieldType__Kind type, ColumnEncoding__Kind encoding, int streamIndex)
{
	switch (type)
	{
		case FIELD_TYPE__KIND__BINARY:
		{
			switch (streamIndex)
			{
			case 0:
				return FIELD_TYPE__KIND__BINARY;
			case 1:
				return FIELD_TYPE__KIND__INT;
			default:
				return -1;
			}
			break;
		}
		case FIELD_TYPE__KIND__BOOLEAN: case FIELD_TYPE__KIND__BYTE:
		case FIELD_TYPE__KIND__DOUBLE: case FIELD_TYPE__KIND__FLOAT:
		case FIELD_TYPE__KIND__SHORT: case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG:
		{
			return (streamIndex == 0) ? type : -1;
		}	
		case FIELD_TYPE__KIND__STRING:
		{
			switch (encoding)
			{
			case COLUMN_ENCODING__KIND__DICTIONARY:
			{
				switch (streamIndex)
				{
				case 0:
				case 1:
					return FIELD_TYPE__KIND__INT;
				case 2:
					return FIELD_TYPE__KIND__BINARY;
				default:
					return -1;
				}
			}
			case COLUMN_ENCODING__KIND__DIRECT:
			{
				switch (streamIndex)
				{
				case 0:
					return FIELD_TYPE__KIND__BINARY;
				case 1:
					return FIELD_TYPE__KIND__INT;
				default:
					return -1;
				}
			}
			default:
			{
				return -1;
			}
			}
			break;
		}
		case FIELD_TYPE__KIND__DATE:
		{
			switch (streamIndex)
			{
			case 0:
				return FIELD_TYPE__KIND__INT;
			default:
				return -1;
			}
			break;
		}
		case FIELD_TYPE__KIND__TIMESTAMP:
		{
			switch (streamIndex)
			{
			case 0:
				return FIELD_TYPE__KIND__LONG;
			case 1:
				return FIELD_TYPE__KIND__INT;
			default:
				return -1;
			}
			break;
		}
		case FIELD_TYPE__KIND__LIST:
		{
			return streamIndex ? -1 : FIELD_TYPE__KIND__INT;
		}
		default:
		{
			return -1;
		}
	}
}


/**
 * Get # data streams of a data type (excluding the present bit stream)
 */
int 
GetStreamCount(FieldType__Kind type, ColumnEncoding__Kind encoding)
{
	switch (type)
	{
		case FIELD_TYPE__KIND__BINARY:
		{
			return BINARY_STREAM_COUNT;
		}
		case FIELD_TYPE__KIND__BOOLEAN: case FIELD_TYPE__KIND__BYTE:
		case FIELD_TYPE__KIND__DOUBLE: case FIELD_TYPE__KIND__FLOAT:
		case FIELD_TYPE__KIND__SHORT: case FIELD_TYPE__KIND__INT:
		case FIELD_TYPE__KIND__LONG: case FIELD_TYPE__KIND__DATE:
		{
			return COMMON_STREAM_COUNT;
		}
		case FIELD_TYPE__KIND__STRING:
		{
			switch (encoding)
			{
			case COLUMN_ENCODING__KIND__DICTIONARY:
				return STRING_STREAM_COUNT;
			case COLUMN_ENCODING__KIND__DIRECT:
				return STRING_DIRECT_STREAM_COUNT;
			default:
				return -1;
			}
		}
		case FIELD_TYPE__KIND__TIMESTAMP:
		{
			return TIMESTAMP_STREAM_COUNT;
		}
		case FIELD_TYPE__KIND__LIST:
		{
			/* for length */
			return 1;
		}
		case FIELD_TYPE__KIND__STRUCT:
		{
			return 0;
		}
		default:
		{
			return -1;
		}
	}
}


/*
 * Reads all strings of a dictionary encoded string column into the main memory.
 */
void
FillDictionary(FieldReader* stringFieldReader)
{
	PrimitiveFieldReader *primitiveFieldReader = (PrimitiveFieldReader *) stringFieldReader->fieldReader;
	StreamReader *integerStreamReader = NULL;
	StreamReader *binaryStreamReader = NULL;
	uint64_t wordLength = 0;
	int dictionaryIndex = 0;
	int result = 0;

	/* if dictionary is NULL, read the whole dictionary to the memory */
	primitiveFieldReader->dictionary =
			alloc(sizeof(char *) * primitiveFieldReader->dictionarySize);
	primitiveFieldReader->wordLength =
			alloc(sizeof(int) * primitiveFieldReader->dictionarySize);
	integerStreamReader = &primitiveFieldReader->readers[LENGTH_STREAM];
	binaryStreamReader = &primitiveFieldReader->readers[DICTIONARY_DATA_STREAM];

	/* read the dictionary items one by one*/
	for (dictionaryIndex = 0; dictionaryIndex < primitiveFieldReader->dictionarySize;
			++dictionaryIndex)
	{
		/*
		 * First read dictionary item length. Since lengths cannot be negative,
		 * directly use that number.
		 */
		result = ReadInteger(stringFieldReader->kind, integerStreamReader, &wordLength);
		if (result < 0)
		{
			LogError("Error occurred while reading dictionary item length");
		}

		/* set the word length and allocate memory for the item */
		primitiveFieldReader->wordLength[dictionaryIndex] = (int) wordLength;
		primitiveFieldReader->dictionary[dictionaryIndex] = alloc(wordLength + 1);

		/* read the item from the file */
		result = ReadBinary(binaryStreamReader,
				(uint8_t *) primitiveFieldReader->dictionary[dictionaryIndex], (int) wordLength);

		if (result < 0)
		{
			LogError("Error occurred while reading dictionary item");
		}

		primitiveFieldReader->dictionary[dictionaryIndex][wordLength] = '\0';
	}
}

/*
 * Reads a primitive field from the reader and returns it as a Datum.
 *
 * @param isNull pointer to store whether the field is null or not
 */
Datum 
ReadPrimitiveFieldAsDatum(FieldReader *fieldReader, bool *isNull)
{
	Datum columnValue = 0;
	PrimitiveFieldReader *primitiveReader = (PrimitiveFieldReader *) fieldReader->fieldReader;
	StreamReader *integerStreamReader = NULL;
	int result = 0;

	/* first check if the field has a present bit stream */
	if (fieldReader->hasPresentBitReader)
	{
		StreamReader *presentStreamReader = &fieldReader->presentBitReader;
		char isPresent = ReadBoolean(presentStreamReader);

		if (isPresent == 0)
		{
			*isNull = true;
			return 0;
		}
		else if (isPresent < 0)
		{
			LogError("Error occurred while reading present bit stream");
			return -1;
		}
	}

	switch (OrcGetPSQLType(fieldReader))
	{
		case BOOLOID:
		{
			StreamReader *booleanStreamReader = &primitiveReader->readers[DATA_STREAM];
			columnValue = BoolGetDatum(ReadBoolean(booleanStreamReader));
			break;
		}
		case INT2OID: case INT4OID: case INT8OID: 
		{
			int64_t data64 = 0;
			uint64_t udata64 = 0;
			integerStreamReader = &primitiveReader->readers[DATA_STREAM];

			result = ReadInteger(fieldReader->kind, integerStreamReader, &udata64);

			/* convert the integer to the signed form */
			data64 = ToSignedInteger(udata64);

			switch (OrcGetPSQLType(fieldReader))
			{
				case INT2OID:
				{
					columnValue = Int16GetDatum(data64);
					break;
				}
				case INT4OID:
				{
					columnValue = Int32GetDatum(data64);
					break;
				}
				case INT8OID:
				{
					columnValue = Int64GetDatum(data64);
					break;
				}
				default:
				{
					break;
				}
			}
			break;
		}
		case FLOAT4OID:
		{
			StreamReader *fpStreamReader = &primitiveReader->readers[DATA_STREAM];
			float floatData = 0;

			result = ReadFloat(fpStreamReader, &floatData);
			columnValue = Float4GetDatum(floatData);

			break;
		}
		case FLOAT8OID:
		{
			StreamReader *fpStreamReader = &primitiveReader->readers[DATA_STREAM];
			double doubleData = 0;

			result = ReadDouble(fpStreamReader, &doubleData);
			columnValue = Float8GetDatum(doubleData);

			break;
		}
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		{
			char* dictionaryItem = NULL;

			/* check if the strings are dictionary encoded */
			if (primitiveReader->hasDictionary)
			{
				uint64_t dictionaryIndex = 0;

				/* read the dictionary item position of the current string */
				integerStreamReader = &primitiveReader->readers[DATA_STREAM];
				result = ReadInteger(fieldReader->kind, integerStreamReader, &dictionaryIndex);

				/* get the dictionary item by its position */
				dictionaryItem = primitiveReader->dictionary[dictionaryIndex];
			}
			else
			{
				StreamReader *binaryStreamReader = NULL;
				uint64_t wordLength = 0;

				/* if direct encoding is used, just read the current string */
				binaryStreamReader = &primitiveReader->readers[DATA_STREAM];
				integerStreamReader = &primitiveReader->readers[LENGTH_STREAM];

				/* read the length of the string */
				result = ReadInteger(fieldReader->kind, integerStreamReader, &wordLength);
				if (result < 0)
				{
					LogError("Error occurred while reading string length");
					return -1;
				}

				dictionaryItem = alloc(wordLength + 1);

				/* read the string from the stream */
				result = ReadBinary(binaryStreamReader, (uint8_t*) dictionaryItem, (int) wordLength);
				if (result < 0)
				{
					LogError("Error occurred while reading string");
					return -1;
				}

				dictionaryItem[wordLength] = '\0';
			}

			/* convert the C string into the Datum format */
			switch (OrcGetPSQLType(fieldReader))
			{
				case BPCHAROID:
				{
					columnValue = DirectFunctionCall3(bpcharin, CStringGetDatum(dictionaryItem),
							ObjectIdGetDatum(InvalidOid),
							Int32GetDatum(OrcGetPSQLTypeMod(fieldReader)));
					break;
				}
				case VARCHAROID:
				{
					columnValue = DirectFunctionCall3(varcharin, CStringGetDatum(dictionaryItem),
							ObjectIdGetDatum(InvalidOid),
							Int32GetDatum(OrcGetPSQLTypeMod(fieldReader)));
					break;
				}
				case TEXTOID:
				{
					columnValue = CStringGetTextDatum(dictionaryItem);
					break;
				}
				default:
				{
					break;
				}
			}

			break;
		}
		case DATEOID:
		{
			uint64_t udata64 = 0;
			int days = 0;
			integerStreamReader = &primitiveReader->readers[DATA_STREAM];

			result = ReadInteger(FIELD_TYPE__KIND__INT, integerStreamReader, &udata64);

			/* since days can be negative convert the number into signed format */
			days = ToSignedInteger(udata64);

			/* subtract the difference between the ORC epoch and PostgreSQL epoch */
			days -= ORC_PSQL_EPOCH_IN_DAYS;

			columnValue = DateADTGetDatum(days);

			break;
		}
		case TIMESTAMPOID:
		{
			StreamReader *nanoSecondsReader = NULL;
			uint64_t udata64 = 0;
			int64_t seconds = 0;
			int newNanos = 0;

			/* read seconds data of the timestamp */
			integerStreamReader = &primitiveReader->readers[DATA_STREAM];
			result = ReadInteger(FIELD_TYPE__KIND__LONG, integerStreamReader, &udata64);
			seconds = ToSignedInteger(udata64);
			seconds += ORC_DIFF_POSTGRESQL;

			if(result)
			{
				LogError("Error occurred while reading seconds value of timestamp");
			}

			/* read nano seconds data of the timestamp */
			nanoSecondsReader = &primitiveReader->readers[SECONDARY_STREAM];
			result = ReadInteger(FIELD_TYPE__KIND__INT, nanoSecondsReader, &udata64);

			/* parse the nanosecond, it is encoded such a way to remove the leading 0s */
			newNanos = ParseNanos((long) udata64);

			columnValue = TimestampGetDatum(seconds * MICROSECONDS_PER_SECOND +
					newNanos / NANOSECONDS_PER_MICROSECOND);

			break;
		}
		case NUMERICOID:
		default:
		{
			/* numeric type is not supported right now */
			result = -1;
			break;
		}
	}

	if (result)
	{
		LogError("Error occurred while reading column");
	}

	/* we have successfully read the column, mark the outcome as not null */
	*isNull = false;

	return columnValue;
}


/*
 * Reads a list field from the reader and returns it as a Datum.
 *
 * @param isNull pointer to store whether the list is null or not
 */
Datum 
ReadListFieldAsDatum(FieldReader *fieldReader, bool *isNull)
{
	ListFieldReader* listReader = fieldReader->fieldReader;
	FieldReader* itemReader = &listReader->itemReader;
	StreamReader* presentStreamReader = &fieldReader->presentBitReader;

	Datum columnValue = 0;
	Datum *datumArray = NULL;
	int datumArraySize = 0;
	uint64_t listSize = 0;
	ArrayType *columnValueObject = NULL;
	int result = 0;
	int arrayIndex = 0;
	char isListPresent = 0;
	bool isItemNull = 0;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	if (fieldReader->hasPresentBitReader)
	{
		isListPresent = ReadBoolean(presentStreamReader);

		if(isListPresent == 0)
		{
			/* list not present, return null */
			*isNull = true;
			return 0;
		}
		else if (isListPresent == -1)
		{
			/* error occurred while reading bit, return error code */
			LogError("Error occurred while reading present bit of list");
			return -1;
		}
	}

	/* read the length of the list */
	result = ReadInteger(fieldReader->kind, &listReader->lengthReader, &listSize);
	if (result)
	{
		/* Error occurred while reading the list size */
		LogError("Error occurred while reading the length of the list");
		return -1;
	}

	/* allocate enough room for datum array's maximum possible size */
	datumArray = palloc0(listSize * sizeof(Datum));
	memset(datumArray, 0, listSize * sizeof(Datum));

	for (arrayIndex = 0; arrayIndex < listSize; ++arrayIndex)
	{
		columnValue = ReadPrimitiveFieldAsDatum(itemReader, &isItemNull);

		if (isItemNull)
		{
			/* TODO currently null values in the list are skipped, is this OK? */
			continue;
		}

		datumArray[datumArraySize] = columnValue;
		datumArraySize++;
	}

	/* convert the datum array to PostgreSQL list type */
	get_typlenbyvalalign(OrcGetPSQLType(itemReader), &typeLength, &typeByValue, &typeAlignment);
	columnValueObject = construct_array(datumArray, datumArraySize, OrcGetPSQLType(itemReader),
			typeLength, typeByValue, typeAlignment);
	columnValue = PointerGetDatum(columnValueObject);

	*isNull = false;

	return columnValue;
}
