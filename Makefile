CC			 =	gcc
LIBS		+= 	`pkg-config --libs libprotobuf-c zlib`
INCLUDES	+= 	`pkg-config --cflags libprotobuf-c zlib`
CFLAGS		 = 	-Wall -O3 -Wmissing-prototypes -Wpointer-arith \
				-Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute \
				-Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard
#not added flags -g -pg

SNAPPY_FOLDER	= snappy-c
EXEC_FOLDER		= out

READMETADATA_OBJECTS	= orc_proto.pb-c.o $(SNAPPY_FOLDER)/snappy.o util.o fileReader.o recordReader.o readMetadata.o inputStream.o
TESTFILEREADER_OBJECTS	= orc_proto.pb-c.o testFileReader.o recordReader.o util.o fileReader.o $(SNAPPY_FOLDER)/snappy.o inputStream.o 
EXECUTABLES				= readMetadata testFileReader

all:			snappy_c orc_proto $(EXECUTABLES) 
				mv $(EXECUTABLES) $(EXEC_FOLDER)

snappy_c:
				cd snappy-c && make

orc_proto: 		orc_proto.pb-c.c

orc_proto.pb-c.c:	
				protoc-c --c_out=. orc_proto.proto

.SUFFIXES: 		.c .o

.c.o:	
				$(CC) $(CFLAGS) $(INCLUDES) -c $<

.o:		
				$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

readMetadata:	$(READMETADATA_OBJECTS)

testFileReader:	$(TESTFILEREADER_OBJECTS)

.PHONY:			clean

clean:	
				rm -f $(EXEC_FOLDER)/* *.o gmon.out
