CC			 =	gcc
LIBS		+= 	`pkg-config --libs libprotobuf-c zlib`
INCLUDES	+= 	`pkg-config --cflags libprotobuf-c zlib`
CFLAGS		 = 	-Wall -g -pg -Wmissing-prototypes -Wpointer-arith \
				-Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute \
				-Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard
#not added flags -g -pg

READMETADATA_OBJECTS	= orc.pb-c.o snappy.o orcUtil.o fileReader.o recordReader.o readMetadata.o inputStream.o
TESTFILEREADER_OBJECTS	= orc.pb-c.o testFileReader.o recordReader.o orcUtil.o fileReader.o snappy.o inputStream.o 
EXECUTABLES				= readMetadata testFileReader

all:			orc $(EXECUTABLES) 

orc: 			orc.pb-c.c

orc.pb-c.c:	
				protoc-c --c_out=. orc.proto

.SUFFIXES: 		.c .o

.c.o:	
				$(CC) $(CFLAGS) $(INCLUDES) -c $<

.o:		
				$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

readMetadata:	$(READMETADATA_OBJECTS)

testFileReader:	$(TESTFILEREADER_OBJECTS)

.PHONY:			clean

clean:	
				rm -f *.o gmon.out $(EXECUTABLES)