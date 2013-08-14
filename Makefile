# contrib/orc_fdw/Makefile

MODULE_big = orc_fdw
SNAPPY_FOLDER = snappy-c
OBJS = orc.pb-c.o recordReader.o util.o fileReader.o $(SNAPPY_FOLDER)/snappy.o inputStream.o orc_fdw.o
SHLIB_LINK = -lz

EXTENSION = orc_fdw
DATA = orc_fdw--1.0.sql

REGRESS = orc_fdw

EXTRA_CLEAN = sql/orc_fdw.sql expected/orc_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/orc_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif