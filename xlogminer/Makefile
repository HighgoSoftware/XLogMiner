# contrib/xlogminer/Makefile

MODULE_big	= xlogminer
OBJS		= pg_logminer.o logminer.o organizsql.o xlogreader_logminer.o datadictionary.o xlogminer_contents.o

EXTENSION = xlogminer
DATA = xlogminer--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/xlogminer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
