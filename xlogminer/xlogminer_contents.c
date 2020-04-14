/*-------------------------------------------------------------------------
 *
 * Abstract:
 * Maintain the display table of analyse result.
 *
 * Authored by lichuancheng@highgo.com ,20170524
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Identification:
 * xlogminer_contents.c
 *-------------------------------------------------------------------------
*/
#include "logminer.h"
#include "xlogminer_contents.h"
#include "utils/builtins.h"
#include "catalog/indexing.h"
#include "datadictionary.h"
 


void
addSQLspace()
{
	int							addstep = PG_XLOGMINER_CONTENTS_SPACE_ADDSTEP;
	XlogminerContentsFirst*		xcftemp = NULL; 
	
	if(!srctl.xcf)
	{
		srctl.xcf = (char *)logminer_palloc(addstep * sizeof(XlogminerContentsFirst),0);
		srctl.xcftotnum = addstep;
	}
	else
	{
		xcftemp = (XlogminerContentsFirst *)logminer_palloc((addstep + srctl.xcftotnum)*sizeof(XlogminerContentsFirst),0);
		if(!xcftemp)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					errmsg("Out of memory")));
		memcpy(xcftemp, srctl.xcf, srctl.xcftotnum * sizeof(XlogminerContentsFirst));
		
		logminer_pfree(srctl.xcf,0);
		srctl.xcf = (char *)xcftemp;
		srctl.xcftotnum += addstep;
	}
	padNullToXC();
}


void
cleanSQLspace()
{
	int	loop = 0;
	XlogminerContentsFirst*		xcftemp = NULL;

	
	if(srctl.xcf)
	{
		xcftemp = (XlogminerContentsFirst*)srctl.xcf;
		for(loop = 0;loop < srctl.xcfcurnum; loop++)
		{
			xcftemp[loop].xid = 0;
			cleanSpace(&xcftemp[loop].op_text);
			cleanSpace(&xcftemp[loop].op_undo);
			cleanSpace(&xcftemp[loop].op_type);
			cleanSpace(&xcftemp[loop].record_database);
			cleanSpace(&xcftemp[loop].record_schema);
			cleanSpace(&xcftemp[loop].record_tablespace);
			cleanSpace(&xcftemp[loop].record_user);
		}
		srctl.xcfcurnum = 0;
	}
	padNullToXC();
}

void
freeSQLspace()
{
	int	loop = 0;
	XlogminerContentsFirst*		xcftemp = NULL;

	
	if(srctl.xcf)
	{
		xcftemp = (XlogminerContentsFirst*)srctl.xcf;
		for(loop = 0;loop < srctl.xcfcurnum; loop++)
		{
			freeSpace(&xcftemp[loop].op_text);
			freeSpace(&xcftemp[loop].op_undo);
			freeSpace(&xcftemp[loop].op_type);
			freeSpace(&xcftemp[loop].record_database);
			freeSpace(&xcftemp[loop].record_schema);
			freeSpace(&xcftemp[loop].record_tablespace);
			freeSpace(&xcftemp[loop].record_user);
		}
		srctl.xcftotnum = 0;
		srctl.xcfcurnum = 0;
		logminer_pfree(srctl.xcf,0);
		srctl.xcf = NULL;
	}
}

void
InsertXlogContentsTuple(Form_xlogminer_contents fxc)
{
	Relation	xlogminer_contents = NULL;
	HeapTuple	tup = NULL;
	Oid			reloid = 0;
	text		*op_text = NULL;
	text		*op_undo = NULL;
	text		*op_type = NULL;
	text		*record_database = NULL;
	text		*record_user = NULL;
	text		*record_tablespace = NULL;
	text		*record_schema = NULL;
	Datum		values[Natts_xlogminer_contents];
	bool		nulls[Natts_xlogminer_contents];
	
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	if(0 == rrctl.logprivate.xlogminer_contents_oid)
		if(!getRelationOidByName(PG_LOGMINER_DICTIONARY_TEMPTABLE,&rrctl.logprivate.xlogminer_contents_oid,true))
			ereport(ERROR,(errmsg("It is failed to open temporary table xlogminer_contents.")));/*should not happen*/
	reloid = rrctl.logprivate.xlogminer_contents_oid;
	values[Anum_xlogminer_contents_xid - 1] = Int64GetDatum(fxc->xid);
	values[Anum_xlogminer_contents_virtualxid - 1] = Int32GetDatum(fxc->virtualxid);
	values[Anum_xlogminer_contents_timestamp - 1] = TimestampTzGetDatum(fxc->timestamp);
	if(fxc->record_database)
	{
		record_database =  cstring_to_text(fxc->record_database);
		values[Anum_xlogminer_contents_record_database - 1] = PointerGetDatum(record_database);
	}
	else
		nulls[Anum_xlogminer_contents_record_database - 1] = true;
	
	if(fxc->record_user)
	{
		record_user =  cstring_to_text(fxc->record_user);
		values[Anum_xlogminer_contents_record_user - 1] = PointerGetDatum(record_user);
	}
	else
		nulls[Anum_xlogminer_contents_record_user - 1] = true;
	
	if(fxc->record_tablespace)
	{
		record_tablespace = cstring_to_text(fxc->record_tablespace);
		values[Anum_xlogminer_contents_record_tablespace - 1] = PointerGetDatum(record_tablespace);
	}
	else
		nulls[Anum_xlogminer_contents_record_tablespace - 1] = true;

	if(fxc->record_schema)
	{
		record_schema = cstring_to_text(fxc->record_schema);
		values[Anum_xlogminer_contents_record_schema - 1] = PointerGetDatum(record_schema);
	}
	else
		nulls[Anum_xlogminer_contents_record_schema - 1] = true;
	
	if(fxc->op_type)
	{
		op_type =  cstring_to_text(fxc->op_type);
		values[Anum_xlogminer_contents_op_type - 1] = PointerGetDatum(op_type);
	}
	else
		nulls[Anum_xlogminer_contents_op_type - 1] = true;
	if(fxc->op_text)
	{
		op_text = cstring_to_text(fxc->op_text);
		values[Anum_xlogminer_contents_op_text - 1] = PointerGetDatum(op_text);
	}
	else
		nulls[Anum_xlogminer_contents_op_text - 1] = true;
	if(fxc->op_undo)
	{
		op_undo = cstring_to_text(fxc->op_undo);
		values[Anum_xlogminer_contents_op_undo - 1] = PointerGetDatum(op_undo);
	}
	else
		nulls[Anum_xlogminer_contents_op_undo - 1] = true;

	xlogminer_contents = heap_open(reloid, AccessShareLock);
	if(!xlogminer_contents)
		ereport(ERROR,(errmsg("It is failed to open temporary table xlogminer_contents.")));

	tup = heap_form_tuple(RelationGetDescr(xlogminer_contents), values, nulls);
	simple_heap_insert(xlogminer_contents, tup);
	CatalogUpdateIndexes(xlogminer_contents, tup);
	logminer_pfree((char *)op_text,0);
	op_text = NULL;
	logminer_pfree((char *)op_undo,0);
	op_undo = NULL;
	logminer_pfree((char *)op_type,0);
	op_type = NULL;
	logminer_pfree((char *)record_database,0);
	record_database = NULL;
	logminer_pfree((char *)record_user,0);
	record_user = NULL;
	logminer_pfree((char *)record_tablespace,0);
	record_tablespace = NULL;
	logminer_pfree((char *)record_schema,0);
	record_schema = NULL;
	
	if(tup)
		heap_freetuple(tup);
	if(xlogminer_contents)
		heap_close(xlogminer_contents, AccessShareLock);
}
