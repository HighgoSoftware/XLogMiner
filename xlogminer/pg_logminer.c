/*-------------------------------------------------------------------------
 *
 * Abstract:
 * Main analyse function of XLogminer
 *
 * Authored by lichuancheng@highgo.com ,20170524
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Identification:
 * pg_logminer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include <dirent.h>

#include "pg_logminer.h"
#include "logminer.h"
#include "catalog/pg_class.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_auth_members.h"
#include "access/transam.h"
#include "commands/dbcommands_xlog.h"
#include "datadictionary.h"
#include "xlogminer_contents.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_type.h"




RecordRecycleCtl rrctl;
SQLRecycleCtl	srctl;
uint32	sqlnoser;


static SystemClass sysclass[PG_LOGMINER_SYSCLASS_MAX];
static int sysclassNum = 0;
RelationKind *relkind_miner;

SysClassLevel ImportantSysClass[] = {
	{PG_LOGMINER_IMPTSYSCLASS_PGCLASS, "pg_class", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGDATABASE, "pg_database", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGEXTENSION, "pg_extension", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGNAMESPACE, "pg_namespace", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGTABLESPACE, "pg_tablespace", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGCONSTRAINT, "pg_constraint", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGAUTHID, "pg_authid", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGPROC, "pg_proc", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGDEPEND, "pg_depend", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGINDEX, "pg_index", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGATTRIBUTE, "pg_attribute", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGSHDESC, "pg_shdescription", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGATTRDEF, "pg_attrdef", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGTYPE, "pg_type", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGAUTH_MEMBERS, "pg_auth_members", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGINHERITS, "pg_inherits", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGTRIGGER, "pg_trigger", 0},
	{PG_LOGMINER_IMPTSYSCLASS_PGLANGUAGE, "pg_language", 0}
	
};

static SQLKind sqlkind[] = {
	{"UPDATE",PG_LOGMINER_SQLKIND_UPDATE},
	{"INSERT",PG_LOGMINER_SQLKIND_INSERT},
	{"DELETE",PG_LOGMINER_SQLKIND_DELETE},
	{"CREATE",PG_LOGMINER_SQLKIND_CREATE},
	{"ALTER",PG_LOGMINER_SQLKIND_ALTER},
	{"XACT",PG_LOGMINER_SQLKIND_XACT},
	{"DROP",PG_LOGMINER_SQLKIND_DROP}
};



PG_MODULE_MAGIC;

/*
PG_FUNCTION_INFO_V1(pg_xlog2sql);
*/
PG_FUNCTION_INFO_V1(xlogminer_build_dictionary);
PG_FUNCTION_INFO_V1(xlogminer_load_dictionary);
PG_FUNCTION_INFO_V1(xlogminer_stop);
PG_FUNCTION_INFO_V1(xlogminer_xlogfile_add);
PG_FUNCTION_INFO_V1(xlogminer_xlogfile_list);
PG_FUNCTION_INFO_V1(xlogminer_xlogfile_remove);
PG_FUNCTION_INFO_V1(pg_minerXlog);


static bool tableIfSysclass(char *tablename, Oid reloid);
static bool tableifImpSysclass(char *tablename, Oid reloid);
static void getTupleData_Insert(XLogReaderState *record, char** tuple_info, Oid reloid);
static void getTupleData_Delete(XLogReaderState *record, char** tuple_info, Oid reloid);
static void getTupleData_Update(XLogReaderState *record, char** tuple_info, char** tuple_info_old,Oid reloid);
static bool getTupleInfoByRecord(XLogReaderState *record, uint8 info, NameData* relname,char** schname, char** tuple_info, char** tuple_info_old);
static void minerHeapInsert(XLogReaderState *record, XLogMinerSQL *sql_simple,uint8 info);
static void minerHeapDelete(XLogReaderState *record, XLogMinerSQL *sql_simple,uint8 info);
static void minerHeapUpdate(XLogReaderState *record, XLogMinerSQL *sql_simple, uint8 info);
static void minerHeap2MutiInsert(XLogReaderState *record, XLogMinerSQL *sql_simple, uint8 info)	;
static bool getNextRecord();
static int getsqlkind(char *sqlheader);
static bool parserInsertSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt);
static bool parserDeleteSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt);
static bool parserUpdateSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt);
static void parserXactSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt);
static void XLogMinerRecord_heap(XLogReaderState *record, XLogMinerSQL *sql_simple);
static void XLogMinerRecord_heap2(XLogReaderState *record, XLogMinerSQL *sql_simple);
static void XLogMinerRecord_dbase(XLogReaderState *record, XLogMinerSQL *sql_simple);
static void XLogMinerRecord_xact(XLogReaderState *record, XLogMinerSQL *sql_simple, TimestampTz *xacttime);
static bool XLogMinerRecord(XLogReaderState *record, XLogMinerSQL *sql_simple,TimestampTz *xacttime);
static bool sqlParser(XLogReaderState *record, TimestampTz 	*xacttime);


SysClassLevel *getImportantSysClass()
{
	return ImportantSysClass;
}

/*
*append a string to  XLogMinerSQL
*/
void
appendtoSQL(XLogMinerSQL *sql_simple, char *sqlpara , int spaceKind)
{

	int addsize;

	if(PG_LOGMINER_SQLPARA_OTHER != spaceKind && PG_LOGMINER_XLOG_DBINIT == rrctl.system_init_record)
		return;
	
	if(NULL == sqlpara || 0 == strcmp("",sqlpara) || 0 == strcmp(" ",sqlpara) )
		sqlpara = "NULL";
	
	addsize = strlen(sqlpara);

	while(addsize >= sql_simple->rem_size)
	{
		addSpace(sql_simple,spaceKind);
	}

	memcpy(sql_simple->sqlStr + sql_simple->use_size ,sqlpara ,addsize);
	sql_simple->use_size += addsize;
	sql_simple->rem_size -= addsize;
}

void
appendtoSQL_simquo(XLogMinerSQL *sql_simple, char* ptr, bool quoset)
{
	if(quoset)
		appendtoSQL(sql_simple, "\'", PG_LOGMINER_SQLPARA_OTHER);
	appendtoSQL(sql_simple, ptr, PG_LOGMINER_SQLPARA_OTHER);
	if(quoset)
		appendtoSQL(sql_simple, "\'", PG_LOGMINER_SQLPARA_OTHER);

}

void
appendtoSQL_doubquo(XLogMinerSQL *sql_simple, char* ptr, bool quoset)
{
	if(quoset)
		appendtoSQL(sql_simple, "\"", PG_LOGMINER_SQLPARA_SIMPLE);
	appendtoSQL(sql_simple, ptr, PG_LOGMINER_SQLPARA_SIMPLE);
	if(quoset)
		appendtoSQL(sql_simple, "\"", PG_LOGMINER_SQLPARA_SIMPLE);

}

void
appendtoSQL_atttyptrans(XLogMinerSQL *sql_simple, Oid typoid)
{
	if(POINTOID == typoid || JSONOID == typoid || (POLYGONOID == typoid) || XMLOID == typoid)
		appendtoSQL(sql_simple, "::text", PG_LOGMINER_SQLPARA_SIMPLE);
		
}

void
appendtoSQL_valuetyptrans(XLogMinerSQL *sql_simple, Oid typoid)
{
	if(FLOAT4OID == typoid)
		appendtoSQL(sql_simple, "::float4", PG_LOGMINER_SQLPARA_SIMPLE);
}


/*
*
* Wipe some string from XLogMinerSQL.
* For example
* sql_simple.sqlStr="delete from t1 where values"
* fromstr="where values"
* checkstr="where "
* then sql_simple.sqlStr become "delete from t1 where "
*/
void
wipeSQLFromstr(XLogMinerSQL *sql_simple,char *fromstr,char *checkstr)
{
	char	*strPtr = NULL;
	int 	length_ptr;
	

	if(NULL == sql_simple || NULL == sql_simple->sqlStr ||NULL == fromstr)
		return;
	strPtr = strstr(sql_simple->sqlStr,fromstr);
	if(NULL == strPtr)
		return;
	strPtr = strPtr + strlen(checkstr);
	length_ptr = strlen(strPtr);
	memset(strPtr, 0, length_ptr);
	sql_simple->use_size -= length_ptr;
	sql_simple->rem_size += length_ptr;
}

/*
* Append a space to XLogMinerSQL
*/
void
appendBlanktoSQL(XLogMinerSQL *sql_simple)
{

	int addsize;
	char *sqlpara = " ";
	
	addsize = strlen(sqlpara);
	while(addsize >= sql_simple->rem_size)
	{
		addSpace(sql_simple,PG_LOGMINER_SQLPARA_OTHER);
	}
	memcpy(sql_simple->sqlStr + sql_simple->use_size ,sqlpara ,addsize);
	sql_simple->use_size += addsize;
	sql_simple->rem_size -= addsize;
}

static bool
tableIfSysclass(char *tablename, Oid reloid)
{
	int loop;
	if(FirstNormalObjectId < reloid)
		return false;
	for(loop = 0; loop < sysclassNum; loop++)
	{
		if(0 == strcmp(sysclass[loop].classname.data,tablename))
		{
			return true;
		}
	}
	return false;
}

static bool
tableifImpSysclass(char *tablename, Oid reloid)
{
	int loop;
	if(FirstNormalObjectId < reloid)
		return false;
	for(loop = 0; loop < PG_LOGMINER_IMPTSYSCLASS_IMPTNUM; loop++)
	{
		if(0 == strcmp(ImportantSysClass[loop].relname,tablename))
		{
			return true;
		}
	}
	return false;
}

/*
*	Get useful data from record,and return insert data by tuple_info. 
*/
static void 
getTupleData_Insert(XLogReaderState *record, char** tuple_info, Oid reloid)
{
	HeapTupleData 			tupledata;
	char					*tuplem = NULL;
	char					*data = NULL;
	TupleDesc				tupdesc = NULL;
	Size					datalen = 0;
	uint32					newlen = 0;
	HeapTupleHeader 		htup;
	xl_heap_header 			xlhdr;
	RelFileNode 			target_node;
	BlockNumber 			blkno;
	ItemPointerData 		target_tid;
	xl_heap_insert 			*xlrec = (xl_heap_insert *) XLogRecGetData(record);

	if(!rrctl.tupinfo_init)
	{
		memset(&rrctl.tupinfo, 0, sizeof(XLogMinerSQL));
		rrctl.tupinfo_init = true;
	}
	else
		cleanSpace(&rrctl.tupinfo);
	memset(&tupledata, 0, sizeof(HeapTupleData));
	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	data = XLogRecGetBlockData(record, 0, &datalen);
	if(!data)
		return;

	newlen = datalen - SizeOfHeapHeader;
	Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;

	tuplem = rrctl.tuplem;
	rrctl.reloid = reloid;
	htup = (HeapTupleHeader)tuplem;
	memcpy((char *) htup + SizeofHeapTupleHeader,
		   data,newlen);
	newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;

	if(rrctl.tupdesc)
	{
		freetupdesc(rrctl.tupdesc);
		rrctl.tupdesc = NULL;
	}
		
	rrctl.tupdesc = GetDescrByreloid(reloid);
	tupdesc = rrctl.tupdesc;

	if(NULL != htup)
	{
		tupledata.t_data = htup;
		if(rrctl.toastrel)
		{
			/*if it is insert into a toast table,store it into list*/
			Oid		chunk_id;
			int		chunk_seq;
			char	*chunk_data;
			bool	isnull = false;

			chunk_id = DatumGetObjectId(fastgetattr(&tupledata, 1, tupdesc, &isnull));
			chunk_seq =DatumGetInt32(fastgetattr(&tupledata, 2, tupdesc, &isnull));
			chunk_data = DatumGetPointer(fastgetattr(&tupledata, 3, tupdesc, &isnull));
			toastTupleAddToList(makeToastTuple(VARSIZE(chunk_data) - VARHDRSZ, VARDATA(chunk_data), chunk_id, chunk_seq));
			return;
		}
		else
		{
			rrctl.sqlkind = PG_LOGMINER_SQLKIND_INSERT;
			mentalTup(&tupledata, tupdesc, &rrctl.tupinfo, false);
			*tuple_info = rrctl.tupinfo.sqlStr;
			rrctl.sqlkind = 0;
		}
	}
}

/*
*	Get useful data from record,and return delete data by tuple_info. 
*/

static void 
getTupleData_Delete(XLogReaderState *record, char** tuple_info, Oid reloid)
{
	HeapTupleData			tupledata;
	char					*tuplem = NULL;
	char					*data = NULL;
	TupleDesc				tupdesc = NULL;
	uint16					newlen = 0;
	HeapTupleHeader 		htup;
	xl_heap_header			xlhdr;
	RelFileNode 			target_node;
	BlockNumber 			blkno;
	ItemPointerData 		target_tid;
	xl_heap_delete			*xlrec = (xl_heap_delete *) XLogRecGetData(record);

	if(!rrctl.tupinfo_init)
	{
		memset(&rrctl.tupinfo, 0, sizeof(XLogMinerSQL));
		rrctl.tupinfo_init = true;
	}
	else
		cleanSpace(&rrctl.tupinfo);
	memset(&tupledata, 0, sizeof(HeapTupleData));

	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	data = (char *) xlrec + SizeOfHeapDelete;
	if(!data)
		return;
	newlen = XLogRecGetDataLen(record) - SizeOfHeapDelete;
	if(!(XLH_DELETE_CONTAINS_OLD & xlrec->flags))
		return;

	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;
	if(newlen + SizeOfHeapUpdate > MaxHeapTupleSize)
	{
		rrctl.tuplem_bigold = getTuplemSpace(newlen + SizeOfHeapUpdate + SizeofHeapTupleHeader);
		tuplem = rrctl.tuplem_bigold;
	}
	else
		tuplem = rrctl.tuplem;
	rrctl.reloid = reloid;
	htup = (HeapTupleHeader)tuplem;
	memcpy((char *) htup + SizeofHeapTupleHeader,data,newlen);
	newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;
	if(rrctl.tupdesc)
	{
		freetupdesc(rrctl.tupdesc);
		rrctl.tupdesc = NULL;
	}
		
	rrctl.tupdesc = GetDescrByreloid(reloid);
	tupdesc = rrctl.tupdesc;
	if(NULL != htup)
	{
		tupledata.t_data = htup;
		if(rrctl.toastrel)
		{
			/*if it is insert into a toast table,store it into list*/
			Oid		chunk_id;
			int		chunk_seq;
			char	*chunk_data;
			bool	isnull = false;

			chunk_id = DatumGetObjectId(fastgetattr(&tupledata, 1, tupdesc, &isnull));
			chunk_seq =DatumGetInt32(fastgetattr(&tupledata, 2, tupdesc, &isnull));
			chunk_data = DatumGetPointer(fastgetattr(&tupledata, 3, tupdesc, &isnull));
			toastTupleAddToList(makeToastTuple(VARSIZE(chunk_data) - VARHDRSZ, VARDATA(chunk_data), chunk_id, chunk_seq));
			return;
		}
		else
		{
			rrctl.sqlkind = PG_LOGMINER_SQLKIND_DELETE;
			mentalTup(&tupledata, tupdesc, &rrctl.tupinfo, false);
			*tuple_info = rrctl.tupinfo.sqlStr;
			rrctl.sqlkind = 0;
		}
	}
}


/*
*	Get useful data from record,and return update data by tuple_info and tuple_info_old. 
*/
static void 
getTupleData_Update(XLogReaderState *record, char** tuple_info, char** tuple_info_old,Oid reloid)
{
	xl_heap_update *xlrec = NULL;
	xl_heap_header *xlhdr = NULL;
	xl_heap_header *xlhdr_old;
	uint32			newlen = 0;
	
	char			*tuplem;
	char			*tuplem_old;
	HeapTupleHeader htup;
	HeapTupleHeader htup_old;
	TupleDesc		tupdesc = NULL;
	HeapTupleData 	tupledata;
	
	Size			datalen = 0;
	char 			*recdata = NULL;
	char	   		*newp = NULL;
	RelFileNode 			target_node;
	
	ItemPointerData 		target_tid;
	ItemPointerData 		target_tid_old;
	BlockNumber 			newblk;
	BlockNumber 			oldblk;
	if(!rrctl.tupinfo_init)
	{
		memset(&rrctl.tupinfo, 0, sizeof(XLogMinerSQL));
		rrctl.tupinfo_init = true;
	}
	else
		cleanSpace(&rrctl.tupinfo);
	if(!rrctl.tupinfo_old_init)
	{
		memset(&rrctl.tupinfo_old, 0, sizeof(XLogMinerSQL));
		rrctl.tupinfo_old_init = true;
	}
	else
		cleanSpace(&rrctl.tupinfo_old);
	memset(&tupledata, 0, sizeof(HeapTupleData));

	xlrec = (xl_heap_update *) XLogRecGetData(record);
	XLogRecGetBlockTag(record, 0, &target_node, NULL, &newblk);
	if (!XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
		oldblk = newblk;
	ItemPointerSet(&target_tid, newblk, xlrec->new_offnum);
	ItemPointerSet(&target_tid_old, oldblk, xlrec->old_offnum);

	if(xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
	{
		recdata = XLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		xlhdr = (xl_heap_header *)recdata;
		recdata += SizeOfHeapHeader;	
		tuplem = rrctl.tuplem;
		rrctl.reloid = reloid;
		htup = (HeapTupleHeader)tuplem;

		newp = (char *) htup + offsetof(HeapTupleHeaderData, t_bits);
		memcpy(newp, recdata, newlen);
		recdata += newlen;
		newp += newlen;
		htup->t_infomask2 = xlhdr->t_infomask2;
		htup->t_infomask = xlhdr->t_infomask;


		htup->t_hoff = xlhdr->t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;


		if(rrctl.tupdesc)
		{
			freetupdesc(rrctl.tupdesc);
			rrctl.tupdesc = NULL;
		}
		rrctl.tupdesc = GetDescrByreloid(reloid);
		tupdesc = rrctl.tupdesc;
	}
	else
		return;

	if(xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
	{
		recdata = XLogRecGetData(record) + SizeOfHeapUpdate;
		datalen = XLogRecGetDataLen(record) - SizeOfHeapUpdate;
		if(datalen + SizeOfHeapUpdate > MaxHeapTupleSize)
		{
			rrctl.tuplem_bigold = getTuplemSpace(datalen + SizeOfHeapUpdate + SizeofHeapTupleHeader);
			tuplem_old = rrctl.tuplem_bigold;
		}
		else
		{
			tuplem_old = rrctl.tuplem_old;
		}
		htup_old = (HeapTupleHeader)tuplem_old;
		xlhdr_old  = (xl_heap_header *)recdata;
		recdata += SizeOfHeapHeader;
		newp = (char *) htup_old + offsetof(HeapTupleHeaderData, t_bits);
		memcpy(newp, recdata, datalen);
		newp += datalen ;
		recdata += datalen ;

		htup_old->t_infomask2 = xlhdr_old->t_infomask2;
		htup_old->t_infomask = xlhdr_old->t_infomask;

		htup_old->t_hoff = xlhdr_old->t_hoff;
		HeapTupleHeaderSetXmin(htup_old, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup_old, FirstCommandId);
		htup_old->t_ctid = target_tid_old;
	}
	else
		return;


	if(NULL != htup)
	{
		rrctl.sqlkind = PG_LOGMINER_SQLKIND_UPDATE;
		tupledata.t_data = htup_old;
		mentalTup(&tupledata, tupdesc, &rrctl.tupinfo_old, true);
		*tuple_info_old = rrctl.tupinfo_old.sqlStr;
		tupledata.t_data = htup;
		mentalTup(&tupledata, tupdesc, &rrctl.tupinfo, false);
		*tuple_info = rrctl.tupinfo.sqlStr;
		rrctl.sqlkind = 0;
	}
}

/*Func control that if we reached the valid record*/
void 
processContrl(char* relname, int	contrlkind)
{
	
	if(PG_LOGMINER_XLOG_NOMAL == rrctl.system_init_record)
		return;
	if(PG_LOGMINER_CONTRLKIND_FIND == contrlkind &&
		(0 == strcmp(relname,PG_LOGMINER_DATABASE_HIGHGO) || 0 == strcmp(relname,PG_LOGMINER_DATABASE_POSTGRES)))
	{
		/*We have got "highgo" or "postgres" db create sql.*/
		rrctl.sysstoplocation = PG_LOGMINER_FLAG_FINDHIGHGO;
	}
	else if(PG_LOGMINER_CONTRLKIND_XACT == contrlkind)
	{
		rrctl.sysstoplocation++;
		if(PG_LOGMINER_FLAG_INITOVER == rrctl.sysstoplocation)
		{
			/*We got xact commit just after db create mentioned above*/
			rrctl.system_init_record = PG_LOGMINER_XLOG_NOMAL;
		}
	}
}


static bool
getTupleInfoByRecord(XLogReaderState *record, uint8 info, NameData* relname,char** schname, char** tuple_info, char** tuple_info_old)
{

	RelFileNode 		*node = NULL;
	Oid					reloid = 0;
	Oid					dboid = 0;
	uint8				rmid = XLogRecGetRmid(record);
	BlockNumber 		blknum = 0;

	dboid = getDataDicOid();
	cleanMentalvalues();

	XLogRecGetBlockTag(record, 0, &srctl.rfnode, NULL, &blknum);
	node = &srctl.rfnode;
	if(dboid != node->dbNode)
		return false;

	/*Get which relation it was belonged to*/
	reloid = getRelationOidByRelfileid(node->relNode);
	if(0 == reloid)
	{
		rrctl.reloid = 0;
		rrctl.nomalrel = true;
		/*ereport(NOTICE,(errmsg("Relfilenode %d can not be handled",node->relNode)));*/
		return false;
	}
	
	if(-1 == getRelationNameByOid(reloid, relname))
		return false;
	*schname = getnsNameByReloid(reloid);
	rrctl.sysrel = tableIfSysclass(relname->data,reloid);
	rrctl.nomalrel = (!rrctl.sysrel) && (!tableIftoastrel(reloid));
	rrctl.imprel = tableifImpSysclass(relname->data,reloid);
	rrctl.toastrel = tableIftoastrel(reloid);
	rrctl.recordxid = XLogRecGetXid(record);
	if(rrctl.nomalrel)
	{
		rrctl.tbsoid = node->spcNode;
	}
	/*We does not care unuseful catalog relation
		We does not care update toast relation*/
	if(!rrctl.nomalrel && !rrctl.toastrel)
		return false;	
	if(XLOG_HEAP_INSERT == info && RM_HEAP_ID == rmid)
	{
		getTupleData_Insert(record, tuple_info, reloid);
	}
	else if((XLOG_HEAP_HOT_UPDATE == info || XLOG_HEAP_UPDATE == info) && RM_HEAP_ID == rmid)
	{
		getTupleData_Update(record, tuple_info, tuple_info_old, reloid);
	}
	else if(XLOG_HEAP_DELETE == info && RM_HEAP_ID == rmid)
	{
		getTupleData_Delete(record, tuple_info, reloid);
	}
	return true;
}

static void 
minerHeapInsert(XLogReaderState *record, XLogMinerSQL *sql_simple,uint8 info)
{
	NameData			relname;
	char*				schname;
	bool				sqlFind = false;
	char				*tupleInfo = NULL;
	bool				nomalrel = false;
	bool				sysrel = false;

	memset(&relname, 0, sizeof(NameData));
	sqlFind = getTupleInfoByRecord(record, info, &relname, &schname, &tupleInfo ,NULL);
	if(!sqlFind)
		return;
	nomalrel = rrctl.nomalrel;
	sysrel = rrctl.sysrel;
	/*Assemble "table name","tuple data" and "describe word"*/
	getInsertSQL(sql_simple,tupleInfo,&relname, schname, sysrel);
	if(nomalrel && elemNameFind(relname.data) && 0 == rrctl.prostatu)
	{
		/*Get undo sql*/
		getDeleteSQL(&srctl.sql_undo,tupleInfo,&relname, schname, sysrel, true);
		/*
		*Format delete sql
		*"where values(1,2,3);"-->"where i = 1 AND j = 2 AND k = 3;"
		*/
		reAssembleDeleteSql(&srctl.sql_undo, true);
	}
}

static void
minerHeapDelete(XLogReaderState *record, XLogMinerSQL *sql_simple,uint8 info)
{
	bool				sqlFind = false;
	NameData			relname;
	char*				schname;
	char 				*tupleInfo = NULL;
	bool				nomalrel = false;
	bool				sysrel = false;

	memset(&relname, 0, sizeof(NameData));
	sqlFind = getTupleInfoByRecord(record, info, &relname, &schname, &tupleInfo, NULL);
	if(!sqlFind)
		return;
	nomalrel = rrctl.nomalrel;
	sysrel = rrctl.sysrel;
	/*Assemble "table name","tuple data" and "describe word"*/
	getDeleteSQL(sql_simple,tupleInfo,&relname,schname,sysrel, false);
	if(nomalrel && elemNameFind(relname.data) && 0 == rrctl.prostatu)
	{
		/*Get undo sql*/
		getInsertSQL(&srctl.sql_undo,tupleInfo,&relname,schname,sysrel);
	}	
}

static void
minerHeapUpdate(XLogReaderState *record, XLogMinerSQL *sql_simple, uint8 info)
{
	
	NameData			relname;
	char*				schname;
	bool				sqlFind = false;
	char 				*tupleInfo = NULL;
	char				*tupleInfo_old = NULL;
	bool				nomalrel = false;
	bool				sysrel = false;

	memset(&relname, 0, sizeof(NameData));
	sqlFind = getTupleInfoByRecord(record, info, &relname, &schname, &tupleInfo, &tupleInfo_old);
	if(!sqlFind)
		return;
	nomalrel = rrctl.nomalrel;
	sysrel = rrctl.sysrel;
	/*Assemble "table name","tuple data" and "describe word"*/
	getUpdateSQL(sql_simple, tupleInfo, tupleInfo_old, &relname, schname, sysrel);

	if(nomalrel && elemNameFind(relname.data)  && 0 == rrctl.prostatu)
	{
		/*Get undo sql*/
		getUpdateSQL(&srctl.sql_undo, tupleInfo_old, tupleInfo, &relname, schname, sysrel);
		/*
		*Format update sql
		*"update t1 set values(1,2,4) where values(1,2,3);"
		*-->"update t1 set j = 4 where i = 1 AND j = 2 AND k = 3"
		*/
		reAssembleUpdateSql(&srctl.sql_undo,true);
	}
}

static void
minerHeap2MutiInsert(XLogReaderState *record, XLogMinerSQL *sql_simple, uint8 info)	
{
	RelFileNode 			rnode;
	HeapTupleData 			tupledata;
	NameData				relname;
	char					*schname = NULL;
	char					*tuple_info = NULL;
	xl_heap_multi_insert 	*xlrec = NULL;
	xl_multi_insert_tuple 	*xlhdr = NULL;
	char	   				*data = NULL;
	char	   				*tldata = NULL;
	Size					tuplelen = 0;
	
	BlockNumber 			blkno = 0;
	ItemPointerData 		target_tid;
	Oid						reloid = 0;
	HeapTupleHeader 		htup = NULL;
	int						datalen = 0;
	

	memset(&rnode, 0, sizeof(RelFileNode));
	memset(&tupledata, 0, sizeof(HeapTupleData));
	memset(&relname, 0, sizeof(NameData));

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);
	if(getDataDicOid() != rnode.dbNode)
		return;


	if(!rrctl.tupinfo_init)
	{
		memset(&rrctl.tupinfo, 0, sizeof(XLogMinerSQL));
		rrctl.tupinfo_init = true;
	}
	else
		cleanSpace(&rrctl.tupinfo);
	

	
	xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);	
	tldata = XLogRecGetBlockData(record, 0, &tuplelen);

	if(!srctl.mutinsert)
		data = tldata;
	else
		data = srctl.multdata;

	if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE)
	{
		ItemPointerSetBlockNumber(&target_tid, blkno);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offsets[srctl.sqlindex]);
		xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data);
		data = ((char *) xlhdr) + SizeOfMultiInsertTuple;
		datalen = xlhdr->datalen;

		htup = (HeapTupleHeader)rrctl.tuplem;

		memcpy((char*)htup + SizeofHeapTupleHeader, (char*)data, datalen);
		data += datalen;
		srctl.multdata = data;

		htup->t_infomask = xlhdr->t_infomask;
		htup->t_infomask2 = xlhdr->t_infomask2;
		htup->t_hoff = xlhdr->t_hoff;

		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;

		/*Get which relation it was belonged to*/
		reloid = getRelationOidByRelfileid(rnode.relNode);
		if(0 == reloid)
		{
			rrctl.reloid = 0;
			rrctl.nomalrel = true;
			getInsertSQL(sql_simple,tuple_info,&relname, schname, rrctl.sysrel);
		/*	ereport(NOTICE,(errmsg("Relfilenode %d can not be handled",rnode.relNode)));*/
			return;
		}
		rrctl.reloid = reloid;
		if( -1 == getRelationNameByOid(reloid, &relname))
			return;
		schname = getnsNameByReloid(reloid);
		rrctl.sysrel = tableIfSysclass(relname.data,reloid);
		rrctl.nomalrel = (!rrctl.sysrel) && (!tableIftoastrel(reloid));
		rrctl.imprel = tableifImpSysclass(relname.data,reloid);
		rrctl.tbsoid = rnode.spcNode;
		rrctl.recordxid = XLogRecGetXid(record);
		if(rrctl.tupdesc)
		{
			freetupdesc(rrctl.tupdesc);
			rrctl.tupdesc = NULL;
		}
		rrctl.tupdesc = GetDescrByreloid(reloid);
		
		if(NULL != htup)
		{
			rrctl.sqlkind = PG_LOGMINER_SQLKIND_INSERT;
			tupledata.t_data = htup;
			mentalTup(&tupledata, rrctl.tupdesc, &rrctl.tupinfo, false);
			if(!rrctl.tupinfo.sqlStr)
				rrctl.prostatu = LOGMINER_PROSTATUE_INSERT_MISSING_TUPLEINFO;
			rrctl.sqlkind = 0;
			tuple_info = rrctl.tupinfo.sqlStr;
			getInsertSQL(sql_simple,tuple_info,&relname, schname, rrctl.sysrel);
			if(rrctl.nomalrel && elemNameFind(relname.data)  && 0 == rrctl.prostatu)
			{
				/*Get undo sql*/
				getDeleteSQL(&srctl.sql_undo,tuple_info,&relname, schname, rrctl.sysrel, true);
				/*
				*Format delete sql
				*"where values(1,2,3);"-->"where i = 1 AND j = 2 AND k = 3;"
				*/
				reAssembleDeleteSql(&srctl.sql_undo, true);
				srctl.sqlindex++;
				if(srctl.sqlindex >= xlrec->ntuples)
					srctl.mutinsert = false;
				else
					srctl.mutinsert = true;
			}
			
		}
	}
}

/*find next xlog record and store into rrctl.xlogreader_state*/
static bool
getNextRecord()
{
	XLogRecord *record_t;	
	record_t = XLogReadRecord_logminer(rrctl.xlogreader_state, rrctl.first_record, &rrctl.errormsg);
	rrctl.first_record = InvalidXLogRecPtr;
	if (!record_t)
	{
		return false;
	}
	return true;
}


static int
getsqlkind(char *sqlheader)
{
	int loop,result = -1;
	for(loop = 0 ;loop < PG_LOGMINER_SQLKIND_MAXNUM; loop++)
	{
		if(0 == strcmp(sqlheader,sqlkind[loop].sqlhead))
			result = sqlkind[loop].sqlid;
	}
	return result;
}


/*Parser all kinds of insert sql, and make decide what sql it will form*/
static bool
parserInsertSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt)
{
	char tarTable[NAMEDATALEN] = {0};
	
	

	getPhrases(sql_ori->sqlStr, LOGMINER_INSERT_TABLE_NAME, tarTable, 0);

	/*It just insert to a user's table*/
	if(rrctl.nomalrel && (elemNameFind(tarTable) || 0 == strcmp("NULL",tarTable)))
	{
		appendtoSQL(sql_opt,sql_ori->sqlStr,PG_LOGMINER_SQLPARA_SIMSTEP);
		/*Here reached,it is not in toat,so try to free tthead*/
		freeToastTupleHead();
		return true;
	}
	return false;
}

/*Parser all kinds of delete sql, and make decide what sql it will form*/
static bool
parserDeleteSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt)
{

	char tarTable[NAMEDATALEN] = {0};
	
		

	getPhrases(sql_ori->sqlStr, LOGMINER_DELETE_TABLE_NAME, tarTable, 0);
	if(rrctl.nomalrel && (elemNameFind(tarTable) || 0 == strcmp("NULL",tarTable)))
	{
		reAssembleDeleteSql(sql_ori, false);
		freeToastTupleHead();
		appendtoSQL(sql_opt,sql_ori->sqlStr,PG_LOGMINER_SQLPARA_SIMSTEP);
	}
	return true;
}

/*Parser all kinds of update sql, and make decide what sql it will form*/
static bool
parserUpdateSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt)
{
	char tarTable[NAMEDATALEN] = {0};
	
	

	getPhrases(sql_ori->sqlStr, LOGMINER_ATTRIBUTE_LOCATION_UPDATE_RELNAME, tarTable, 0);
	if(rrctl.nomalrel && (elemNameFind(tarTable) || 0 == strcmp("NULL",tarTable)))
	{
		reAssembleUpdateSql(sql_ori, false);
		freeToastTupleHead();
		if(sql_ori->sqlStr)
			appendtoSQL(sql_opt,sql_ori->sqlStr,PG_LOGMINER_SQLPARA_SIMSTEP);
	}
	return true;
}

static void
parserXactSql(XLogMinerSQL *sql_ori, XLogMinerSQL *sql_opt)
{
	appendtoSQL(sql_opt,sql_ori->sqlStr,PG_LOGMINER_SQLPARA_SIMSTEP);
}


static void
XLogMinerRecord_heap(XLogReaderState *record, XLogMinerSQL *sql_simple)
{
	uint8				info;
	
	
	
	
	
	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	info &= XLOG_HEAP_OPMASK;
	
	if (XLOG_HEAP_INSERT == info)
	{
		minerHeapInsert(record, sql_simple, info);
	}
	else if(XLOG_HEAP_DELETE == info)
	{
		minerHeapDelete(record, sql_simple, info);
	}
	else if (XLOG_HEAP_UPDATE == info)
	{
		minerHeapUpdate(record, sql_simple, info);
	}
	else if (XLOG_HEAP_HOT_UPDATE == info)
	{
		minerHeapUpdate(record, sql_simple, info);
	}
}

static void
XLogMinerRecord_heap2(XLogReaderState *record, XLogMinerSQL *sql_simple)
{
	uint8		info = XLogRecGetInfo(record) & XLOG_HEAP_OPMASK;

	if(XLOG_HEAP2_MULTI_INSERT == info)
	{
		minerHeap2MutiInsert(record, sql_simple, info);
	}

}

static void
XLogMinerRecord_dbase(XLogReaderState *record, XLogMinerSQL *sql_simple)
{
	uint8				info;
	

	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	if(XLOG_DBASE_CREATE == info)
	{
		minerDbCreate(record, sql_simple, info);
	}
}

static void
XLogMinerRecord_xact(XLogReaderState *record, XLogMinerSQL *sql_simple, TimestampTz *xacttime)
{
	uint8						info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	xl_xact_parsed_commit		parsed_commit;
	char 						timebuf[MAXDATELEN + 1] = {0};
	TransactionId 				xid = 0;
	bool						commitxact = false;

	if(info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = NULL;
		memset(&parsed_commit, 0, sizeof(xl_xact_parsed_commit));
		xlrec = (xl_xact_commit *) XLogRecGetData(record);
		ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed_commit);
		if (!TransactionIdIsValid(parsed_commit.twophase_xid))
			xid = XLogRecGetXid(record);
		else
			xid = parsed_commit.twophase_xid;

		memcpy(timebuf,timestamptz_to_str(xlrec->xact_time),MAXDATELEN + 1);

		*xacttime = xlrec->xact_time;
		commitxact = true;
	}
	else if(info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = NULL;
		xl_xact_parsed_abort parsed_abort;

		memset(&parsed_abort, 0, sizeof(xl_xact_parsed_abort));
		xlrec = (xl_xact_abort *) XLogRecGetData(record);
		ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed_abort);
		if (!TransactionIdIsValid(parsed_abort.twophase_xid))
			xid = XLogRecGetXid(record);
		else
			xid = parsed_abort.twophase_xid;

		memcpy(timebuf,timestamptz_to_str(xlrec->xact_time),MAXDATELEN + 1);
		*xacttime = xlrec->xact_time;
	}
	
	processContrl(NULL,PG_LOGMINER_CONTRLKIND_XACT);
	if(curXactCheck(*xacttime, xid, commitxact, &parsed_commit))
	{
		xactCommitSQL(timebuf,sql_simple,info);
	}
}

static bool
XLogMinerRecord(XLogReaderState *record, XLogMinerSQL *sql_simple,TimestampTz *xacttime)
{
	uint8				info;
	bool				getxact = false;
	uint8				rmid = 0;

	rmid = XLogRecGetRmid(record);

	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	info &= XLOG_HEAP_OPMASK;

	/*if has not get valid record(input para) and it is not a xact record,parser nothing*/
	if(PG_LOGMINER_XLOG_NOMAL == rrctl.system_init_record && !rrctl.logprivate.staptr_reached && RM_XACT_ID != rmid)
		return false;

	if((RM_DBASE_ID == rmid || PG_LOGMINER_XLOG_DBINIT == rrctl.system_init_record)
		&& (0 == rrctl.sysstoplocation || PG_LOGMINER_FLAG_INITOVER == rrctl.sysstoplocation))
	{
		if(RM_DBASE_ID != rmid)
			return false;
		/*If we find 'postgresql' or 'highgo' db create cade,than it may will reach valid data after a xact commit*/
		XLogMinerRecord_dbase(record, sql_simple);
	}
	else if(RM_XACT_ID == rmid || PG_LOGMINER_FLAG_INITOVER != rrctl.sysstoplocation)
	{
		if(RM_XACT_ID != rmid)
			return false;
		/*
		if first time reach here,then we just find 'postgresql' or 'highgo' db create code,than it may be will reach valid data
		*/
		XLogMinerRecord_xact(record, sql_simple, xacttime);
		getxact = true;
	}
	else if(RM_HEAP_ID == rmid)
	{
		XLogMinerRecord_heap(record, sql_simple);
	}
	else if(RM_HEAP2_ID == rmid)
	{
		XLogMinerRecord_heap2(record, sql_simple);
	}
	
	return getxact;
}


static bool
sqlParser(XLogReaderState *record, TimestampTz 	*xacttime)
{
	char command_sql[NAMEDATALEN] = {0};
	
	XLogMinerSQL	sql_reass;
	XLogMinerSQL	sql;
	int				sskind = 0;
	
	bool			getxact = false;
	bool			getsql = false;
	Oid				server_dboid = 0;
	
	memset(&sql_reass, 0, sizeof(XLogMinerSQL));
	memset(&sql, 0, sizeof(XLogMinerSQL));

	/*parsert data that stored in a record to a simple sql */
	getxact = XLogMinerRecord(record, &sql, xacttime);


	/*avoid to parse the initdb  record*/
	if(rrctl.system_init_record != PG_LOGMINER_XLOG_NOMAL)
		return false;

	getPhrases(sql.sqlStr, LOGMINER_SQL_COMMAND, command_sql, 0);
	sskind = getsqlkind(command_sql);

	if(0 == rrctl.prostatu)
	{
		/*get a sql nomally*/
		/*Deal with every simple sql*/
		switch(sskind)
		{
			case PG_LOGMINER_SQLKIND_INSERT:
				getsql = parserInsertSql(&sql, &sql_reass);
				break;
			case PG_LOGMINER_SQLKIND_UPDATE:
				getsql = parserUpdateSql(&sql, &sql_reass);
				break;
			case PG_LOGMINER_SQLKIND_DELETE:
				getsql = parserDeleteSql(&sql, &sql_reass);
				break;
			case PG_LOGMINER_SQLKIND_XACT:
				parserXactSql(&sql, &sql_reass);
				break;
			default:
				break;
		}
	}
	else
	{
		/*Get a unnomal sql,and junk it*/
		rrctl.prostatu = 0;
	}
	
	if(!isEmptStr(sql_reass.sqlStr) && !getxact)
	{
		/*Now, we get a SQL, it need to be store tempory.*/
		char	*record_schema = NULL;
		char	*record_database = NULL;
		char	*record_user = NULL;
		char	*record_tablespace = NULL;
		Oid		useroid = 0, schemaoid = 0;
		
		
		if(getsql)
		{
			/*It is a simple sql,DML*/
			server_dboid = getDataDicOid();
			record_database = getdbNameByoid(server_dboid, false);
			record_tablespace = gettbsNameByoid(rrctl.tbsoid);
			if(0 != rrctl.reloid)
			{
				useroid = gettuserOidByReloid(rrctl.reloid);
				if(0 != useroid)
					record_user = getuserNameByUseroid(useroid);

				schemaoid = getnsoidByReloid(rrctl.reloid);
				if(0 != schemaoid)
					record_schema = getnsNameByOid(schemaoid);
			}
			padingminerXlogconts(record_schema, 0, Anum_xlogminer_contents_record_schema, schemaoid);
			padingminerXlogconts(record_user, 0, Anum_xlogminer_contents_record_user, useroid);
			padingminerXlogconts(record_database, 0, Anum_xlogminer_contents_record_database, server_dboid);
			padingminerXlogconts(record_tablespace, 0, Anum_xlogminer_contents_record_tablespace, rrctl.tbsoid);
			padingminerXlogconts(srctl.sql_undo.sqlStr, 0, Anum_xlogminer_contents_op_undo, -1);
		}
		else
		{
			/*It is a assemble sql,DDL*/
			server_dboid = getDataDicOid();
			record_database = getdbNameByoid(server_dboid,false);
			padingminerXlogconts(record_database, 0, Anum_xlogminer_contents_record_database, server_dboid);
		}
		getPhrases(sql_reass.sqlStr, LOGMINER_SQL_COMMAND, command_sql, 0);
		padingminerXlogconts(sql_reass.sqlStr, 0, Anum_xlogminer_contents_op_text, -1);
		padingminerXlogconts(command_sql, 0, Anum_xlogminer_contents_op_type, -1);
		padingminerXlogconts(NULL,rrctl.recordxid, Anum_xlogminer_contents_xid, -1);
		
		srctl.xcfcurnum++;
		padNullToXC();
		cleanAnalyseInfo();
	}
	cleanTuplemSpace(rrctl.tuplem);
	cleanTuplemSpace(rrctl.tuplem_old);
	if(rrctl.tuplem_bigold)
		pfree(rrctl.tuplem_bigold);
	rrctl.tuplem_bigold = NULL;
	rrctl.nomalrel = false;
	rrctl.imprel = false;
	rrctl.sysrel = false;
	rrctl.toastrel = false;
	freeSpace(&sql);
	freeSpace(&sql_reass);
	return getxact;
}

Datum
xlogminer_build_dictionary(PG_FUNCTION_ARGS)
{
	text	*dictionary = NULL;
	cleanSystableDictionary();
	checkLogminerUser();
	if(!PG_GETARG_DATUM(0))
		ereport(ERROR,(errmsg("Please enter a file path or directory.")));
	dictionary = PG_GETARG_TEXT_P(0);
	outputSysTableDictionary(text_to_cstring(dictionary), ImportantSysClass,false);
	PG_RETURN_TEXT_P(cstring_to_text("Dictionary build success!"));
}

Datum
xlogminer_load_dictionary(PG_FUNCTION_ARGS)
{
	text	*dictionary = NULL;
	cleanSystableDictionary();
	checkLogminerUser();
	if(!PG_GETARG_DATUM(0))
		ereport(ERROR,(errmsg("Please enter a file path or directory.")));
	dictionary = PG_GETARG_TEXT_P(0);
	if(DataDictionaryCache)
		ereport(ERROR,(errmsg("Dictionary has already been loaded.")));
	loadSystableDictionary(text_to_cstring(dictionary), ImportantSysClass, false);
	writeDicStorePath(text_to_cstring(dictionary));
	cleanSystableDictionary();
	PG_RETURN_TEXT_P(cstring_to_text("Dictionary load success!"));
}

Datum
xlogminer_stop(PG_FUNCTION_ARGS)
{
	checkLogminerUser();
	cleanSystableDictionary();
	cleanXlogfileList();
	dropAnalyseFile();
	PG_RETURN_TEXT_P(cstring_to_text("xlogminer stop!"));
}

Datum
xlogminer_xlogfile_add(PG_FUNCTION_ARGS)
{
	text	*xlogfile = NULL;
	char	backstr[100] = {0};
	int		addnum = 0;
	int		dicloadtype = 0;
	char	dic_path[MAXPGPATH] = {0};
	
	if(!PG_GETARG_DATUM(0))
		ereport(ERROR,(errmsg("Please enter a file path or directory.")));
	xlogfile = PG_GETARG_TEXT_P(0);
	cleanSystableDictionary();
	checkLogminerUser();
	loadXlogfileList();

	loadDicStorePath(dic_path);
	if(0 == dic_path[0])
	{
		dicloadtype = PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING;
	}
	else
	{
		loadSystableDictionary(dic_path, ImportantSysClass,true);
		dicloadtype = getDatadictionaryLoadType();
	}
	if(PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING == dicloadtype)
	{
		char *datadic = NULL;
		datadic = outputSysTableDictionary(NULL, ImportantSysClass, true);
		loadSystableDictionary(NULL, ImportantSysClass,true);
		writeDicStorePath(dictionary_path);
		ereport(NOTICE,(errmsg("Get data dictionary from current database.")));
	}
	addnum = addxlogfile(text_to_cstring(xlogfile));
	writeXlogfileList();
	cleanXlogfileList();
	cleanSystableDictionary();
	snprintf(backstr, 100, "%d file add success",addnum);
	PG_RETURN_TEXT_P(cstring_to_text(backstr));
}

Datum
xlogminer_xlogfile_remove(PG_FUNCTION_ARGS)
{
	text	*xlogfile = NULL;
	char	backstr[100] = {0};
	int		removenum = 0;
	int		dicloadtype = 0;
	char	dic_path[MAXPGPATH] = {0};

	cleanSystableDictionary();
	checkLogminerUser();

	if(!PG_GETARG_DATUM(0))
		ereport(ERROR,(errmsg("Please enter a file path or directory.")));
	xlogfile = PG_GETARG_TEXT_P(0);
	loadXlogfileList();

	loadDicStorePath(dic_path);
	if(0 == dic_path[0])
	{
		dicloadtype = PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING;
	}
	else
	{
		loadSystableDictionary(dic_path, ImportantSysClass,true);
		dicloadtype = getDatadictionaryLoadType();
	}
	
	if(PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING == getDatadictionaryLoadType())
		ereport(ERROR,(errmsg("DataDictionary has not been loaded.")));
	removenum = removexlogfile(text_to_cstring(xlogfile));
	writeXlogfileList();
	cleanXlogfileList();
	snprintf(backstr, 100, "%d file remove success",removenum);
	PG_RETURN_TEXT_P(cstring_to_text(backstr));
}

Datum
xlogminer_xlogfile_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = NULL;
	logminer_fctx	*temp_fctx = NULL;
	if (SRF_IS_FIRSTCALL())
	{
		logminer_fctx	*fctx = NULL;
		MemoryContext 	oldcontext = NULL;
		TupleDesc		tupdesc = NULL;
		cleanSystableDictionary();
		checkLogminerUser();
		loadXlogfileList();
		if(!is_xlogfilelist_exist())
			ereport(ERROR,(errmsg("Xlogfilelist has not been loaded or has been removed.")));
		fctx = (logminer_fctx *)logminer_palloc(sizeof(logminer_fctx),0);
		fctx->hasnextxlogfile= true;
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->user_fctx = fctx;
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		tupdesc = makeOutputXlogDesc();
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	temp_fctx = (logminer_fctx*)funcctx->user_fctx;
	while(temp_fctx->hasnextxlogfile)
	{
		HeapTuple	tuple;
		char		*values[1];
		char		*xlogfile = NULL;
		
		xlogfile = getNextXlogFile(funcctx->user_fctx, true);
		
		values[0] = xlogfile;
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	cleanXlogfileList();
	SRF_RETURN_DONE(funcctx);
}

/*
xlog analyse begin here
*/
Datum pg_minerXlog(PG_FUNCTION_ARGS)
{
	TimestampTz				xacttime = 0;
	bool					getrecord = true;
	bool					getxact = false;
	text					*starttimestamp = NULL;
	text					*endtimestamp = NULL;
	int32					startxid = 0;
	int32					endxid = 0;
	XLogSegNo				segno;
	char					*directory = NULL;
	char					*fname = NULL;
	char					*firstxlogfile = NULL;
	int						dicloadtype = 0;
	char					dictionary[MAXPGPATH] = {0};

	memset(&rrctl, 0, sizeof(RecordRecycleCtl));
	memset(&sysclass, 0, sizeof(SystemClass)*PG_LOGMINER_SYSCLASS_MAX);
	memset(&srctl, 0, sizeof(SQLRecycleCtl));
	sqlnoser = 0;
	cleanSystableDictionary();
	checkLogminerUser();
	logminer_createMemContext();
	rrctl.tuplem = getTuplemSpace(0);
	rrctl.tuplem_old = getTuplemSpace(0);
	rrctl.lfctx.sendFile = -1;

	if(!PG_GETARG_DATUM(0) || !PG_GETARG_DATUM(1))
		ereport(ERROR,(errmsg("The time parameter can not be null.")));
	starttimestamp = (text *)PG_GETARG_CSTRING(0);
	endtimestamp = (text *)PG_GETARG_CSTRING(1);
	startxid = PG_GETARG_INT32(2);
	endxid = PG_GETARG_INT32(3);
	if(0 > startxid || 0 > endxid)
		ereport(ERROR,(errmsg("The XID parameters cannot be negative.")));

	rrctl.logprivate.parser_start_xid = startxid;
	rrctl.logprivate.parser_end_xid = endxid;
	/*parameter check*/
	inputParaCheck(starttimestamp, endtimestamp);
		
	loadDicStorePath(dictionary);
	if(0 == dictionary[0])
		ereport(ERROR,(errmsg("Xlogfilelist must be loaded first.")));
	loadSystableDictionary(dictionary, ImportantSysClass,false);
	dicloadtype = getDatadictionaryLoadType();

	if(PG_LOGMINER_DICTIONARY_LOADTYPE_SELF == dicloadtype)
	{
		char *datadic = NULL;
		cleanSystableDictionary();
		datadic = outputSysTableDictionary(NULL, ImportantSysClass, true);
		loadSystableDictionary(NULL, ImportantSysClass,true);
		if(datadic)
			remove(datadic);
	}

	loadXlogfileList();
	if(!is_xlogfilelist_exist())
		ereport(ERROR,(errmsg("Xlogfilelist must be loaded first.")));
	checkXlogFileList();

	searchSysClass(sysclass,&sysclassNum);
	relkind_miner = getRelKindInfo();

	
	firstxlogfile = getNextXlogFile((char*)(&rrctl.lfctx),false);
	rrctl.lfctx.xlogfileptr = NULL;
	split_path_fname(firstxlogfile, &directory, &fname);
	XLogFromFileName(fname, &rrctl.logprivate.timeline, &segno);
	if(fname)
		logminer_pfree(fname,0);
	if(directory)
		logminer_pfree(directory,0);

	/* if this wal file include catalog relation info*/
	if(1 == segno)
		rrctl.system_init_record = PG_LOGMINER_XLOG_DBINIT;
	else
	{
		rrctl.system_init_record = PG_LOGMINER_XLOG_NOMAL;
		rrctl.sysstoplocation = PG_LOGMINER_FLAG_INITOVER;
	}
	
	/*configure call back func*/
	rrctl.xlogreader_state = XLogReaderAllocate(XLogMinerReadPage, &rrctl.logprivate);
	XLogSegNoOffsetToRecPtr(segno, 0, rrctl.logprivate.startptr);
	if(!rrctl.xlogreader_state)
		ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("Out of memory")));
	rrctl.first_record = XLogFindFirstRecord(rrctl.xlogreader_state, rrctl.logprivate.startptr);
	while(!rrctl.logprivate.endptr_reached)
	{
		/*if in a mutiinsert now, avoid to get new record*/
		if(!srctl.mutinsert)
			getrecord = getNextRecord();
		if(getrecord)
			getxact = sqlParser(rrctl.xlogreader_state, &xacttime);
		else if(!rrctl.logprivate.serialwal)
		{
			rrctl.logprivate.serialwal = true;
			rrctl.logprivate.changewal = false;
			rrctl.first_record = XLogFindFirstRecord(rrctl.xlogreader_state, rrctl.logprivate.startptr);
		}
	}
	cleanSystableDictionary();
	cleanXlogfileList();
	dropAnalyseFile();
	freeSQLspace();
	freeSpace(&srctl.sql_simple);
	freeSpace(&srctl.sql_undo);
	XLogReaderFree(rrctl.xlogreader_state);
	pfree(rrctl.tuplem);
	pfree(rrctl.tuplem_old);
	logminer_switchMemContext();
	PG_RETURN_TEXT_P(cstring_to_text("xlogminer start!"));
}
