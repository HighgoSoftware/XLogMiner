/*
 * Abstract:
 * Some tool function for Xlogminer
 *
 * Authored by lichuancheng@highgo.com ,20170524
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Identification: 
 * logminer.c
 */
#include "logminer.h"
#include "datadictionary.h"
#include "catalog/pg_type.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "access/tupdesc.h"
#include "access/attnum.h"
#include "access/xlog_internal.h"
#include "xlogminer_contents.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "access/tuptoaster.h"



RelationKind relkind[]={
	{LOGMINER_SQLGET_DDL_CREATE_TABLE,LOGMINER_RELKINDID_TABLE,"TABLE",'r',true},
	{LOGMINER_SQLGET_DDL_CREATE_INDEX,LOGMINER_RELKINDID_INDEX,"INDEX",'i',true},
	{LOGMINER_SQLGET_DDL_CREATE_SEQUENCE,LOGMINER_RELKINDID_SEQUENCE,"SEQUENCE",'s',true},
	{LOGMINER_SQLGET_DDL_CREATE_VIEW,LOGMINER_RELKINDID_VIEW,"VIEW",'v',true},
	{LOGMINER_SQLGET_DDL_CREATE_TOAST,LOGMINER_RELKINDID_TOAST,"TOAST",'t',false},
	{LOGMINER_SQLGET_DDL_CREATE_COMPLEX,LOGMINER_RELKINDID_COMPLEX,"COMPLEX",'c',true},
	{LOGMINER_SQLGET_DDL_CREATE_TABLE,LOGMINER_RELKINDID_TABLE,"TABLE",'P',true}
};
static bool separateJudge(char ch ,bool ignoresbrack);
static bool passOver(char **sql, bool isspace,bool ignoresbrack);
static int countCharInString(char* str, char ch);
char* addSinglequoteFromStr(char* strPara);


char* logminer_palloc(int size,int checkflag)
{
	void *result = NULL;
	result = (char*)palloc0(size);
	return result;
}

void logminer_pfree(char* ptr,int checkflag)
{
	if(ptr)
		pfree(ptr);
}

char* logminer_malloc(int size,int checkflag)
{
	char *result = NULL;	
	result = (char*)malloc(size);
	memset(result,0,size);
	return result;
}

void logminer_free(char* ptr,int checkflag)
{
	if(ptr)
	{
		free(ptr);
	}
}



static bool
separateJudge(char ch ,bool ignoresbrack)
{
	if(!ignoresbrack)
	{
		if(PG_LOGMINER_SPACE == ch
		 || PG_LOGMINER_SBRACK_L == ch
		 || PG_LOGMINER_SBRACK_R == ch
		 || PG_LOGMINER_COMMA == ch
			)
			return true;
	}
	else
	{
		if(PG_LOGMINER_SPACE == ch
		 || PG_LOGMINER_COMMA == ch
			)
			return true;
	}
	return false;
}

static bool
passOver(char **sql, bool isspace,bool ignoresbrack)
{
	int		length = 0;
	int		recCount = 1;
	int		entflagnum = 0;
	if(NULL == *sql)
		return false;
	length = strlen(*sql);
	if(isspace)
	{
		if(!separateJudge(**sql,ignoresbrack))
			return true;
		while(separateJudge(**sql,ignoresbrack))
		{
			(*sql)++;
			recCount++;
			if(recCount > length)
				return false;
		}
	}
	else
	{
		if(separateJudge(**sql,ignoresbrack))
			return true;
		if(bbl_Judge(**sql))
		{
			entflagnum++;
			while(0 < entflagnum)
			{
				(*sql)++;
				if(bbl_Judge(**sql))
					entflagnum++;
				else if(bbr_Judge(**sql))
					entflagnum--;

				recCount++;
				if(recCount > length)
					return false;
			}
			(*sql)++;
		}
		else if(sbl_Judge(**sql))
		{
			entflagnum++;
			while(0 < entflagnum)
			{
				(*sql)++;
				if(sbl_Judge(**sql))
					entflagnum++;
				else if(sbr_Judge(**sql))
					entflagnum--;

				recCount++;
				if(recCount > length)
					return false;
			}
			(*sql)++;
		}
		else
		{
			while(!separateJudge(**sql,ignoresbrack))
			{
				(*sql)++;
				recCount++;
				if(recCount > length)
					return false;
			}
		}
	}
	return true;
}

void
fixPathEnd(char *path)
{
	int pathLength = 0;
	pathLength = strlen(path);
	if('/' == path[pathLength - 1])
		path[pathLength - 1] = 0;
}

bool
isEmptStr(char *str)
{
	bool	result = false;
	char 	*strptr;
	if(!str)
	{
		result = true;
		return result;
	}
	strptr = str;
	
	passOver(&strptr,true,false);
	if(0 == strcmp("NULL",strptr))
		result = true;
	else if(0 == strcmp("null",strptr))
		result = true;
	else if(0 == strcmp("\"\"",strptr))
		result = true;
	else if(0 == strcmp("",strptr))
		result = true;
	return result;
}

bool
getPhrases(char *sql,int loc, char *term, int ignoresbrackph)
{
	char *sql_ptr = NULL;
	char *ptr_term_start = NULL;
	char *ptr_term_end = NULL;
	bool result = true; 
	int	 termLength = 0;
	int loop;
	sql_ptr = sql;
	if(!passOver(&sql_ptr,true,false))
			return false;
	for(loop = 1; loop < loc; loop++)
	{
		if(!passOver(&sql_ptr,false,ignoresbrackph == loop))
			return false;
		if(!passOver(&sql_ptr,true,(ignoresbrackph == loop + 1)))
			return false;
	}
	ptr_term_start = sql_ptr;
	passOver(&sql_ptr,false,ignoresbrackph == loop);
	ptr_term_end = sql_ptr;
	termLength = ptr_term_end - ptr_term_start;

	memset(term, 0, NAMEDATALEN);
	memcpy(term,ptr_term_start,termLength);

	return result;
}

void 
addSpace(XLogMinerSQL *sql_simple, int spaceKind)
{
	char *temp = NULL;
	int	 addstep = 0;
	
	if(!sql_simple)
		return;

	if(PG_LOGMINER_SQLPARA_TOTLE == spaceKind)
		addstep = PG_LOGMINER_SQLPARA_TOTSTEP;
	else
		addstep = PG_LOGMINER_SQLPARA_SIMSTEP;

	temp = logminer_palloc(sql_simple->tot_size + addstep,0);
	memset(temp,0,sql_simple->tot_size + addstep);
	if(sql_simple->sqlStr)
	{
		memcpy(temp,sql_simple->sqlStr,sql_simple->tot_size);
		logminer_pfree(sql_simple->sqlStr,0);
		sql_simple->sqlStr = NULL;
	}
	sql_simple->tot_size += addstep;
	sql_simple->rem_size += addstep;
	sql_simple->sqlStr = temp;
	
}

void
cleanSpace(XLogMinerSQL *minersql)
{
	if(!minersql)
		return;
	if(!minersql->sqlStr)
		return;
	memset(minersql->sqlStr, 0, minersql->tot_size);
	minersql->rem_size = minersql->tot_size;
	minersql->use_size = 0;
}

void
freeSpace(XLogMinerSQL *minersql)
{
	if(!minersql)
		return;
	if(!minersql->sqlStr)
		return;

	logminer_pfree(minersql->sqlStr,0);
	memset(minersql , 0, sizeof(XLogMinerSQL));
}



void
cleanMentalvalues()
{
	if(rrctl.values)
			logminer_pfree((char *)rrctl.values,0);
	if(rrctl.nulls)
			logminer_pfree((char *)rrctl.nulls,0);
	if(rrctl.values_old)
			logminer_pfree((char *)rrctl.values_old,0);
	if(rrctl.nulls_old)
			logminer_pfree((char *)rrctl.nulls_old,0);
	rrctl.values = NULL;
	rrctl.nulls = NULL;
	rrctl.values_old = NULL;
	rrctl.nulls_old = NULL;
}

bool
elemNameFind(char* elenname)
{
	if(!elenname)
		return false;
	if(isEmptStr(elenname))
		return false;
	return true;
}

void
split_path_fname(const char *path, char **dir, char **fname)
{
	char	   *sep = NULL;
	int			length_dir = 0;
	int			length_fname = 0;
	

	/* split filepath into directory & filename */
#ifdef WIN32
	sep = strrchr(path, '\\');
	if(NULL == sep)
		sep = strrchr(path, '/');
#else
	sep = strrchr(path, '/');
#endif

	/* directory path */
	if (sep != NULL)
	{	
		length_dir = sep - path;
		length_fname = strlen(sep + 1);

		*dir = logminer_palloc(length_dir + 1,0);
		memcpy(*dir, path, length_dir);
		*fname = logminer_palloc(length_fname + 1,0);
		memcpy(*fname, sep + 1, length_fname);

	}
	/* local directory */
	else
	{
		length_dir = strlen(path);
		*fname = logminer_palloc(length_dir + 1,0);
		*dir = NULL;
		memcpy(*fname, path, length_dir);
	}
}

RelationKind*
getRelKindInfo()
{
	return relkind;
}


int
xlog_file_open(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	if (directory == NULL)
	{
		const char *datadir;

		/* fname */
		fd = open(fname, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		/* XLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 XLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		datadir = DataDir;
		/* $PGDATA / XLOGDIR / fname */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s/%s",
					 datadir, XLOGDIR, fname);
			fd = open(fpath, O_RDONLY | PG_BINARY, 0);
			if (fd < 0 && errno != ENOENT)
				return -1;
			else if (fd >= 0)
				return fd;
		}
	}
	else
	{
		/* directory / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s",
				 directory, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;

		/* directory / XLOGDIR / fname */
		snprintf(fpath, MAXPGPATH, "%s/%s/%s",
				 directory, XLOGDIR, fname);
		fd = open(fpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0 && errno != ENOENT)
			return -1;
		else if (fd >= 0)
			return fd;
	}
	return -1;
}

TupleDesc
makeOutputXlogDesc()
{
	TupleDesc tupdesc = NULL;
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "path", TEXTOID, -1, 0);
	return tupdesc;
}

int
strcmp_withlength(char *str1,char *str2,int length)
{
	int loop = 0;
	for(loop = 0; loop < length; loop++)
	{
		if(str1[loop] != str2[loop])
		{
			return -1;
		}
	}
	return 0;
}

bool
inputParaCheck(text *st, text *et)
{
	char *starttimestamp = NULL, *endtimestamp = NULL;

	if(!st || !et)
		return false;

	starttimestamp = text_to_cstring(st);
	endtimestamp = text_to_cstring(et);

	/*get begin timestamp and end timestamp. if it din not been limited, will be 0*/
	if(isEmptStr(starttimestamp))
		rrctl.logprivate.parser_start_time = 0;
	else
		rrctl.logprivate.parser_start_time = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
													CStringGetDatum(starttimestamp),
													ObjectIdGetDatum(InvalidOid),
															Int32GetDatum(-1)));
	if(isEmptStr(endtimestamp))
		rrctl.logprivate.parser_end_time = 0;
	else
		rrctl.logprivate.parser_end_time = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
													CStringGetDatum(endtimestamp),
													ObjectIdGetDatum(InvalidOid),
															Int32GetDatum(-1)));

	/*there must  be less than one group valid.*/
	if((0 != rrctl.logprivate.parser_start_time || 0 != rrctl.logprivate.parser_end_time)
		&&(0 != rrctl.logprivate.parser_start_xid || 0 != rrctl.logprivate.parser_start_xid))
		ereport(ERROR,(errmsg("Time parameters and XID parameters cannot be provided at the same time.")));
	
	if(0 != rrctl.logprivate.parser_end_time && rrctl.logprivate.parser_start_time > rrctl.logprivate.parser_end_time)
		ereport(ERROR,(errmsg("Start time is greater than end time.")));
	if(0 != rrctl.logprivate.parser_end_xid && rrctl.logprivate.parser_start_xid > rrctl.logprivate.parser_end_xid)
		ereport(ERROR,(errmsg("Start xid is greater than end xid.")));

	if(0 != rrctl.logprivate.parser_start_time || 0 != rrctl.logprivate.parser_end_time)
		rrctl.logprivate.timecheck = true;
	if(0 != rrctl.logprivate.parser_start_xid || 0 != rrctl.logprivate.parser_end_xid)
		rrctl.logprivate.xidcheck = true;

	if(rrctl.logprivate.timecheck || rrctl.logprivate.xidcheck)
		rrctl.logprivate.staptr_reached = false;
	else
		rrctl.logprivate.staptr_reached = true;
	rrctl.logprivate.endptr_reached = false;
	rrctl.logprivate.timeline = 1;
	rrctl.logprivate.startptr = InvalidXLogRecPtr;
	rrctl.logprivate.analynum = getXlogFileNum();
	return true;
}

/*
When it occurs a commit,we must check if we have some sql to insert into temp table.
*/
bool
curXactCheck(TimestampTz xact_time ,TransactionId xid, bool xactcommit,xl_xact_parsed_commit *parsed_commit)
{
	bool	result = true;
	FormData_xlogminer_contents fxc;
	XlogminerContentsFirst		*xcf = NULL;

	memset(&fxc,0,sizeof(FormData_xlogminer_contents));

	if(rrctl.logprivate.timecheck)
	{
		if(0 != rrctl.logprivate.parser_start_time && xact_time < rrctl.logprivate.parser_start_time)
			result = false;
		if(0 != rrctl.logprivate.parser_end_time && xact_time > rrctl.logprivate.parser_end_time)
		{
			rrctl.logprivate.endptr_reached = true;
			result = false;
		}
	}
	if((rrctl.logprivate.xidcheck))
	{
		if(0 != rrctl.logprivate.parser_start_xid && xid < rrctl.logprivate.parser_start_xid)
			result = false;
		if(0 != rrctl.logprivate.parser_end_xid && xid > rrctl.logprivate.parser_end_xid)

		{
			rrctl.logprivate.endptr_reached = true;
			result = false;
		}
	}

	/*
		result is false,means that it did not reach valid record(input limit).
		then store the point for a while,just in case,valid next record.
	*/
	if(!result)
	{
		rrctl.logprivate.limitstartptr = rrctl.xlogreader_state->ReadRecPtr;
		rrctl.logprivate.limitendptr = rrctl.xlogreader_state->EndRecPtr;
	}
	
	if(result && !rrctl.logprivate.staptr_reached)
	{
		/*first reached the valid(input valid) xlog tuple*/
		rrctl.xlogreader_state->ReadRecPtr = rrctl.logprivate.limitstartptr;
		rrctl.xlogreader_state->EndRecPtr = rrctl.logprivate.limitendptr;
		rrctl.logprivate.staptr_reached = true;

	}
	else if(!xactcommit)
	{
		/*abord xact,do not show the info,then clean the space*/
		cleanSQLspace();
	}
	else if(result)
	{
		/*reach the nomal valid(input limit) xlog tuple*/
		int	 loop = 0;
		
		/*fxc.xid = xid;*/
		fxc.timestamp = xact_time;
		fxc.record_database = NULL;
		fxc.record_schema = NULL;
		fxc.record_tablespace = NULL;
		fxc.record_user = NULL;
		for(loop = 0; loop < srctl.xcfcurnum; loop++)
		{
			xcf = (XlogminerContentsFirst *)srctl.xcf;
			if(0 == xcf[loop].xid)
				fxc.xid = xid;
			else if(xid != xcf[loop].xid)
				continue;
			
			fxc.op_text = xcf[loop].op_text.sqlStr;
			fxc.op_type = xcf[loop].op_type.sqlStr;
			fxc.op_undo = xcf[loop].op_undo.sqlStr;
			fxc.virtualxid = loop + 1;
			fxc.sqlno = ++sqlnoser;
			fxc.record_database = xcf[loop].record_database.sqlStr;
			fxc.record_user = xcf[loop].record_user.sqlStr;
			fxc.record_tablespace = xcf[loop].record_tablespace.sqlStr;
			fxc.record_schema = xcf[loop].record_schema.sqlStr;
			fxc.xid = xid;

			
			InsertXlogContentsTuple(&fxc);
		}
		cleanSQLspace();
	}
	return result;
}

void
logminer_createMemContext()
{
	rrctl.mycxt = AllocSetContextCreate(CurrentMemoryContext,
									  "logminer_main",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	rrctl.oldcxt= MemoryContextSwitchTo(rrctl.mycxt);
}

void
logminer_switchMemContext()
{
	if(!rrctl.mycxt || !rrctl.oldcxt)
		return;
	MemoryContextSwitchTo(rrctl.oldcxt);
	MemoryContextDelete(rrctl.mycxt);
}

bool
checkLogminerUser()
{
	bool	result = false;	
	result = superuser();
	if(!result)
		ereport(ERROR,(errmsg("Only the superuser execute xlogminer.")));
	return result;
}

bool
padingminerXlogconts(char* elemname, TransactionId xid,int loc, long elemoid)
{
	XlogminerContentsFirst		*xcf = NULL;
	XLogMinerSQL				*temp_sql = NULL;
	char						tempName[NAMEDATALEN] = {0};
	char						*appPter = NULL;

	if(srctl.xcfcurnum == srctl.xcftotnum)
			addSQLspace();
		
	xcf = (XlogminerContentsFirst*)srctl.xcf;

	if(!elemname && -1 != elemoid)
	{
		if(0 < elemoid)
		{
			sprintf(tempName,"(%ld)",elemoid);
		}
		else
		{
			sprintf(tempName,"(missing data)");
		}
		appPter = tempName;
	}
	else
		appPter = elemname;

	
	
	if(Anum_xlogminer_contents_xid == loc)
	{
		xcf[srctl.xcfcurnum].xid = xid;
	}
	else if(Anum_xlogminer_contents_record_database == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].record_database;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_record_user == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].record_user;
		cleanSpace(temp_sql);
		
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_record_tablespace == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].record_tablespace;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_record_schema == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].record_schema;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_op_type == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].op_type;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_op_text == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].op_text;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}
	else if(Anum_xlogminer_contents_op_undo == loc)
	{
		temp_sql = &xcf[srctl.xcfcurnum].op_undo;
		cleanSpace(temp_sql);
		appendtoSQL(temp_sql, appPter, PG_LOGMINER_SQLPARA_OTHER);
	}

	return true;
}


void
padNullToXC()
{
	/*for padding null while did not get any info*/
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_xid, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_record_database, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_record_user,-1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_record_tablespace, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_record_schema, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_op_type, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_op_text, -1);
	padingminerXlogconts(NULL, 0, Anum_xlogminer_contents_op_undo, -1);
}


void
cleanAnalyseInfo()
{
	rrctl.reloid = 0;
	rrctl.tbsoid = 0;
	rrctl.recordxid = 0;
	memset(&srctl.rfnode, 0, sizeof(RelFileNode));
	srctl.toastoid = 0;
	cleanSpace(&srctl.sql_undo);
}

char*
getTuplemSpace(int addsize)
{
	int 		size = 0;
	char*		result = NULL;
	if(0 == addsize)
		size = MaxHeapTupleSize + SizeofHeapTupleHeader;
	else
		size = addsize;
	result = logminer_palloc(size, 0);
	if(NULL == result)
		ereport(ERROR,(errmsg("Out of memory.")));
	return result;
}


void
cleanTuplemSpace(char* tuplem)
{
	int			size = 0;
	size = MaxHeapTupleSize + SizeofHeapTupleHeader;
	memset(tuplem,0,size);
}

bool
ifquoneed(Form_pg_attribute attrs)
{
	Oid	quoNeedArray[] = {BPCHAROID,VARCHAROID,TEXTOID,BYTEAOID,BOXOID,CASHOID,TSVECTOROID
	,TSQUERYOID,TIMESTAMPOID,TIMESTAMPTZOID,TIMEOID,TIMETZOID,DATEOID,INTERVALOID,BOOLOID
	,BYTEAOID,POINTOID,CIRCLEOID,CIDROID,INETOID,MACADDROID,BITOID,VARBITOID,UUIDOID,XMLOID
	,JSONBOID,3908,3910,LSEGOID,PATHOID,NAMEOID,POLYGONOID,JSONOID,LINEOID,-1};
	int	arrNum = 35;
	int loop = 0;

	if(0 < attrs->attndims)
		return true;
	
	for(loop = 0; loop < arrNum; loop++)
	{
		if(attrs->atttypid == quoNeedArray[loop])
			return true;
	}
	return false;

}

bool
ifQueNeedDelete(Form_pg_attribute attrs)
{
	Oid quoNeedDeleteArray[] = {TSVECTOROID,TSQUERYOID};
	int arrNum = 2;
	int loop = 0;

	for(loop = 0; loop < arrNum; loop++)
	{
		if(attrs->atttypid == quoNeedDeleteArray[loop])
			return true;
	}

	return false;
}

ToastTuple*
makeToastTuple(int datalength,char* data, Oid id, int seq)
{
	char* ptr = NULL;
	ToastTuple*	result = NULL;
	ptr = logminer_palloc(datalength + sizeof(ToastTuple), 0);
	result = (ToastTuple*)ptr;
	result->chunk_data = ptr + sizeof(ToastTuple);

	result->chunk_id = id;
	result->chunk_seq = seq;
	result->datalength = datalength;
	result->next = NULL;
	memcpy(result->chunk_data, data, datalength);

	return result;
}

void
freeToastTupleHead()
{
	ToastTuple *ttptr = NULL, *ttnext = NULL;

	ttptr = rrctl.tthead;
	while(ttptr)
	{
		ttnext = ttptr->next;
		logminer_pfree((char*)ttptr,0);
		ttptr = ttnext;
	}
	rrctl.tthead = NULL;
}

void
toastTupleAddToList(ToastTuple *tt)
{
	ToastTuple	*ttptr = NULL;
	
	if(!rrctl.tthead)
	{
		rrctl.tthead = tt;
	}
	else
	{
		ttptr = rrctl.tthead;
		while(ttptr->next)
			ttptr = ttptr->next;
		ttptr->next = tt;
	}
}

void 
checkVarlena(Datum attr,struct varlena** att_return)
{
	text *attr_text = NULL;
	ToastTuple *ttptr = NULL;
	struct varlena *attr_varlena = NULL;
	struct varlena *result;
	struct varatt_external toast_pointer;
	int32		ressize = 0;
	
	attr_text = (text*)DatumGetPointer(attr);
	attr_varlena = (struct varlena *)attr_text;

	if(!VARATT_IS_EXTERNAL_ONDISK(attr_varlena))
	{
		*att_return = (struct varlena *)attr;
		return;
	}

	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
	ressize = toast_pointer.va_extsize;
	result = (struct varlena *) palloc(ressize + VARHDRSZ);

	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		SET_VARSIZE_COMPRESSED(result, ressize + VARHDRSZ);
	else
		SET_VARSIZE(result, ressize + VARHDRSZ);

	ttptr = rrctl.tthead;
	while(ttptr)
	{
		if(ttptr->chunk_id == toast_pointer.va_valueid)
		{
  			memcpy(VARDATA(result) + ttptr->chunk_seq * TOAST_MAX_CHUNK_SIZE, ttptr->chunk_data, ttptr->datalength);
		}
		ttptr = ttptr->next;
	}
	if (VARATT_IS_COMPRESSED(result))
	{
		struct varlena *tmp = result;
		result = heap_tuple_untoast_attr(tmp);
		pfree(tmp);
	}
	*att_return = result;
}

text *
cstringToTextWithLen(const char *s, int len)
{
	text	   *result = (text *) palloc(len + VARHDRSZ);

	SET_VARSIZE(result, len + VARHDRSZ);
	memcpy(VARDATA(result), s, len);

	return result;
}

void
deleteQueFromStr(char* strPara)
{
	int 	strlength = 0,loopo = 0,loopt = 0;
	char*	strtemp = NULL;
	
	if(!strPara)
		return;
	strlength = strlen(strPara);
	strtemp = (char*)palloc0(strlength + 1);
	if(!strtemp)
		ereport(ERROR,(errmsg("Out of memory during deleteQueFromStr")));
	
	while(loopo != strlength)
	{
		if((('\'' == strPara[loopo]) && (0 == loopo))
			|| (('\'' == strPara[loopo]) && (strlength - 1 == loopo))
			|| (('\'' == strPara[loopo]) && (strlength - 1 != loopo && (0 != loopo)) && (' ' == strPara[loopo - 1] || ' ' == strPara[loopo + 1])))
		{
			loopo++;
			continue;
		}
		else
		{
			strtemp[loopt++] = strPara[loopo++];
		}
	}
	memset(strPara, 0, strlength);
	memcpy(strPara, strtemp, strlen(strtemp));
	pfree(strtemp);
}

void
keepDigitFromStr(char* strPara)
{
	int 	strlength = 0,loopo = 0,loopt = 0;
	char*	strtemp = NULL;
	
	if(!strPara)
		return;
	strlength = strlen(strPara);
	strtemp = (char*)palloc0(strlength + 1);
	if(!strtemp)
		ereport(ERROR,(errmsg("Out of memory during keepDigitFromStr")));
	
	while(loopo != strlength)
	{
		if(('0' <= strPara[loopo] && '9' >=  strPara[loopo]) || '.' == strPara[loopo])
		{
			strtemp[loopt++] = strPara[loopo++];
		}
		else
		{
			loopo++;
			continue;
		}
	}
	memset(strPara, 0, strlength);
	memcpy(strPara, strtemp, strlen(strtemp));
	pfree(strtemp);
}

static int
countCharInString(char* str, char ch)
{
	char	*strPtr = NULL;
	int 	result = 0;
	int 	strlength = 0;
	int 	loop = 0;

	if(!str)
		return result;

	strlength = strlen(str);
	strPtr = str;
	for(;loop < strlength;loop++)
	{
		if(*strPtr == ch)
			result++;
		strPtr++;
	}
	return result;
}


char*
addSinglequoteFromStr(char* strPara)
{
	int 	strlength = 0,loopo = 0,loopt = 0;
	char*	strtemp = NULL;
	int		simplequenum = 0;

	if(!strPara)
		return NULL;

	simplequenum = countCharInString(strPara,'\'');
	if(0 >= simplequenum)
		return strPara;
	
	strlength = strlen(strPara);
	strtemp = (char*)palloc0(strlength + simplequenum + 1);
	if(!strtemp)
		ereport(ERROR,(errmsg("Out of memory during addSinglequoteFromStr")));
	
	while(loopo != strlength)
	{
		if('\'' == strPara[loopo])
		{
			strtemp[loopt++] = '\'';
		}
		strtemp[loopt++] = strPara[loopo++];
	}
	return strtemp;
}


char*
convertAttrToStr(Form_pg_attribute fpa,Oid typoutput, Datum attr)
{
	char	*resultstr = NULL;
	resultstr = OidOutputFunctionCall(typoutput, attr);


	if(ifQueNeedDelete(fpa))
	{
		deleteQueFromStr(resultstr);
	}
	if(CASHOID == fpa->atttypid)
	{
		keepDigitFromStr(resultstr);
	}
	else if(JSONOID == fpa->atttypid || TEXTOID == fpa->atttypid || BPCHAROID == fpa->atttypid || VARCHAROID == fpa->atttypid
		|| XMLOID == fpa->atttypid || NAMEOID == fpa->atttypid || JSONBOID == fpa->atttypid || CHAROID == fpa->atttypid
		|| 199 == fpa->atttypid || TEXTARRAYOID == fpa->atttypid || 1014 == fpa->atttypid || 1015 == fpa->atttypid
		|| 143 == fpa->atttypid || 1003 == fpa->atttypid || 3807 == fpa->atttypid || 1002 == fpa->atttypid)
	{
		resultstr = addSinglequoteFromStr(resultstr);
	}
	return resultstr;
}	


char*
OutputToByte(text* attrpter, int attlen)
{

	int				len = 0;
	char			*attstr = NULL;
	bytea	   		*str_byte = NULL;
	char			*str = NULL;
	Datum			str_datum;
	XLogMinerSQL	result;

	memset(&result,0,sizeof(XLogMinerSQL));


	if(0 < attlen)
	{
		len = attlen;
		attstr = (char*)(&attrpter);
	}
	else if(-1 == attlen)
	{
		len = VARSIZE_ANY(attrpter);
		attstr = VARDATA_ANY(attrpter);
	}
	else if(-2 == attlen)
	{
		len = (strlen((char *) (attrpter)) + 1);
		attstr = (char *)attrpter;
	}

	if(-1 ==attlen)
	{
		str = DatumGetCString(DirectFunctionCall1(byteaout, (Datum)attrpter));
	}
	else
	{
		str_byte = cstringToTextWithLen(attstr, len);
		str_datum = PointerGetDatum(str_byte);
		str = DatumGetCString(DirectFunctionCall1(byteaout, str_datum));
	}
	appendtoSQL(&result, "encode(\'", PG_LOGMINER_SQLPARA_OTHER);
	appendtoSQL(&result, str, PG_LOGMINER_SQLPARA_OTHER);
	appendtoSQL(&result, "\', \'hex\')", PG_LOGMINER_SQLPARA_OTHER);
	return result.sqlStr;

}

bool
getTypeOutputFuncFromDb(Oid type, Oid *typOutput, bool *typIsVarlena)
{
	HeapTuple	typeTuple;
	Form_pg_type pt;

	typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(typeTuple))
		return false;
	pt = (Form_pg_type) GETSTRUCT(typeTuple);

	*typOutput = pt->typoutput;
	*typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);

	ReleaseSysCache(typeTuple);
	return true;
}
