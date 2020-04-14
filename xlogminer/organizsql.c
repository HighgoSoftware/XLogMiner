/*-------------------------------------------------------------------------
 *
 * Abstract:
 * Function to spell the SQL.
 *
 * Authored by lichuancheng@highgo.com ,20170524
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Identification:
 * organizsql.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "logminer.h"
#include "datadictionary.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands_xlog.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

void 
xactCommitSQL(char* timestr,XLogMinerSQL *sql_opt,uint8 info)
{
	if(XLOG_XACT_COMMIT == info)
	{
		appendtoSQL(sql_opt, "XACT COMMIT(time:", PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_opt, timestr, PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_opt, ")", PG_LOGMINER_SQLPARA_SIMPLE);
	}
	else if(XLOG_XACT_ABORT == info)
	{
		appendtoSQL(sql_opt, "XACT ABORT(time:", PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_opt, timestr, PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_opt, ")", PG_LOGMINER_SQLPARA_SIMPLE);
	}
	appendtoSQL(sql_opt, ";", PG_LOGMINER_SQLPARA_SIMPLE);
}


void
getInsertSQL(XLogMinerSQL *sql_simple, char *tupleInfo, NameData *relname, char* schname, bool sysrel)
{

	bool				nameFind = false;
	appendtoSQL(sql_simple, "INSERT INTO ", PG_LOGMINER_SQLPARA_SIMPLE);
	nameFind = (0 == relname->data[0])?false:true;

	if(nameFind)
	{
		if(sysrel)
			appendtoSQL(sql_simple, relname->data, PG_LOGMINER_SQLPARA_SIMPLE);
		else
		{
			appendtoSQL_doubquo(sql_simple, schname, true);
			appendtoSQL(sql_simple, ".", PG_LOGMINER_SQLPARA_SIMPLE);
			appendtoSQL_doubquo(sql_simple, relname->data, true);
		}
	}
	else
	{
		appendtoSQL(sql_simple, "(NULL)", PG_LOGMINER_SQLPARA_SIMPLE);
	}	

	if(tupleInfo)
	{
		if(sysrel)
			appendBlanktoSQL(sql_simple);
		appendtoSQL(sql_simple, tupleInfo, PG_LOGMINER_SQLPARA_SIMPLE);
	}
	else
	{
		rrctl.prostatu = LOGMINER_PROSTATUE_INSERT_MISSING_TUPLEINFO;
		appendtoSQL(sql_simple, " VALUES(NULL) (NOTICE:Miss table info.)", PG_LOGMINER_SQLPARA_SIMPLE);
	}

	
	appendtoSQL(sql_simple, ";", PG_LOGMINER_SQLPARA_SIMPLE);

}

void 
getDeleteSQL(XLogMinerSQL *sql_simple, char *tupleInfo, NameData *relname, char* schname, bool sysrel, bool undo)
{

	bool				nameFind = false;
	char				*diviloc = NULL;

	appendtoSQL(sql_simple, "DELETE FROM ", PG_LOGMINER_SQLPARA_SIMPLE);
	nameFind = (0 == relname->data[0])?false:true;
	if(nameFind)
	{
		if(sysrel)
			appendtoSQL(sql_simple, relname->data, PG_LOGMINER_SQLPARA_SIMPLE);
		else
		{
			appendtoSQL_doubquo(sql_simple, schname, true);
			appendtoSQL(sql_simple, ".", PG_LOGMINER_SQLPARA_SIMPLE);
			appendtoSQL_doubquo(sql_simple, relname->data, true);
		}
	}
	else
	{
		appendtoSQL(sql_simple, "(NULL)", PG_LOGMINER_SQLPARA_SIMPLE);
	}

	if(tupleInfo)
	{
		appendtoSQL(sql_simple, " WHERE ", PG_LOGMINER_SQLPARA_SIMPLE);
		if(sysrel)
			appendtoSQL(sql_simple, tupleInfo, PG_LOGMINER_SQLPARA_SIMPLE);
		else
		{
			diviloc = strstr(tupleInfo,") VALUES(") + 2;
			appendtoSQL(sql_simple, diviloc, PG_LOGMINER_SQLPARA_SIMPLE);
		}
	}
	else
	{
		rrctl.prostatu = LOGMINER_PROSTATUE_INSERT_MISSING_TUPLEINFO;
		appendtoSQL(sql_simple, " WHERE VALUES(NULL) (NOTICE:Miss table info.)", PG_LOGMINER_SQLPARA_SIMPLE);
	}
	
	appendtoSQL(sql_simple, ";", PG_LOGMINER_SQLPARA_SIMPLE);
}

void
getUpdateSQL(XLogMinerSQL *sql_simple, char *tupleInfo, char *tupleInfo_old,NameData *relname, char* schname, bool sysrel)
{
	bool				nameFind = false;
	nameFind = (0 == relname->data[0])?false:true;
	if(nameFind)
	{
		appendtoSQL(sql_simple, "UPDATE ", PG_LOGMINER_SQLPARA_SIMPLE);
		if(sysrel)
			appendtoSQL(sql_simple, relname->data, PG_LOGMINER_SQLPARA_SIMPLE);
		else
		{
			appendtoSQL_doubquo(sql_simple, schname, true);
			appendtoSQL(sql_simple, ".", PG_LOGMINER_SQLPARA_SIMPLE);
			appendtoSQL_doubquo(sql_simple, relname->data, true);
		}
		
	}
	else
	{
		appendtoSQL(sql_simple, "UPDATE ", PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_simple, "(NULL)", PG_LOGMINER_SQLPARA_SIMPLE);
	}

	if(tupleInfo)
	{
		appendtoSQL(sql_simple, " SET ", PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_simple, tupleInfo, PG_LOGMINER_SQLPARA_SIMPLE);
	}
	else
	{
		rrctl.prostatu = LOGMINER_PROSTATUE_UPDATE_MISSING_NEW_TUPLEINFO;
		appendtoSQL(sql_simple, " SET VALUES(NULL) (NOTICE:Miss table info.)", PG_LOGMINER_SQLPARA_SIMPLE);
	}

	if(tupleInfo_old)
	{
		appendtoSQL(sql_simple, " WHERE ", PG_LOGMINER_SQLPARA_SIMPLE);
		appendtoSQL(sql_simple, tupleInfo_old, PG_LOGMINER_SQLPARA_SIMPLE);
	}
	else
		rrctl.prostatu |= LOGMINER_PROSTATUE_UPDATE_MISSING_OLD_TUPLEINFO;
		
	appendtoSQL(sql_simple, ";", PG_LOGMINER_SQLPARA_SIMPLE);

}

void
minerDbCreate(XLogReaderState *record, XLogMinerSQL *sql_simple,uint8 info)
{
	xl_dbase_create_rec *xlrec = NULL;
	char				*dbname = NULL;

	xlrec = (xl_dbase_create_rec *) XLogRecGetData(record);
	dbname = getdbNameByoid(xlrec->db_id, true);
	processContrl(dbname,PG_LOGMINER_CONTRLKIND_FIND);
}

void
mentalTup_nulldata(int natts, int index, XLogMinerSQL *values_sql,XLogMinerSQL *att_sql, bool valueappend, bool attdroped, bool *firstattget)
{
	if(1 == natts)
	{
		if(valueappend)
		{
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
		{
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
				appendtoSQL(values_sql, PG_LOGMINER_RELNULL, PG_LOGMINER_SQLPARA_OTHER);
			else
				appendtoSQL(values_sql, "encode(\'\',\'hex\')", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
			*firstattget = true;
			
		}
	}
	else if(0 == index)
	{
		if(valueappend)
		{
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
		{
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
				appendtoSQL(values_sql, PG_LOGMINER_RELNULL, PG_LOGMINER_SQLPARA_OTHER);
			else
				appendtoSQL(values_sql, "encode(\'\',\'hex\')", PG_LOGMINER_SQLPARA_OTHER);
			*firstattget = true;
		/*	appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
		}
	}
	else if(natts - 1 == index)
	{
		if(valueappend)
		{
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
		{
			if(*firstattget)
				appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
			else
				*firstattget = true;
			if(!attdroped)
				appendtoSQL(values_sql, PG_LOGMINER_RELNULL, PG_LOGMINER_SQLPARA_OTHER);
			else
				appendtoSQL(values_sql, "encode(\'\',\'hex\')", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
	}
	else
	{
		if(valueappend)
		{
		}
		else
		{
			if(*firstattget)
				appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
			else
				*firstattget = true;
			if(!attdroped)
				appendtoSQL(values_sql, PG_LOGMINER_RELNULL, PG_LOGMINER_SQLPARA_OTHER);
			else
				appendtoSQL(values_sql, "encode(\'\',\'hex\')", PG_LOGMINER_SQLPARA_OTHER);
			/*appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
		}
	}


}

void
mentalTup_valuedata(int natts, int index, XLogMinerSQL *values_sql,XLogMinerSQL *att_sql, bool valueappend, bool attdroped, bool quoset,char* strPara, TupleDesc typeinfo,bool *firstattget)
{
	char	   	temp_name[NAMEDATALEN + 3] = {0};

	if(1 == natts)
	{
		if(valueappend)
		{
			/*value*/
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(values_sql, strPara, quoset);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
			/*attribute*/
			appendtoSQL(att_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
				appendtoSQL_doubquo(att_sql, typeinfo->attrs[index]->attname.data, true);
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", index+1);
				appendtoSQL_doubquo(att_sql, temp_name, true);
			}
			appendtoSQL(att_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
		{
			/*value*/
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(values_sql, strPara, quoset);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
		*firstattget = true;
	}
	else if(0 == index)
	{
		if(valueappend)
		{
			/*value*/
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(values_sql, strPara, quoset);
			/*appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
			/*attribute*/
			appendtoSQL(att_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
				appendtoSQL_doubquo(att_sql, typeinfo->attrs[index]->attname.data, true);
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", index+1);
				appendtoSQL_doubquo(att_sql, temp_name, true);
			}
			/*appendtoSQL(att_sql, ", ", PG_LOGMINER_SQLPARA_OTHER); */
		}
		else
		{
			/*value*/
			appendtoSQL(values_sql, "(", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(values_sql, strPara, quoset);
			/*appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
		}
		*firstattget = true;
	}
	else if(natts - 1 == index)
	{
		if(*firstattget)
		{
			appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
			*firstattget = true;
		if(valueappend)
		{
			/*value*/
			appendtoSQL_simquo(values_sql, strPara, quoset);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
			/*attribute*/
			if(!attdroped)
				appendtoSQL_doubquo(att_sql, typeinfo->attrs[index]->attname.data, true);
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", index+1);
				appendtoSQL_doubquo(att_sql, temp_name, true);
			}
			appendtoSQL(att_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
		{
			/*value*/
			appendtoSQL_simquo(values_sql, strPara, quoset);
			appendtoSQL(values_sql, ")", PG_LOGMINER_SQLPARA_OTHER);
		}
	}
	else 
	{
		if(*firstattget)
		{
			appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL(att_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);
		}
		else
			*firstattget = true;
		if(valueappend)
		{
			/*value*/
			appendtoSQL_simquo(values_sql, strPara, quoset);
		/*	appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
			/*attribute*/
			if(!attdroped)
				appendtoSQL_doubquo(att_sql, typeinfo->attrs[index]->attname.data, true);
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", index+1);
				appendtoSQL_doubquo(att_sql, temp_name, true);
			}
		/*	appendtoSQL(att_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
		}
		else
		{
			/*value*/
			appendtoSQL_simquo(values_sql, strPara, quoset);
		/*	appendtoSQL(values_sql, ", ", PG_LOGMINER_SQLPARA_OTHER);*/
		}
	}

}


/*
*  
* parser tuple info from binary to string.  
*  
*/
void
mentalTup(HeapTuple tuple, TupleDesc typeinfo ,XLogMinerSQL *sql_simple, bool olddata)
{
	int 			natts;
	int 			i;
	Datum			attr;
	Datum			attr1;
	char	   		*strPara;
	
	Oid				typoutput;
	Oid 			typoutputfromdb;
	Oid 			typoutputfromdic;
	bool			typisvarlena;
	Datum	  		*values;
	bool	   		*nulls;
	bool			quoset = false;
	bool			attdroped = false;
	bool			valueappend = false;
	XLogMinerSQL 	values_sql;
	XLogMinerSQL 	att_sql;
	bool			firstattget = false;
	bool			gettype = false;
	struct varlena* att_return = NULL;

	memset(&values_sql, 0, sizeof(XLogMinerSQL));
	memset(&att_sql, 0, sizeof(XLogMinerSQL));
	
	natts = typeinfo->natts;
	valueappend = (rrctl.nomalrel) && (PG_LOGMINER_SQLKIND_INSERT == rrctl.sqlkind || PG_LOGMINER_SQLKIND_DELETE == rrctl.sqlkind);

	if(!olddata)
	{
		rrctl.values = (Datum *) logminer_palloc(natts * sizeof(Datum),0);
		rrctl.nulls = (bool *) logminer_palloc(natts * sizeof(bool),0);
		values = rrctl.values;
		nulls = rrctl.nulls;
	}
	else
	{
		rrctl.values_old = (Datum *) logminer_palloc(natts * sizeof(Datum),0);
		rrctl.nulls_old = (bool *) logminer_palloc(natts * sizeof(bool),0);
		values = rrctl.values_old;
		nulls = rrctl.nulls_old;
	}
	
	heap_deform_tuple(tuple, typeinfo, values ,nulls);
	
	for (i = 0; i < natts; ++i)
	{
		attr = values[i];

		/*If this attribute has been droped*/
		if(0 == typeinfo->attrs[i]->atttypid)
			attdroped = true;
		else
			attdroped = false;
		
		if (nulls[i])
		{
			mentalTup_nulldata(natts, i, &values_sql,&att_sql, valueappend, attdroped,&firstattget);
			continue;
		}

		if(!attdroped)
		{
			gettype = getTypeOutputFuncFromDb(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdb, &typisvarlena);
			gettype = gettype && getTypeOutputFuncFromDic(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdic, &typisvarlena);
			gettype = gettype && (typoutputfromdic == typoutputfromdb);

			gettype = gettype && (FirstNormalObjectId > typeinfo->attrs[i]->atttypid);
		}
		else
			gettype = false;


		if(!attdroped && gettype)
		{
			/*Attribute is nomal,get data nomal*/
			quoset = ifquoneed(typeinfo->attrs[i]) && rrctl.nomalrel;
			typoutput = typoutputfromdic;
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("There are some wrong data in record.")));
				attr1 = CStringGetDatum(att_return);
				strPara = convertAttrToStr(typeinfo->attrs[i], typoutput, attr1);
			}
			else
				strPara = convertAttrToStr(typeinfo->attrs[i], typoutput, attr);
		}
		else if(attdroped || !gettype)
		{
			/*Attribute is droped,get data via byte*/
			typisvarlena = (!typeinfo->attrs[i]->attbyval) && (-1 == typeinfo->attrs[i]->attlen);
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				attr1 = CStringGetDatum(att_return);
				strPara =OutputToByte((text *)attr1, typeinfo->attrs[i]->attlen);
			}
			else
				strPara = OutputToByte((text *)attr, typeinfo->attrs[i]->attlen);
			quoset = false;
		}
		mentalTup_valuedata(natts, i, &values_sql,&att_sql, valueappend, attdroped, quoset, strPara, typeinfo, &firstattget);
		
	}
	if(valueappend)
	{
		appendtoSQL(sql_simple, att_sql.sqlStr, PG_LOGMINER_SQLPARA_OTHER);
		appendBlanktoSQL(sql_simple);
	}
	appendtoSQL(sql_simple, "VALUES", PG_LOGMINER_SQLPARA_OTHER);
	appendtoSQL(sql_simple, values_sql.sqlStr, PG_LOGMINER_SQLPARA_OTHER);
	freeSpace(&att_sql);
	freeSpace(&values_sql);
}

void
reAssembleUpdateSql(XLogMinerSQL *sql_ori, bool undo)
{
	int 		natts = 0;
	int 		i = 0;
	Datum		attr;
	Datum		attr1;
	Datum		ctid;
	Datum		attr_old;
	Datum		attr_old1;
	char		*strPara = NULL;
	char	   	*strPara_old = NULL;
	char	    *ctid_str = NULL;
	char	   	temp_name[NAMEDATALEN + 3];
	
	Oid			typoutput;
	Oid 		typoutputfromdb;
	Oid 		typoutputfromdic;
	bool		typisvarlena = false;
	TupleDesc	typeinfo = NULL;
	int 		count_value = 0;
	Datum	   *values = NULL;
	bool	   *nulls = NULL;
	Datum	   *values_old = NULL;
	bool	   *nulls_old = NULL;
	bool		quoset = false;
	bool		attdroped = false;
	bool		getcondition = false;
	bool		gettype = false;
	bool		getchange = false;
	struct varlena* att_return = NULL;
	HeapTupleHeader 		htup_old = NULL;
	
	
	if(!rrctl.values || !rrctl.nulls || !rrctl.values_old || !rrctl.nulls_old || !rrctl.tupdesc)
	{
		/*should not happen*/
		ereport(ERROR,(errmsg("values or nulls or tupdesc is not exist")));
	}
	typeinfo = rrctl.tupdesc;
	natts = typeinfo->natts;

	if(!undo)
	{
		values = rrctl.values;
		nulls = rrctl.nulls;
		values_old = rrctl.values_old;
		nulls_old = rrctl.nulls_old;
	}
	else
	{
		values = rrctl.values_old;
		nulls = rrctl.nulls_old;
		values_old = rrctl.values;
		nulls_old = rrctl.nulls;
	}
	
	/*set cause*/
	for (i = 0; i < natts; ++i)
	{
		attr = values[i];
		attr_old = values_old[i];
		if(nulls[i] != nulls_old[i])
		{
			ereport(ERROR,(errmsg("new tuple nulls is different from olds")));
		}
		if(nulls[i])
			continue;

		if(0 == typeinfo->attrs[i]->atttypid)
			attdroped = true;
		else
			attdroped = false;
		
		if(!attdroped)
		{
			gettype = getTypeOutputFuncFromDb(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdb, &typisvarlena);
			gettype = gettype && getTypeOutputFuncFromDic(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdic, &typisvarlena);
			gettype = gettype && (typoutputfromdic == typoutputfromdb);
			gettype = gettype && (FirstNormalObjectId > typeinfo->attrs[i]->atttypid);
		}
		else
			gettype = false;
		
		if(!attdroped && gettype)
		{
			typoutput = typoutputfromdic;
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("wrong varlena data")));
				attr1 = CStringGetDatum(att_return);
				strPara = convertAttrToStr(typeinfo->attrs[i], typoutput, attr1);

				att_return = NULL;
				checkVarlena(attr_old,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("wrong varlena data")));
				attr_old1 = CStringGetDatum(att_return);
				strPara_old = convertAttrToStr(typeinfo->attrs[i],typoutput, attr_old1);
				
			}
			else
			{
				strPara = convertAttrToStr(typeinfo->attrs[i], typoutput, attr);
				strPara_old = convertAttrToStr(typeinfo->attrs[i], typoutput, attr_old);
			}
			quoset = ifquoneed(typeinfo->attrs[i]) && rrctl.nomalrel;
		}
		else
		{
			typisvarlena = (!typeinfo->attrs[i]->attbyval) && (-1 == typeinfo->attrs[i]->attlen);
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				attr1 = CStringGetDatum(att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("wrong varlena data")));
				strPara =OutputToByte((text *)attr1, typeinfo->attrs[i]->attlen);

				att_return = NULL;
				checkVarlena(attr_old,&att_return);
				attr_old1 = CStringGetDatum(att_return);
				strPara_old =OutputToByte((text *)attr_old1, typeinfo->attrs[i]->attlen);
			}
			else
			{		
				strPara =  OutputToByte((text *)attr, typeinfo->attrs[i]->attlen);
				strPara_old =  OutputToByte((text *)attr_old, typeinfo->attrs[i]->attlen);
			}
			quoset = false;
		}
		
		wipeSQLFromstr(sql_ori, " VALUES", " ");
		
		if(0 != strcmp(strPara ,strPara_old))
		{
			if(!attdroped)
				appendtoSQL_doubquo(sql_ori, typeinfo->attrs[i]->attname.data, true);
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", i+1);
				appendtoSQL_doubquo(sql_ori, temp_name, true);
			}
			appendtoSQL(sql_ori, " = ", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(sql_ori, strPara, quoset);
			getchange = true;
		}
	}
	/*we get nothing changed,discard the update SQL*/
	if(!getchange)
	{
		freeSpace(sql_ori);
		return;
	}
	
	appendtoSQL(sql_ori, " WHERE ", PG_LOGMINER_SQLPARA_OTHER);
	for (i = 0; i < natts; ++i)
	{
		attr_old = values_old[i];
		if(nulls_old[i])
		{
			continue;
		}

		if(0 == typeinfo->attrs[i]->atttypid)
			attdroped = true;
		else
			attdroped = false;

		
		if(!attdroped)
		{
			gettype = getTypeOutputFuncFromDb(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdb, &typisvarlena);
			gettype = gettype && getTypeOutputFuncFromDic(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdic, &typisvarlena);
			gettype = gettype && (typoutputfromdic == typoutputfromdb);
			gettype = gettype && (FirstNormalObjectId > typeinfo->attrs[i]->atttypid);
		}
		else
			gettype = false;
		
		
		if(!attdroped && gettype)
		{
			typoutput = typoutputfromdic;

			if(typisvarlena)
			{
				checkVarlena(attr_old,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("There are some wrong data in record.")));
				attr_old1 = CStringGetDatum(att_return);
				strPara_old = convertAttrToStr(typeinfo->attrs[i], typoutput, attr_old1);
			}
			else
			{
				strPara_old = convertAttrToStr(typeinfo->attrs[i], typoutput, attr_old);
			}
			quoset = ifquoneed(typeinfo->attrs[i]) && rrctl.nomalrel;
		}
		else
		{
			typisvarlena = (!typeinfo->attrs[i]->attbyval) && (-1 == typeinfo->attrs[i]->attlen);
			if(typisvarlena)
			{
				checkVarlena(attr_old,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("There are some wrong data in record.")));
				attr_old1 = CStringGetDatum(att_return);
				strPara_old =OutputToByte((text *)attr_old1, typeinfo->attrs[i]->attlen);
			}
			else
			{
				strPara_old =  OutputToByte((text *)attr_old, typeinfo->attrs[i]->attlen);
			}
		}
		
		if(0 == count_value)
		{
			if(!attdroped)
			{
				appendtoSQL_doubquo(sql_ori, typeinfo->attrs[i]->attname.data, true);
				appendtoSQL_atttyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			}
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", i+1);
				appendtoSQL_doubquo(sql_ori, temp_name, true);
			}
			appendtoSQL(sql_ori, "=", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(sql_ori, strPara_old, quoset);
			appendtoSQL_valuetyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			getcondition = true;
		}
		else
		{
			appendtoSQL(sql_ori, " AND ", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
			{
				appendtoSQL_doubquo(sql_ori, typeinfo->attrs[i]->attname.data, true);
				appendtoSQL_atttyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			}
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", i+1);
				appendtoSQL_doubquo(sql_ori, temp_name, true);
			}
			appendtoSQL(sql_ori, "=", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(sql_ori, strPara_old, quoset);
			appendtoSQL_valuetyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			getcondition = true;
		}
		count_value++;
	}
	if(undo)
	{
		htup_old = (HeapTupleHeader)rrctl.tuplem;
		ctid = (Datum)(&htup_old->t_ctid);
		ctid_str = DatumGetCString(DirectFunctionCall1(tidout, ctid));
		if(getcondition)
			appendtoSQL(sql_ori, " AND ", PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, "ctid = \'", PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, ctid_str, PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, "\';", PG_LOGMINER_SQLPARA_OTHER);
	}
	else
		appendtoSQL(sql_ori, ";", PG_LOGMINER_SQLPARA_OTHER);
}

void
reAssembleDeleteSql(XLogMinerSQL *sql_ori, bool undo)
{
	int 		natts = 0;
	int 		i = 0;
	Datum		attr;
	Datum		attr1;
	Datum		ctid;
	char	    *strPara = NULL;
	char	    *ctid_str = NULL;
	char	   	temp_name[NAMEDATALEN + 3];
	Oid			typoutput;
	Oid 		typoutputfromdb;
	Oid 		typoutputfromdic;
	bool		typisvarlena = false;
	TupleDesc 	typeinfo = NULL;
	Datum	   *values = NULL;
	bool	   *nulls = NULL;
	int			count_value = 0;
	bool		quoset = false;
	bool		attdroped = false;
	bool		getcondition = false;
	bool		gettype = false;
	struct varlena* att_return = NULL;
	HeapTupleHeader 		htup = NULL;
	
	if(!rrctl.values || !rrctl.nulls || !rrctl.tupdesc)
	{
		/*should not happen*/
		ereport(ERROR,(errmsg("values or nulls or tupdesc is not exist")));
	}
	values = rrctl.values;
	nulls = rrctl.nulls;
	typeinfo = rrctl.tupdesc;
	natts = typeinfo->natts;
	
	wipeSQLFromstr(sql_ori, "WHERE VALUES", "WHERE ");
	
	for (i = 0; i < natts; ++i)
	{
		attr = values[i];
		if (nulls[i])
		{
			continue;
		}
		
		if(0 == typeinfo->attrs[i]->atttypid)
			attdroped = true;
		else
			attdroped = false;

		
		if(!attdroped)
		{
			gettype = getTypeOutputFuncFromDb(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdb, &typisvarlena);
			gettype = gettype && getTypeOutputFuncFromDic(typeinfo->attrs[i]->atttypid,
										  &typoutputfromdic, &typisvarlena);
			gettype = gettype && (typoutputfromdic == typoutputfromdb);
			gettype = gettype && (FirstNormalObjectId > typeinfo->attrs[i]->atttypid);
		}
		else
			gettype = false;

		if(!attdroped && gettype)
		{
			typoutput = typoutputfromdic;
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("There are some wrong data in record.")));
				attr1 = CStringGetDatum(att_return);
				strPara = convertAttrToStr(typeinfo->attrs[i], typoutput, attr1);
			}
			else
				strPara = convertAttrToStr(typeinfo->attrs[i],typoutput, attr);
			quoset = ifquoneed(typeinfo->attrs[i]) && rrctl.nomalrel;
		}
		else
		{
			/*Attribute is droped,get data via byte*/
			typisvarlena = (!typeinfo->attrs[i]->attbyval) && (-1 == typeinfo->attrs[i]->attlen);
			if(typisvarlena)
			{
				checkVarlena(attr,&att_return);
				if(!att_return)
					/*should not happen*/
					ereport(ERROR,(errmsg("There are some wrong data in record.")));
				attr1 = CStringGetDatum(att_return);
				strPara =OutputToByte((text *)attr1, typeinfo->attrs[i]->attlen);
			}
			else
				strPara = OutputToByte((text *)attr, typeinfo->attrs[i]->attlen);
			quoset = false;
		}
		
		if(0 == count_value)
		{
			if(!attdroped)
			{
				appendtoSQL_doubquo(sql_ori, typeinfo->attrs[i]->attname.data, true);
				appendtoSQL_atttyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			}
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", i+1);
				appendtoSQL_doubquo(sql_ori, temp_name, true);
			}
			appendtoSQL(sql_ori, "=", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(sql_ori, strPara, quoset);
			appendtoSQL_valuetyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			getcondition = true;
		}
		else
		{
			appendtoSQL(sql_ori, " AND ", PG_LOGMINER_SQLPARA_OTHER);
			if(!attdroped)
			{
				appendtoSQL_doubquo(sql_ori, typeinfo->attrs[i]->attname.data, true);
				appendtoSQL_atttyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			}
			else
			{
				memset(temp_name,0,NAMEDATALEN + 3);
				sprintf(temp_name, "COL%d", i+1);
				appendtoSQL_doubquo(sql_ori, temp_name, true);
			}
			appendtoSQL(sql_ori, "=", PG_LOGMINER_SQLPARA_OTHER);
			appendtoSQL_simquo(sql_ori, strPara, quoset);
			appendtoSQL_valuetyptrans(sql_ori, typeinfo->attrs[i]->atttypid);
			getcondition = true;
		}
		count_value++;
	}
	/*append ctid condition*/
	if(undo)
	{
		if(rrctl.tuplem_bigold)
			htup = (HeapTupleHeader)rrctl.tuplem_bigold;
		else
			htup = (HeapTupleHeader)rrctl.tuplem;
		ctid = (Datum)(&htup->t_ctid);
		ctid_str = DatumGetCString(DirectFunctionCall1(tidout, ctid));
		if(getcondition)
			appendtoSQL(sql_ori, " AND ", PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, "ctid = \'", PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, ctid_str, PG_LOGMINER_SQLPARA_OTHER);
		appendtoSQL(sql_ori, "\';", PG_LOGMINER_SQLPARA_OTHER);
	}
	else
		appendtoSQL(sql_ori, ";", PG_LOGMINER_SQLPARA_OTHER);
}

