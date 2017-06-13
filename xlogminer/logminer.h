/*
*
*contrib/xlogminer/logminer.h
*/

#ifndef LOGMINER_H
#define LOGMINER_H
#include "postgres.h"
#include "pg_logminer.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "access/xlog_internal.h"



#define bbr_Judge(ch) ((PG_LOGMINER_BBRACK_R == ch)?true:false)
#define bbl_Judge(ch) ((PG_LOGMINER_BBRACK_L == ch)?true:false)
#define sbr_Judge(ch) ((PG_LOGMINER_SBRACK_R == ch)?true:false)
#define sbl_Judge(ch) ((PG_LOGMINER_SBRACK_L == ch)?true:false)



#define	LOGMINERDIR						"xlogminer"
#define	LOGMINERDIR_DIC					"dic"
#define	LOGMINERDIR_LIST				"list"

#define PG_LOGMINER_SPACE				' '
#define PG_LOGMINER_SBRACK_L			'('
#define PG_LOGMINER_SBRACK_R			')'
#define PG_LOGMINER_COMMA				','
#define PG_LOGMINER_BBRACK_L			'{'
#define PG_LOGMINER_BBRACK_R			'}'
#define PG_LOGMINER_ENDSTR				'\n'

#define	PG_LOGMINER_RELNULL				"NULL"
#define PG_LOMINER_ALLROLEPRIV			"arwdDxt"

#define LOGMINER_PRIVKIND_MAXNUM	15

#define PG_LOGMINER_SQLPARA_TOTSTEP 10000
#define PG_LOGMINER_SQLPARA_SIMSTEP 100
#define PG_LOGMINER_SQLPARA_TOTLE 	1
#define PG_LOGMINER_SQLPARA_SIMPLE 	2
#define PG_LOGMINER_SQLPARA_OTHER 	3

#define	LOGMINER_INSERTSTEP						4
#define	LOGMINER_DELETESTEP						5


/*-----------sysclass  attribute location-----------*/
#define	LOGMINER_STEP							1
#define LOGMINER_ATTRIBUTE_LOCATION_UPDATE_RELNAME			2
#define	LOGMINER_ATTRIBUTE_LOCATION_UPDATE_NEWDATA			5
#define	LOGMINER_ATTRIBUTE_LOCATION_UPDATE_OLDDATA			8
#define LOGMINER_ATTRIBUTE_LOCATION_CREATE_KIND						2
#define LOGMINER_ATTRIBUTE_LOCATION_CREATE_NAME						3
#define	LOGMINER_INSERT_TABLE_NAME						3
#define	LOGMINER_SQL_COMMAND							1
#define	LOGMINER_DELETE_TABLE_NAME						3
/*--------------------------------------------*/


#define	LOGMINER_RELKINDID_TABLE				1
#define	LOGMINER_RELKINDID_INDEX				2
#define	LOGMINER_RELKINDID_SEQUENCE				3
#define	LOGMINER_RELKINDID_VIEW					4
#define	LOGMINER_RELKINDID_TOAST				5
#define	LOGMINER_RELKINDID_COMPLEX				6
#define LOGMINER_RELATIONKIND_NUM 				7

#define LOGMINER_CONSTRAINT_CONNUM			6
#define LOGMINER_CONSTRAINT_CHECK			'c'
#define LOGMINER_CONSTRAINT_FOREIGN			'f'
#define LOGMINER_CONSTRAINT_PRIMARY			'p'
#define LOGMINER_CONSTRAINT_UNIQUE			'u'
#define LOGMINER_CONSTRAINT_TRIGGER			't'
#define LOGMINER_CONSTRAINT_EXCLUSION		'x'



#define 	LOGMINER_SQLGET_DDL_CREATE_TABLE			1
#define 	LOGMINER_SQLGET_DDL_CREATE_INDEX			2
#define 	LOGMINER_SQLGET_DDL_CREATE_SEQUENCE			3
#define 	LOGMINER_SQLGET_DDL_CREATE_VIEW				4
#define 	LOGMINER_SQLGET_DDL_CREATE_TOAST			5
#define 	LOGMINER_SQLGET_DDL_CREATE_COMPLEX			6
#define 	LOGMINER_SQLGET_DDL_CREATE_CONSTRAINT		7
#define 	LOGMINER_SQLGET_DDL_CREATE_MATERIALIZEDVIEW			8

#define 	LOGMINER_SQLGET_DML_INSERT					8
#define 	LOGMINER_SQLGET_DML_UPDATE					9
#define 	LOGMINER_SQLGET_DML_DELETE					10
#define		LOGMINER_SQLGET_DML_ALTERTABLE_ADDCOLUMN					11
#define		LOGMINER_SQLGET_DML_ALTERTABLE_ALTERCOLTYP_SEG1				12
#define		LOGMINER_SQLGET_DML_ALTERTABLE_ALTERCOLTYP_SEG2				13
#define		LOGMINER_SQLGET_DML_ALTERTABLE_COLUMNDEFAULT				14
#define		LOGMINER_SQLGET_DDL_CREATE_DATABASE							15
#define 	LOGMINER_SQLGET_DDL_CREATE_PARTTABLE		16




#define		LOGMINER_SIMPLESQL_BUFSIZE					1024

#define		LOGMINER_TEMPFILE_SIMPLESQL		"simple.sql"
#define		LOGMINER_TEMPFILE_DBSQL		"db.sql"
#define		LOGMINER_PASSKIND_SPECIALLOC_MAX			5
#define		LOGMINER_PASSKIND_SPECIALCH_CUT				1	/*a special char as Separator only*/
#define		LOGMINER_PASSKIND_SPECIALCH_AVOID			2	/*a special char escape a Separator*/

#define		LOGMINER_WARNING_WORD_FULLPAGEWRITE			"data missing. \n"

#define		LOGMINER_PROSTATUE_INSERT_MISSING_TUPLEINFO			0x01
#define		LOGMINER_PROSTATUE_DELETE_MISSING_TUPLEINFO			0x02
#define		LOGMINER_PROSTATUE_UPDATE_MISSING_NEW_TUPLEINFO		0x03
#define		LOGMINER_PROSTATUE_UPDATE_MISSING_OLD_TUPLEINFO		0x04




typedef struct RelationKind{
	int		sqlkind;
	int		relkindid;
	char	*relname;
	char	relkind;
	bool	show;
}RelationKind;

typedef struct SQLPassOver{
	/*special location*/
	int		passloc[LOGMINER_PASSKIND_SPECIALLOC_MAX];
	/*special location num*/
	int		passlocnum;
	
	int		passkind;
	/*special char*/
	char	specialch[2];
	/*special char num*/
	int		specialnum;
}SQLPassOver;

typedef struct OperaPriv{
	char elemname[NAMEDATALEN];
	char privkind[LOGMINER_PRIVKIND_MAXNUM];
}OperaPriv;

typedef struct PrivKind{
	char	privch;
	char	*privstr;
}PrivKind;


typedef struct XlogminerContentsFirst{
	/*int 					sqlno*/
	TransactionId			xid;
	/*uint32					virtualxid;*/
	/*Timestamp				timestamp;*/
	XLogMinerSQL			record_database;
	XLogMinerSQL			record_user;
	XLogMinerSQL			record_tablespace;
	XLogMinerSQL			record_schema;
	XLogMinerSQL			op_type;
	XLogMinerSQL			op_text;
	XLogMinerSQL			op_undo;
}XlogminerContentsFirst;


bool getPhrases(char *sql,int loc, char *term, int ignoresbrackph);
void addSpace(XLogMinerSQL *sql_simple, int spaceKind);
void cleanSpace(XLogMinerSQL *minersql);
void freeSpace(XLogMinerSQL *minersql);
void split_path_fname(const char *path, char **dir, char **fname);
RelationKind* getRelKindInfo();
void xactCommitSQL(char* timestr,XLogMinerSQL *sql_opt,uint8 info);
int xlog_file_open(const char *directory, const char *fname);
bool isEmptStr(char *str);
void xlsql_addtoList(XLsqlhead *xlhead,XLsqList *xlsql);
TupleDesc makeOutputXlogDesc();
bool inputParaCheck();
bool curXactCheck(TimestampTz xact_time ,TransactionId xid, bool xactcommit,xl_xact_parsed_commit *parsed_commit);
char* logminer_palloc(int size,int checkflag);
void logminer_free(char* ptr,int checkflag);
char* logminer_malloc(int size,int checkflag);
void logminer_free(char* ptr,int checkflag);
void logminer_createMemContext();
void logminer_switchMemContext();
bool checkLogminerUser();
bool padingminerXlogconts(char* elemname, TransactionId xid,int loc,long elemoid);
void cleanMentalvalues();
bool elemNameFind(char* elenname);
void cleanAnalyseInfo();
void padNullToXC();
char* getTuplemSpace(int size);
void cleanTuplemSpace(char* tuplem);
bool ifquoneed(Form_pg_attribute attrs);
char* OutputToByte(text* attrpter, int attlen);
ToastTuple* makeToastTuple(int datalength,char* data, Oid id, int seq);
void freeToastTupleHead();
void toastTupleAddToList(ToastTuple *tt);
text* cstringToTextWithLen(const char *s, int len);
bool getTypeOutputFuncFromDb(Oid type, Oid *typOutput, bool *typIsVarlena);
char* convertAttrToStr(Form_pg_attribute fpa,Oid typoutput, Datum attr);
int strcmp_withlength(char *str1,char *str2,int length);
bool ifQueNeedDelete(Form_pg_attribute attrs);
void checkVarlena(Datum attr,struct varlena** att_return);
void deleteQueFromStr(char* strPara);
void keepDigitFromStr(char* strPara);
void fixPathEnd(char *path);


#endif

