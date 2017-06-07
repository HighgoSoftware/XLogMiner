/*
*
*contrib/xlogminer/datadictionary.h
*/
#ifndef PG_DATADICTIONARY_H
#define PG_DATADICTIONARY_H
#include "postgres.h"

#include "logminer.h"

#define PG_LOGMINER_DICTIONARY_DEFAULTNAME			"dictionary.d"
#define PG_LOGMINER_DICTIONARY_STOREPATH			"pgdictory.store"
#define	PG_LOGMINER_DICTIONARY_XLOGFILELIST			"xloglist.list"
#define PG_LOGMINER_DICTIONARY_TEMPTABLE			"xlogminer_contents"

#define PG_LOGMINER_DICTIONARY_PATHCHECK_NULL			0
#define PG_LOGMINER_DICTIONARY_PATHCHECK_FILE			1
#define PG_LOGMINER_DICTIONARY_PATHCHECK_DIR			2
#define PG_LOGMINER_DICTIONARY_PATHCHECK_INVALID		3
#define PG_LOGMINER_DICTIONARY_PATHCHECK_SINGLE			4
#define	PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE		1024
#define	PG_LOGMINER_XLOGFILE_REPEAT_CHECK_DIFFERENT		1
#define	PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SAME			2
#define	PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SEGSAME		3


#define	PG_LOGMINER_DICTIONARY_LOADTYPE_SELF			1
#define	PG_LOGMINER_DICTIONARY_LOADTYPE_OTHER			2
#define	PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING			3




#define	PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE		200 * 1024
extern char* DataDictionaryCache;
extern char* XlogfileListCache;
extern char	dictionary_path[MAXPGPATH];

typedef struct SysDataCache{
	char*	data;
	char*	curdata;
	int64	usesize;
	int64	totsize;
	int		elemnum;
}SysDataCache;

typedef struct DataDicHead{
	NameData relname;
	int		 elemnum;
}DataDicHead;

typedef struct PgDataDic{
	uint64			sysid;
	Oid				dboid;
	SysDataCache	sdc[PG_LOGMINER_IMPTSYSCLASS_IMPTNUM];
	int				loadtype;
}PgDataDic;

typedef struct XlogFile	
{			
	char filepath[MAXPGPATH];
	struct XlogFile* tail;
	struct XlogFile* next;
}XlogFile;

typedef XlogFile* XlogFileList;			


char* outputSysTableDictionary(char *path, SysClassLevel *scl, bool self);
void loadSystableDictionary(char *path, SysClassLevel *scl, bool self);
int addxlogfile(char *path);
int removexlogfile(char *path);
void cleanSystableDictionary();
void cleanXlogfileList();
bool getRelationOidByName(char* relname, Oid* reloid, bool gettemptable);
uint64 getDataDicSysid();
int getDatadictionaryLoadType();
bool is_xlogfilelist_exist();
int getXlogFileNum();
int getRelationNameByOid(Oid reloid, NameData* relname);
TupleDesc GetDescrByreloid(Oid reloid);
bool tableIftoastrel(Oid reloid);
bool getTypeOutputFuncFromDic(Oid type, Oid *typOutput, bool *typIsVarlena);
Oid getDataDicOid();
bool loadXlogfileList();
bool loadDicStorePath(char *dicstorepath);
void writeXlogfileList();
void writeDicStorePath(char* dicstorepath);
void dropAnalyseFile();
char* getNextXlogFile(char *fctx, bool show);
void searchSysClass( SystemClass *sys_class,int	*sys_classNum);
char* getdbNameByoid(Oid dboid, bool createdb);
char* gettbsNameByoid(Oid tbsoid);
Oid gettuserOidByReloid(Oid reloid);
char* getuserNameByUseroid(Oid useroid);
Oid getnsoidByReloid(Oid reloid);
char* getnsNameByOid(Oid schoid);
char* getnsNameByReloid(Oid reloid);
Oid getRelationOidByRelfileid(Oid relNodeid);
void freetupdesc(TupleDesc tupdesc);


#endif
