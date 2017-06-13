/*
*
*DataDictionary and XlogFileList function 
*
*contrib/xlogminer/datadictionary.h
*/

#include "datadictionary.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_index.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_language.h"
#include "utils/lsyscache.h"
#include <sys/types.h>
#include <sys/stat.h>


	
char* DataDictionaryCache = NULL;
char* XlogfileListCache = NULL;
char	dictionary_path[MAXPGPATH] = {0};
char	xlogfilelist_path[MAXPGPATH] = {0};

static void initcache(SysDataCache *systabcatch);
static void addspacetocache(SysDataCache *systabcatch, int stepsize);
static void	freecache(SysDataCache *systabcatch);
static void cleancache(SysDataCache *systabcatch);
static int is_file_exist(char *filepath);
static int is_dir_exist(char *dirpath);
static int scanDir_getfilenum(char *scdir);
static int scanDir_getfilename(char *scdir,NameData *datafilename, bool sigfile);
static void dropAllFileInADir(char *dir);
static int pathcheck(char *path);
static bool create_dir(char *path);
static PgDataDic* initDataDicMemory();
static bool checkXlogFileValid(char *path,int pathkind);
static int checkXlogFileRepeat(char *path,XlogFile **xfrepeat);
static void paddingscl(SysClassLevel *scl);
static void proCheckBit(FILE *fp, char *outPtr, int outsize);
static void cheCheckBit(char *outPtr, int outsize, uint64 checkbit);
static void buildhead(FILE	*fp, char *relname, int elemnum);
static void builddata(SysDataCache *sdc,char *relname);
static void buildsysid(int dicloadtype);
static void loadsysid(FILE *fp,PgDataDic* pdd);
static void loadhead(FILE *fp, DataDicHead *ddh);
static void loaddata(FILE *fp, SysDataCache *sdc, DataDicHead *ddh, int sigsize);
static XlogFile* fillXlogFile(char *path);
static XLogSegNo getXlogFilesegno(XlogFile *xf);
static TimeLineID getXlogFiletimeline(XlogFile *xf);
static void* insertXlogFileToList(XlogFile *xf);
static void addxlogfileToList(char *path);
static void removexlogfileFromList(XlogFile *removexlogfile);
static void outSigSysTableDictionary(Oid reloid, char* relname,int datasgsize, SysDataCache *sdc);
static void loadSigSysTableDictionary(FILE *fp, int tabid, SysDataCache *sdc, int sigsize);
static char* logminer_getnext(int search_tabid,SysClassLevel *scl);
static char* logminer_getFormByOid(int search_tabid, Oid reloid);
static void setMaxTimeLineId(TimeLineID tempid);
static TimeLineID getMaxTimeLineId();



static void
initcache(SysDataCache *systabcatch)
{
	/*cache space malloc*/
	memset(systabcatch, 0, sizeof(SysDataCache));
	systabcatch->data = logminer_malloc(PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE,0);
	memset(systabcatch->data, 0, sizeof(PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE));
	if(!systabcatch->data)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("Out of memory")));
	systabcatch->totsize = PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE;
	systabcatch->curdata = systabcatch->data;
}

static void
addspacetocache(SysDataCache *systabcatch, int stepsize)
{
	char *temp_space = NULL;
	int	 relstep = 0;

	relstep = (0 == stepsize)?PG_LOGMINER_DICTIONARY_SYSDATACACHE_SIZE:stepsize;
	
	temp_space = logminer_malloc(systabcatch->totsize + relstep,0);
	if(!temp_space)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("Out of memory")));
	memset(temp_space, 0, systabcatch->totsize + relstep);
	if(systabcatch->data)
	{
		memcpy(temp_space, systabcatch->data, systabcatch->totsize);
		logminer_free(systabcatch->data,0);
	}
	systabcatch->totsize += relstep;
	systabcatch->data = temp_space;
	systabcatch->curdata = temp_space + systabcatch->usesize;
	temp_space = NULL;
}

static void
adddatatocache(SysDataCache *systabcatch,HeapTuple tuple, int datasize)
{
	char	*data = NULL;
	Oid		reloid = 0;

	reloid = HeapTupleHeaderGetOid(tuple->t_data);
	data = GETSTRUCT(tuple) - sizeof(Oid);

	
	if(systabcatch->totsize - systabcatch->usesize < datasize)
		addspacetocache(systabcatch,0);
	
	memcpy(systabcatch->curdata, data, datasize);
	systabcatch->elemnum++;
	systabcatch->usesize += datasize;
	systabcatch->curdata += datasize;
}

static void
freecache(SysDataCache *systabcatch)
{
	if(!systabcatch)
		return;
	if(!systabcatch->data)
		return;
	logminer_free(systabcatch->data,0);
	memset(systabcatch, 0, sizeof(SysDataCache));
}

static void
cleancache(SysDataCache *systabcatch)
{
	if(!systabcatch)
		return;
	if(!systabcatch->data)
		return;
	memset(systabcatch->data, 0, systabcatch->totsize);
	systabcatch->usesize = 0;
	systabcatch->elemnum = 0;
	systabcatch->curdata = systabcatch->data;
}


static int
is_file_exist(char *filepath)
{
	if(!filepath)
		return 0;
#ifdef WIN32
	if(_access(filepath,F_OK) == 0)
#else
	if(access(filepath,F_OK) == 0)
#endif
		return 1;
	return 0;
}

static int
is_dir_exist(char *dirpath)
{
	DIR		*dirptr = NULL;
	if(!dirpath)
		return 0;
	dirptr = opendir(dirpath);
	if(!dirptr)
		return 0;
	closedir(dirptr);
	return 1;
}

static int
scanDir_getfilenum(char *scdir)
{
	int				filecount = 0;
	DIR				*pDir = NULL;
	char			dir[MAXPGPATH];
	struct stat 	statbuf;
	struct dirent*	ent = NULL;
	
	if(!scdir)
		return;
	if (NULL == (pDir = opendir(scdir)))
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg("Open dir \"%s\" failed", scdir)));

	while (NULL != (ent = readdir(pDir)))
	{
		snprintf(dir, MAXPGPATH, "%s/%s", scdir, ent->d_name);
		lstat(dir, &statbuf);
		if (!S_ISDIR(statbuf.st_mode))
		{
			if(0 == strcmp("..",ent->d_name) || 0 == strcmp(".",ent->d_name))
				continue;
			filecount++;
		}
	}
	closedir(pDir);

	return filecount;
}


static int
scanDir_getfilename(char *scdir,NameData *datafilename, bool sigfile)
{
	int				filecount = 0;
	int				filenamlength = 0;
	DIR				*pDir = NULL;
	char			dir[MAXPGPATH];
	struct stat 	statbuf;
	struct dirent*	ent = NULL;
	
	if(!scdir)
		return;
	if (NULL == (pDir = opendir(scdir)))
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg("Open dir \"%s\" failed", scdir)));

	while (NULL != (ent = readdir(pDir)))
	{
		snprintf(dir, MAXPGPATH, "%s/%s", scdir, ent->d_name);
		lstat(dir, &statbuf);
		if (S_ISREG(statbuf.st_mode))
		{
			if(0 == strcmp("..",ent->d_name) || 0 == strcmp(".",ent->d_name))
				continue;
			filenamlength = strlen(ent->d_name);
			if(NAMEDATALEN <= filenamlength)
				ereport(ERROR,
				(errcode(ERRCODE_INVALID_SQL_STATEMENT_NAME),
				errmsg("Filename \"%s\" is too long",ent->d_name)));
			memcpy(datafilename[filecount].data, ent->d_name, filenamlength);
			filecount++;
			if(sigfile && 1 < filecount)
				break;
			
		}
	}
	closedir(pDir);

	return filecount;
}


static void
dropAllFileInADir(char *dir)
{
	char		temp[MAXPGPATH] = {0};
	NameData	*filenamelist = NULL;
	int			filenum = 0,i = 0;

	filenum = scanDir_getfilenum(dir);
	if(0 < filenum)
	{
		filenamelist = (NameData *)palloc0(filenum * sizeof(NameData));
		scanDir_getfilename(dir, filenamelist,false);
		for(; i < filenum; i++)
		{
			memset(temp,0,MAXPGPATH);
			snprintf(temp, MAXPGPATH, "%s/%s", dir, filenamelist[i].data);
			remove(temp);
		}
	}
}

static int
pathcheck(char *path)
{
	int 		result = PG_LOGMINER_DICTIONARY_PATHCHECK_INVALID;
	char	   *directory = NULL;
	char	   *fname = NULL;
	int			posend = 0;
	
	if(!path || 0 == strcmp("",path))
		return PG_LOGMINER_DICTIONARY_PATHCHECK_NULL;
	
	if(1 == is_dir_exist(path))
	{
		result = PG_LOGMINER_DICTIONARY_PATHCHECK_DIR;
		posend = strlen(path) - 1;
		if('/' == path[posend])
			path[posend] = 0;
		return result;
	}
	
	split_path_fname(path, &directory, &fname);
	if(strlen(fname) == strlen(path))
	{
		result = PG_LOGMINER_DICTIONARY_PATHCHECK_SINGLE;
	}
	else if(1 == is_dir_exist(directory))
	{
		result = PG_LOGMINER_DICTIONARY_PATHCHECK_FILE;
	}
	if(fname)
		pfree(fname);
	if(directory)
		pfree(directory);
	return result;
}


static bool
create_dir(char *path)
{	
	int	result = 0;
	if(is_dir_exist(path))
		return true;
	result = mkdir(path,S_IRWXU);
	if(0 == result)
		return true;
	else
		return false;
}


static PgDataDic*
initDataDicMemory()
{
	PgDataDic *pdd = NULL;
	DataDictionaryCache = logminer_malloc(sizeof(PgDataDic),0);
	if(!DataDictionaryCache)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("Out of memory")));
	memset(DataDictionaryCache, 0, sizeof(PgDataDic));
	pdd = (PgDataDic *)DataDictionaryCache;
	
	return pdd;
}

int
getDatadictionaryLoadType()
{
	PgDataDic *pdd = NULL;
	if(!DataDictionaryCache)
		return PG_LOGMINER_DICTIONARY_LOADTYPE_NOTHING;

	pdd = (PgDataDic *)DataDictionaryCache;
	return pdd->dicloadtype;
}

bool
is_xlogfilelist_exist()
{
	if(!XlogfileListCache)
		return false;
	return true;
}

uint64
getDataDicSysid()
{
	PgDataDic *pdd = NULL;
	pdd = (PgDataDic*)DataDictionaryCache;
	return pdd->sysid;
}

Oid
getDataDicOid()
{
	PgDataDic *pdd = NULL;
	pdd = (PgDataDic*)DataDictionaryCache;
	return pdd->dboid;
}

static bool
checkXlogFileValid(char *path,int pathkind)
{
	FILE 					*fp = NULL;
	int						length = 0,loop = 0;
	bool					filenamevalid = true;
	char					*filedir =NULL, *filename=NULL;
	
	XLogLongPageHeaderData 	xlphd;
	
	split_path_fname(path,&filedir,&filename);
	/*file name check*/
	length = strlen(filename);
	if(length != 24)
		filenamevalid = false;
	
	for(;loop < length && filenamevalid; loop++)
	{
		if(filename[loop] < '0' || filename[loop] > 'F')
			filenamevalid = false;
	}
	
	if(filename)
		pfree(filename);
	if(filedir)
		pfree(filedir);
	
	if(!filenamevalid)
	{
		if(PG_LOGMINER_DICTIONARY_PATHCHECK_DIR != pathkind)
			ereport(ERROR,(errmsg("Xlog file \"%s\" is invalid.",path)));
		else
			return false;
	}
	
	/*match the datadictionary check*/
	fp = fopen(path, "rb");
	if(!fp)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg("Open file \"%s\" failed",path)));
	memset(&xlphd, 0, SizeOfXLogLongPHD);
	fread(&xlphd, SizeOfXLogLongPHD, 1, fp);

	if(getDataDicSysid() == xlphd.xlp_sysid)
		return true;
	else
	{
		ereport(NOTICE,(errmsg("Wal file \"%s\" is not match with datadictionary.",path)));
		return false;
	}
}

static int
checkXlogFileRepeat(char *path,XlogFile **xfrepeat)
{
	XlogFileList xfl = NULL;
	XlogFile	 *xfptr = NULL;
	char		*filedir = NULL,*filename = NULL;
	TimeLineID	timelinecheck = 0;
	XLogSegNo	segnocheck = 0;
	TimeLineID	timelinecur = 0;
	XLogSegNo	segnocur = 0;
	
	xfl = (XlogFileList)XlogfileListCache;
	if(!xfl)
		return PG_LOGMINER_XLOGFILE_REPEAT_CHECK_DIFFERENT;
	
	split_path_fname(path,&filedir,&filename);
	XLogFromFileName(filename, &timelinecheck, &segnocheck);

	xfptr = xfl;
	while(xfptr)
	{
		split_path_fname(xfptr->filepath,&filedir,&filename);
		XLogFromFileName(filename, &timelinecur, &segnocur);
		if(0 == strcmp(path, xfptr->filepath))
		{
			*xfrepeat = xfptr;
			return PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SAME;
		}
		else if(timelinecur == timelinecheck && segnocur == segnocheck)
		{
			*xfrepeat = xfptr;
			return PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SEGSAME;
		}
		xfptr = xfptr->next;
	}
	return PG_LOGMINER_XLOGFILE_REPEAT_CHECK_DIFFERENT;
	
}

void
cleanSystableDictionary()
{
	PgDataDic*	pdd = NULL;
	int			loop = 0;
	if(!DataDictionaryCache)
		return;
	
	pdd = (PgDataDic*)DataDictionaryCache;
	for(loop = 0; loop < PG_LOGMINER_IMPTSYSCLASS_IMPTNUM; loop++)
	{
		if(pdd->sdc[loop].data)
		{
			logminer_free(pdd->sdc[loop].data,0);
			pdd->sdc[loop].data = NULL;
			pdd->sdc[loop].curdata = NULL;
		}
	}
	logminer_free(DataDictionaryCache,0);
	DataDictionaryCache = NULL;
}

static void
paddingscl(SysClassLevel *scl)
{
	Form_pg_class				fpclass = NULL;
	Form_pg_database			fpdatabase = NULL;
	Form_pg_extension			fpextension = NULL;
	Form_pg_namespace			fpnamespace = NULL;
	Form_pg_tablespace			fptablespace = NULL;
	Form_pg_constraint			fpconstraint = NULL;
	Form_pg_authid				fpauthid = NULL;
	Form_pg_proc				fpproc = NULL;
	Form_pg_depend				fpdepend = NULL;
	Form_pg_index				fpindex = NULL;
	Form_pg_attribute			fpattribute = NULL;
	Form_pg_shdescription		fpshdescription = NULL;
	Form_pg_attrdef				fpattrdef = NULL;
	Form_pg_type				fptype = NULL;
	Form_pg_auth_members		fpauthmembers = NULL;
	Form_pg_inherits			fpinherits = NULL;
	Form_pg_trigger				fptrigger = NULL;
	Form_pg_language			fplanguage = NULL;

	
	scl[PG_LOGMINER_IMPTSYSCLASS_PGCLASS].datasgsize = sizeof(*fpclass) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGDATABASE].datasgsize = sizeof(*fpdatabase) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGEXTENSION].datasgsize = sizeof(*fpextension) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGNAMESPACE].datasgsize = sizeof(*fpnamespace) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGTABLESPACE].datasgsize = sizeof(*fptablespace) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGCONSTRAINT].datasgsize = sizeof(*fpconstraint) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGAUTHID].datasgsize = sizeof(*fpauthid) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGPROC].datasgsize = sizeof(*fpproc) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGDEPEND].datasgsize = sizeof(*fpdepend) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGINDEX].datasgsize = sizeof(*fpindex) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGATTRIBUTE].datasgsize = sizeof(*fpattribute) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGSHDESC].datasgsize = sizeof(*fpshdescription) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGATTRDEF].datasgsize = sizeof(*fpattrdef) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGTYPE].datasgsize = sizeof(*fptype) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGAUTH_MEMBERS].datasgsize = sizeof(*fpauthmembers) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGINHERITS].datasgsize = sizeof(*fpinherits) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGTRIGGER].datasgsize = sizeof(*fptrigger) + sizeof(Oid);
	scl[PG_LOGMINER_IMPTSYSCLASS_PGLANGUAGE].datasgsize = sizeof(*fplanguage) + sizeof(Oid);
}


bool
getRelationOidByName(char* relname, Oid* reloid, bool gettemptable)
{
	bool				result = false;
	Relation			pgclass = NULL;
	HeapScanDesc		scan = NULL;
	HeapTuple			tuple = NULL;
	Form_pg_class 		classForm = NULL;
	
	pgclass = heap_open(RelationRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(pgclass, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		classForm = (Form_pg_class) GETSTRUCT(tuple);
		if(0 == strcmp(relname,classForm->relname.data))
		{
			if(gettemptable && RELPERSISTENCE_TEMP != classForm->relpersistence)
				continue;
			if(!gettemptable && PG_CATALOG_NAMESPACE != classForm->relnamespace)
				continue;
			*reloid = HeapTupleHeaderGetOid(tuple->t_data);
			result = true;
			break;
		}
	}
	heap_endscan(scan);
	heap_close(pgclass, AccessShareLock);
	return result;
}

static void
proCheckBit(FILE *fp, char *outPtr, int outsize)
{
	int		loop = 0;
	uint64	checkbit = 0;
	for(;loop < outsize; loop++)
	{
		checkbit += outPtr[loop];
	}
	fwrite(&checkbit, sizeof(uint64), 1, fp);
}

static void
cheCheckBit(char *outPtr, int outsize, uint64 checkbit)
{
	int		loop = 0;
	uint64	checkbitcal = 0;
	
	for(;loop < outsize; loop++)
	{
		checkbitcal += outPtr[loop];
	}
	if(checkbitcal != checkbit)
		ereport(ERROR,(errmsg("Invalid data dictionary file.")));
}

static void
buildhead(FILE	*fp, char *relname, int elemnum)
{
	DataDicHead ddh;
	memset(&ddh, 0, sizeof(DataDicHead));
	memcpy(ddh.relname.data, relname, strlen(relname));
	ddh.elemnum = elemnum;
	fwrite(&ddh, sizeof(DataDicHead), 1, fp);
	proCheckBit(fp, &ddh, sizeof(DataDicHead));
}

static void
builddata(SysDataCache *sdc,char *relname)
{
	FILE	*fp = NULL;
	fp = fopen(dictionary_path,"ab");
	if(!fp)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg("Can not create dictionary file \"%s\"",dictionary_path)));
	buildhead(fp, relname, sdc->elemnum);
	fwrite(sdc->data, sdc->usesize, 1, fp);
	proCheckBit(fp, sdc->data, sdc->usesize);
	fclose(fp);
}

static void
buildsysid(int dicloadtype)
{
	FILE	*fp = NULL;
	uint64	sysid = 0;
	uint64	checkbit = 0;
	fp = fopen(dictionary_path,"wb");
	if(!fp)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg("Can not create dictionary file %s",dictionary_path)));

	sysid = GetSystemIdentifier();
	fwrite(&sysid, sizeof(sysid), 1,fp);
	fwrite(&MyDatabaseId, sizeof(Oid), 1,fp);
	fwrite(&dicloadtype, sizeof(int),1,fp);
	checkbit = MyDatabaseId + sysid;
	fwrite(&checkbit, sizeof(sysid), 1,fp);
	fclose(fp);
}

static void
loadsysid(FILE *fp,PgDataDic* pdd)
{
	uint64	checkbit = 0;
	if(!fp)
		return;
	fread(&pdd->sysid,sizeof(uint64),1,fp);
	fread(&pdd->dboid,sizeof(Oid),1,fp);
	fread(&pdd->dicloadtype,sizeof(int),1,fp);
	fread(&checkbit,sizeof(uint64),1,fp);
	if(pdd->sysid + pdd->dboid != checkbit)
	{
		cleanSystableDictionary();
		ereport(ERROR,(errmsg("Invalid data dictionary file.")));
	}
	if(0 == checkbit)
	{
		cleanSystableDictionary();
		ereport(ERROR,(errmsg("Invalid data dictionary file.")));
	}
}

static void
loadhead(FILE *fp, DataDicHead *ddh)
{
	uint64	checkbit = 0;
	if(!ddh || !fp)
		return;
	fread(ddh,sizeof(DataDicHead),1,fp);
	fread(&checkbit,sizeof(uint64),1,fp);
	cheCheckBit(ddh, sizeof(DataDicHead), checkbit);
}

static void
loaddata(FILE *fp, SysDataCache *sdc, DataDicHead *ddh, int sigsize)
{
	uint64	checkbit = 0;
	if(!sdc || !ddh || !fp)
		return;
	
	addspacetocache(sdc, sigsize * ddh->elemnum);
	
	fread(sdc->data, sigsize, ddh->elemnum, fp);
	sdc->usesize = sigsize * ddh->elemnum;
	fread(&checkbit,sizeof(uint64),1,fp);
	cheCheckBit(sdc->data, sdc->usesize, checkbit);
}


static XlogFile*
fillXlogFile(char *path)
{

	XlogFile	*xfl_temp = NULL;
	xfl_temp = (XlogFile*)logminer_malloc(sizeof(XlogFile),0);
	if(!xfl_temp)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("Out of memory")));
	memset(xfl_temp, 0, sizeof(XlogFile));
	memcpy(xfl_temp->filepath, path, strlen(path));
	return xfl_temp;
}


static XLogSegNo
getXlogFilesegno(XlogFile *xf)
{
	XLogSegNo				segno = 0;
	TimeLineID				timeline = 0;
	char					*fname = NULL;
	char					*directory = NULL;
	
	
	split_path_fname(xf->filepath, &directory, &fname);
	XLogFromFileName(fname, &timeline, &segno);
	if(fname)
		logminer_pfree(fname,0);
	if(directory)
		logminer_pfree(directory,0);
	fname = NULL;
	directory = NULL;
	return segno;
}

static TimeLineID
getXlogFiletimeline(XlogFile *xf)
{
	XLogSegNo				segno = 0;
	TimeLineID				timeline = 0;
	char					*fname = NULL;
	char					*directory = NULL;
	
	
	split_path_fname(xf->filepath, &directory, &fname);
	XLogFromFileName(fname, &timeline, &segno);
	if(fname)
		logminer_pfree(fname,0);
	if(directory)
		logminer_pfree(directory,0);
	fname = NULL;
	directory = NULL;
	return timeline;
}

static void
setMaxTimeLineId(TimeLineID tempid)
{
	PgDataDic *pdd = NULL;

	if(!DataDictionaryCache)
		return;
	pdd = (PgDataDic *)DataDictionaryCache;
	if(pdd->maxtl < tempid)
	{
		if(0 != pdd->maxtl)
			pdd->mutitimeline = true;
		pdd->maxtl = tempid;
	}
}

static TimeLineID
getMaxTimeLineId()
{
	PgDataDic *pdd = NULL;

	if(!DataDictionaryCache)
		return 0;
	
	pdd = (PgDataDic *)DataDictionaryCache;
	return pdd->maxtl;
}


void
checkXlogFileList()
{
	PgDataDic *pdd = NULL;
	
	if(!DataDictionaryCache)
		return;
	pdd = (PgDataDic *)DataDictionaryCache;

	if(pdd->mutitimeline)
		ereport(NOTICE,
				(errmsg("There are xlog files on multiple timeline, only the files on latest timeline are parsed.")));

}


static void*
insertXlogFileToList(XlogFile *xf)
{
	XlogFileList			xflhead = NULL;
	XLogSegNo				segnoCur = 0;
	XLogSegNo				segnoPtr = 0;
	TimeLineID				timelineCur = 0;
	TimeLineID				timelinePtr = 0;
	XlogFile				*xfPre = NULL;
	XlogFile				*xfPtr = NULL;
	
	xflhead = (XlogFileList)XlogfileListCache;
	xfPtr = xflhead;
	segnoCur = getXlogFilesegno(xf);
	timelineCur = getXlogFiletimeline(xf);
	setMaxTimeLineId(timelineCur);
	while(NULL != xfPtr)
	{
		segnoPtr = getXlogFilesegno(xfPtr);
		timelinePtr = getXlogFiletimeline(xfPtr);

		if(timelinePtr > timelineCur
			|| (timelinePtr == timelineCur && segnoPtr > segnoCur))
		{
			if(!xfPre)
			{
				/*add to head*/
				xf->next = xfPtr;
				XlogfileListCache = xf;
			}
			else
			{
				/*add to middle*/
				xfPre->next = xf;
				xf->next = xfPtr;
			}
			break;
		}
		else if(!xfPtr->next)
		{
			xfPtr->next = xf;
			break;
		}
		xfPre = xfPtr;
		xfPtr = xfPtr->next;
	}
}

static void 
addxlogfileToList(char *path)
{
	
	XlogFileList xflhead = NULL;
	XlogFile	 *xfl = NULL;
	TimeLineID	 timelineCur = 0;
	if(!XlogfileListCache)
	{
		xflhead = fillXlogFile(path);
		xflhead->tail = xflhead;
		XlogfileListCache = (char*)xflhead;
		timelineCur = getXlogFiletimeline(xflhead);
		setMaxTimeLineId(timelineCur);
	}
	else
	{
		xflhead = (XlogFileList)XlogfileListCache;
		xfl = fillXlogFile(path);
		insertXlogFileToList(xfl);
	}
}
static void
removexlogfileFromList(XlogFile *removexlogfile)
{
	XlogFileList xflhead = NULL;
	XlogFile	 *xfptr_prv = NULL;
	XlogFile	 *xfptr_cur = NULL;

	if(!XlogfileListCache)
		return;
	xflhead = (XlogFileList)XlogfileListCache;
	xfptr_cur = xflhead;

	while(xfptr_cur)
	{
		if(removexlogfile == xfptr_cur)
		{
			if(xfptr_cur == xflhead)
			{
				/*remove the first xlogfile*/
				xflhead = xfptr_cur->next;
				XlogfileListCache = (char*)xflhead;
				
			}
			else
			{
				xfptr_prv->next = xfptr_cur->next;
			}
			logminer_free(xfptr_cur,0);
			xfptr_cur = NULL;
			return;
		}
		xfptr_prv = xfptr_cur;
		xfptr_cur = xfptr_cur->next;
	}
	
}

void
cleanXlogfileList()
{
	XlogFileList xflhead = NULL;
	XlogFile	*listNext = NULL;
	XlogFile	*listPtr = NULL;
	if(!XlogfileListCache)
		return;
	xflhead = (XlogFileList)XlogfileListCache;
	listPtr = xflhead;

	while(NULL != listPtr)
	{
		listNext = listPtr->next;
		if(listPtr)
			logminer_free(listPtr,0);
		listPtr = listNext;
	}
	XlogfileListCache = NULL;
}

int
getXlogFileNum()
{

	XlogFileList	xflhead = NULL;
	XlogFile		*listPtr = NULL;
	int				xlogfilecount = 0;

	if(!XlogfileListCache)
		return 0;

	xflhead = (XlogFileList)XlogfileListCache;
	listPtr = xflhead;
	
	while(NULL != listPtr)
	{
		xlogfilecount++;
		listPtr = listPtr->next;
	}
	return xlogfilecount;
}

static void 
outSigSysTableDictionary(Oid reloid, char* relname,int datasgsize, SysDataCache *sdc)
{
	bool				result = false;
	Oid					oid_temp = 0;
	Relation			pgrel = NULL;
	HeapScanDesc		scan = NULL;
	HeapTuple			tuple = NULL;
	Form_pg_class		classForm = NULL;
	char				*curdata = NULL;
	
	pgrel = heap_open(reloid, AccessShareLock);
	if(!pgrel)
		ereport(ERROR,(errmsg("Open relation(oid:%d) failed",reloid)));
	scan = heap_beginscan_catalog(pgrel, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		adddatatocache(sdc, tuple, datasgsize);
	}

	builddata(sdc, relname);
	heap_endscan(scan);
	heap_close(pgrel, AccessShareLock);
	return result;

}


char*
outputSysTableDictionary(char *path, SysClassLevel *scl, bool self)
{
	int				loop = 0;
	int				pathkind = 0;
	Oid				reloid = 0;
	char			*relname = NULL;
	SysDataCache	sdc;
	char			*dbname_cur = NULL;
	int				dicloadtype = PG_LOGMINER_DICTIONARY_LOADTYPE_OTHER;
	

	paddingscl(scl);
	pathkind = pathcheck(path);
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_INVALID == pathkind)
	{
		if(isEmptStr(path))
		{
			ereport(ERROR, (errmsg("Please enter a file path or directory.")));
		}
		else	
			ereport(ERROR,(errmsg("File or directory \"%s\" access is denied or does not exists.",path)));
	}
	dbname_cur = get_database_name(MyDatabaseId);
	memset(dictionary_path, 0, MAXPGPATH);
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_NULL == pathkind)
	{
		if(!self)
			ereport(ERROR,(errmsg("Please enter a file path or directory.")));
		snprintf(dictionary_path, MAXPGPATH, "%s/%s",
					 DataDir, LOGMINERDIR);
		if(!create_dir(dictionary_path))
			ereport(ERROR,(errmsg("It is failed to create dictionary \"%s\".",dictionary_path)));

		memset(dictionary_path,0,MAXPGPATH);
		snprintf(dictionary_path, MAXPGPATH, "%s/%s/%s",DataDir, LOGMINERDIR,LOGMINERDIR_DIC);
		if(!create_dir(dictionary_path))
			ereport(ERROR,(errmsg("It is failed to create dictionary \"%s\".",dictionary_path)));
		
		memset(dictionary_path,0,MAXPGPATH);
		snprintf(dictionary_path, MAXPGPATH, "%s/%s/%s/%s_%s",
					 DataDir, LOGMINERDIR, LOGMINERDIR_DIC, dbname_cur, PG_LOGMINER_DICTIONARY_DEFAULTNAME);
		dicloadtype = PG_LOGMINER_DICTIONARY_LOADTYPE_SELF;
		if(is_file_exist(dictionary_path))
		{
			remove(dictionary_path);
		}
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_SINGLE == pathkind)
	{
		snprintf(dictionary_path, MAXPGPATH, "%s/%s",DataDir, LOGMINERDIR);
		if(!create_dir(dictionary_path))
			ereport(ERROR,(errmsg("It is failed to create dictionary \"%s\".",dictionary_path)));
		memset(dictionary_path,0,MAXPGPATH);
		snprintf(dictionary_path, MAXPGPATH, "%s/%s/%s", DataDir, LOGMINERDIR, path);
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_FILE == pathkind)
		snprintf(dictionary_path, MAXPGPATH, "%s",path);
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_DIR == pathkind)
		snprintf(dictionary_path, MAXPGPATH, "%s/%s_%s", path, dbname_cur,PG_LOGMINER_DICTIONARY_DEFAULTNAME);

	if(is_file_exist(dictionary_path))
		ereport(ERROR,(errmsg("Dictionary file \"%s\" is already exist.",dictionary_path)));
	buildsysid(dicloadtype);
	initcache(&sdc);
	for(loop = 0; loop < PG_LOGMINER_IMPTSYSCLASS_IMPTNUM; loop++)
	{
		
		relname = scl[loop].relname;

		getRelationOidByName(relname, &reloid, false);
		outSigSysTableDictionary(reloid, relname, scl[loop].datasgsize, &sdc);
		cleancache(&sdc);
	}
	freecache(&sdc);
	return dictionary_path;
}

static void
loadSigSysTableDictionary(FILE *fp, int tabid, SysDataCache *sdc, int sigsize)
{
	DataDicHead ddh;
	memset(&ddh, 0, sizeof(DataDicHead));

	if(!fp || !sdc)
		return;
	
	loadhead(fp,&ddh);
	loaddata(fp, sdc, &ddh, sigsize);
}

void
loadSystableDictionary(char *path, SysClassLevel *scl, bool self)
{

	int			pathkind = 0;
	int			loop = 0;
	PgDataDic*	pgddc = NULL;
	FILE		*fp = NULL;
	
	paddingscl(scl);
	pathkind = pathcheck(path);
	
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_INVALID == pathkind)
	{
		if(isEmptStr(path))
		{
			ereport(ERROR,(errmsg("Please enter a file path or directory.")));
		}
		else
			ereport(ERROR,(errmsg(" File or directory \"%s\" access is denied or does not exists.",path)));
	}
	
	memset(dictionary_path, 0, MAXPGPATH);
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_NULL == pathkind)
	{
		if(!self)
			ereport(ERROR,(errmsg("Please enter a file path or directory.")));
		else			
		{
			char*	dbname_cur = NULL;
			dbname_cur = get_database_name(MyDatabaseId);
			snprintf(dictionary_path, MAXPGPATH, "%s/%s/%s/%s_%s",
						 DataDir, LOGMINERDIR, LOGMINERDIR_DIC,dbname_cur, PG_LOGMINER_DICTIONARY_DEFAULTNAME);
		}
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_SINGLE == pathkind)
	{
		snprintf(dictionary_path, MAXPGPATH, "%s/%s/%s", DataDir, LOGMINERDIR, path);
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_FILE == pathkind)
	{
		snprintf(dictionary_path, MAXPGPATH, "%s",path);
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_DIR == pathkind)
	{
		NameData	datafilename;
		NameData	*filenamePtr = NULL;
		int		filenum = 0;

		memset(&datafilename, 0, sizeof(NameData));
		filenamePtr = &datafilename;
		filenum = scanDir_getfilename(path, filenamePtr, true);
		if(0 == filenum)
			ereport(ERROR,(errmsg("There is no file in the directory  \"%s\".",path)));
		else if(1 < filenum)
			ereport(ERROR,(errmsg("Only one file is allowed in dir \"%s\".",path)));
		snprintf(dictionary_path, MAXPGPATH, "%s/%s", path, datafilename.data);
	}
	if(!is_file_exist(dictionary_path))
	{
		ereport(ERROR,(errmsg("File or directory \"%s\" access is denied or does not exists.",dictionary_path)));
	}
	pgddc = initDataDicMemory();
	
	fp = fopen(dictionary_path,"rb");
	if(!fp)
		ereport(ERROR,(errmsg("File or directory \"%s\" access is denied or does not exists.",dictionary_path)));
	loadsysid(fp, pgddc);
	for(loop = 0; loop < PG_LOGMINER_IMPTSYSCLASS_IMPTNUM; loop++)
	{
		loadSigSysTableDictionary(fp, loop, &pgddc->sdc[loop],scl[loop].datasgsize);
	}
	fclose(fp);
}

int
removexlogfile(char *path)
{
	int			pathkind = 0;
	int			filenum = 0;
	int			loop = 0;
	int			removenum = 0;
	XlogFile	*xftemp = NULL;
	XlogFile	*xfptr = NULL;
	XlogFileList xfl = NULL;
	char		*filedir = NULL, *filename = NULL;
	TimeLineID	timeline = 0;
	XLogSegNo	segno = 0;

	if(0 == strcmp("",path))
		ereport(ERROR,(errmsg("Please enter a file path or directory.")));

	if(PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SAME == checkXlogFileRepeat(path,&xftemp))
	{
		removenum++;
		removexlogfileFromList(xftemp);
		return removenum;
	}

	xfl = (XlogFileList)XlogfileListCache;
	xfptr = xfl;
	while(xfptr)
	{
		split_path_fname(xfptr->filepath,&filedir,&filename);
		fixPathEnd(path);
		
		if(0 == strcmp(path, filedir))
		{
			xftemp = xfptr;
			xfptr = xfptr->next;
			removexlogfileFromList(xftemp);
			removenum++;
		}
		else
			xfptr = xfptr->next;
	}
	return removenum;
}


int
addxlogfile(char *path)
{
	int			pathkind = 0;
	int			filenum = 0,validnum = 0;
	int 		loop = 0;
	NameData	*filenamelist = NULL;
	XlogFile	*xftemp = NULL;
	pathkind = pathcheck(path);
	
	
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_INVALID == pathkind)
	{
		if(isEmptStr(path))
		{
			ereport(ERROR,(errmsg("Please enter a file path or directory.")));
		}
		else
			ereport(ERROR,(errmsg("File or directory \"%s\" access is denied or does not exists.",path)));
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_NULL == pathkind)
		ereport(ERROR,(errmsg("Please enter a file path or directory.",path)));

	memset(dictionary_path, 0, MAXPGPATH);
	if(PG_LOGMINER_DICTIONARY_PATHCHECK_FILE == pathkind || PG_LOGMINER_DICTIONARY_PATHCHECK_SINGLE == pathkind)
	{
		snprintf(dictionary_path, MAXPGPATH, "%s",path);
		if(!is_file_exist(dictionary_path) && (PG_LOGMINER_DICTIONARY_PATHCHECK_FILE == pathkind))
			ereport(ERROR,(errmsg("File or directory \"%s\" access is denied or does not exists.",path)));
		filenum = 1;
	}
	else if(PG_LOGMINER_DICTIONARY_PATHCHECK_DIR == pathkind)
	{
		filenum = scanDir_getfilenum(path);
		if(0 < filenum)
		{
			filenamelist = (NameData *)palloc0(filenum * sizeof(NameData));
			scanDir_getfilename(path, filenamelist,false);
			/*for code Simplify. Make process same with "FILE == pathkind" while filenum was 1*/
			if(1 == filenum)
				snprintf(dictionary_path, MAXPGPATH, "%s/%s",path,filenamelist[0].data);
		}
	}

	/*check all file valid add add to list*/	
	for(loop = 0; loop < filenum; loop++)
	{
		if(1 != filenum)
		{
			memset(dictionary_path, 0, MAXPGPATH);
			snprintf(dictionary_path, MAXPGPATH, "%s/%s",path,filenamelist[loop].data);
		}
		if(checkXlogFileValid(dictionary_path,pathkind))
		{
			if(PG_LOGMINER_XLOGFILE_REPEAT_CHECK_DIFFERENT == checkXlogFileRepeat(dictionary_path,&xftemp))
			{
				validnum++;
				addxlogfileToList(dictionary_path);
			}
			else if(PG_LOGMINER_XLOGFILE_REPEAT_CHECK_SEGSAME == checkXlogFileRepeat(dictionary_path,&xftemp))
			{
				ereport(NOTICE,(errmsg(" \"%s\" is conflicted with file \"%s\" ",dictionary_path,xftemp->filepath)));
			}
		}
	}
	if(filenamelist)
		pfree(filenamelist);
	return validnum;
}

bool
loadXlogfileList()
{
	FILE	*fp = NULL;
	char	temp_path[MAXPGPATH] = {0};
	bool	getdata = true;
	int		getsize = 0;
	bool	getfile = false;
	
	snprintf(xlogfilelist_path, MAXPGPATH, "%s/%s/%d_%s",
						 DataDir, LOGMINERDIR, MyProcPid, PG_LOGMINER_DICTIONARY_XLOGFILELIST);

	fp = fopen(xlogfilelist_path, "r");
	if(!fp)
		return;
	
	while(getdata)
	{
		getsize = fread(temp_path, MAXPGPATH, 1, fp);
		if(0 == getsize)
			getdata = false;
		else
		{
			addxlogfileToList(temp_path);
			getfile = true;
		}
	}

	if(fp)
		fclose(fp);
	return getfile;
}

bool
loadDicStorePath(char *dicstorepath)
{
	FILE	*fp = NULL;
	char	dicstore_path[MAXPGPATH] = {0};
	
	snprintf(dicstore_path, MAXPGPATH, "%s/%s/%d_%s",
						 DataDir, LOGMINERDIR, MyProcPid, PG_LOGMINER_DICTIONARY_STOREPATH);
	fp = fopen(dicstore_path, "r");
	if(!fp)
		return false;
	fread(dicstorepath, MAXPGPATH, 1, fp);
	if(fp)
		fclose(fp);
	return true;

}


void
writeXlogfileList()
{
	FILE					*fp = NULL;
	XlogFileList			xflhead = NULL;
	XlogFile				*xfPre = NULL;
	XlogFile				*xfPtr = NULL;
	char					logminer_dir[MAXPGPATH] = {0};

	snprintf(xlogfilelist_path, MAXPGPATH, "%s/%s/%d_%s",
						 DataDir, LOGMINERDIR, MyProcPid, PG_LOGMINER_DICTIONARY_XLOGFILELIST);
	

	fp = fopen(xlogfilelist_path, "w");
	if(!fp)
	{

		snprintf(logminer_dir, MAXPGPATH, "%s/%s",
						 DataDir, LOGMINERDIR);
			if(!create_dir(logminer_dir))
				ereport(ERROR,
					   (errmsg("fail to create dir",logminer_dir)));
	}
	fp = fopen(xlogfilelist_path, "w");
	if(!fp)
		ereport(ERROR,
			(errmsg("can not open %s to write",xlogfilelist_path)));	
		
	xflhead = (XlogFileList)XlogfileListCache;
	xfPtr = xflhead;
	
	while(NULL != xfPtr)
	{
		fwrite(xfPtr->filepath, MAXPGPATH, 1, fp);
		xfPtr = xfPtr->next;
	}

	if(fp)
		fclose(fp);
}

void
writeDicStorePath(char* dicstorepath)
{
	FILE	*fp = NULL;
	char	dicstore_path[MAXPGPATH] = {0};
	char	logminer_dir[MAXPGPATH] = {0};
	
	snprintf(dicstore_path, MAXPGPATH, "%s/%s/%d_%s",
							 DataDir, LOGMINERDIR, MyProcPid, PG_LOGMINER_DICTIONARY_STOREPATH);
	if(is_file_exist(dicstore_path))
		ereport(ERROR,(errmsg("Dictionary has already been loaded.")));
	
	fp = fopen(dicstore_path, "w");
	if(!fp)
	{

		snprintf(logminer_dir, MAXPGPATH, "%s/%s",DataDir, LOGMINERDIR);
		if(!create_dir(logminer_dir))
			ereport(ERROR,(errmsg("fail to create dir",logminer_dir)));
	}
	fp = fopen(dicstore_path, "w");
	if(!fp)
		ereport(ERROR,
			(errmsg("can not open %s to write",dicstore_path)));		
	fwrite(dicstorepath, strlen(dicstorepath), 1, fp);
	fclose(fp);
}

void
dropAnalyseFile()
{
	char		temp[MAXPGPATH] = {0};
	NameData	*filenamelist = NULL;
	int			filenum = 0,i = 0;

	
	snprintf(temp, MAXPGPATH, "%s/%s/%s",DataDir, LOGMINERDIR, LOGMINERDIR_DIC);
	if(is_dir_exist(temp))
		dropAllFileInADir(temp);
	
	memset(temp,0,MAXPGPATH);
	snprintf(temp, MAXPGPATH, "%s/%s", DataDir, LOGMINERDIR);
	if(is_dir_exist(temp))
		dropAllFileInADir(temp);
}

char*
getNextXlogFile(char *fctx, bool show)
{
	logminer_fctx	*lfctx = NULL;
	XlogFile		*xf = NULL;
	char			*result = NULL;
	TimeLineID		maxtl = 0;
	char			*filedir = NULL,*filename = NULL;
	XlogFile		*xf_next = NULL;
	TimeLineID		timelinenext = 0;
	XLogSegNo		segnonext = 0;
	TimeLineID		timeline = 0;
	XLogSegNo		segno = 0;
	
	if(!fctx)
		return;
	lfctx = (logminer_fctx*)fctx;
	
	if(!lfctx->xlogfileptr)
	{
		lfctx->hasnextxlogfile = true;
		lfctx->xlogfileptr = XlogfileListCache;
	}
	else
	{
		if(!lfctx->hasnextxlogfile)
			return NULL;
	}
	
	xf = (XlogFile*)lfctx->xlogfileptr;
	result = xf->filepath;

	if(xf->next)
	{
		lfctx->xlogfileptr = xf->next;
	}
	else
		lfctx->hasnextxlogfile = false;
	split_path_fname(result,&filedir,&filename);
	XLogFromFileName(filename, &timeline, &segno);
	maxtl = getMaxTimeLineId();

	if(maxtl != timeline && !show)
		result = getNextXlogFile(fctx, show);

	return result;
}


static char*
logminer_getnext(int search_tabid,SysClassLevel *scl)
{
	PgDataDic *pdd = NULL;
	char	  *result = NULL;

	pdd = (PgDataDic *)DataDictionaryCache;

	if(pdd->sdc[search_tabid].curdata - pdd->sdc[search_tabid].data < pdd->sdc[search_tabid].usesize)
	{
		result = pdd->sdc[search_tabid].curdata;
		pdd->sdc[search_tabid].curdata += scl[search_tabid].datasgsize;
	}	
	return result;
}


static char*
logminer_getFormByOid(int search_tabid, Oid reloid)
{
	PgDataDic 			*pdd = NULL;
	char				*fpg = NULL;
	char 				*serchPtr = NULL;
	char				*result = NULL;
	int					*oidPtr = NULL;
	SysClassLevel 		*scl = NULL;

	scl = getImportantSysClass();
	
	pdd = (PgDataDic *)DataDictionaryCache;

	while(NULL != (serchPtr = logminer_getnext(search_tabid,scl)))
	{
		fpg = serchPtr + sizeof(Oid);
		oidPtr = serchPtr;
		if(reloid == *oidPtr)
		{
			result = fpg;
			break;
		}
	}
	pdd->sdc[search_tabid].curdata = pdd->sdc[search_tabid].data;
	return result;
}

void 
searchSysClass( SystemClass *sys_class,int	*sys_classNum)
{
	PgDataDic 			*pdd = NULL;
	Form_pg_class		fpc = NULL;
	char 				*serchPtr = NULL;
	char				*result = NULL;
	SysClassLevel 		*scl = NULL;
	int					search_tabid = 0;
	int					syscount = 0;
	char				*relname_source = NULL;
	char				*relname_opt = NULL;

	scl = getImportantSysClass();
	pdd = (PgDataDic *)DataDictionaryCache;
	search_tabid = PG_LOGMINER_IMPTSYSCLASS_PGCLASS;
	
	while(NULL != (serchPtr = logminer_getnext(search_tabid,scl)))
	{
		fpc = (Form_pg_class)(serchPtr + sizeof(Oid));
		if (fpc->relnamespace == PG_LOGMINER_OID_PGCATALOG &&
			fpc->relkind == RELKIND_RELATION)
		{
			relname_source = fpc->relname.data;
			relname_opt = sys_class[syscount].classname.data;
			memcpy(relname_opt, relname_source, sizeof(NameData));
			sys_class[syscount].attnum = fpc->relnatts;
			syscount++;
		}
	}
	*sys_classNum = syscount;
	pdd->sdc[search_tabid].curdata = pdd->sdc[search_tabid].data;
}


char*
getdbNameByoid(Oid dboid, bool createdb)
{
	Form_pg_database	fpc = NULL;
	char				*result = NULL;

	if(0 == dboid)
		return NULL;

	fpc = (Form_pg_database)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGDATABASE, dboid);
	if(!fpc && !createdb)
		return NULL;
	else
		result = fpc->datname.data;
	return result;
}


char*
gettbsNameByoid(Oid tbsoid)
{
	Form_pg_tablespace	fpt = NULL;
	char				*result = NULL;

	if(0 == tbsoid)
		tbsoid = 1663;
	fpt = (Form_pg_database)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGTABLESPACE, tbsoid);
	if(fpt)
		result = fpt->spcname.data;
	return result;
}

Oid
gettuserOidByReloid(Oid reloid)
{
	Form_pg_class	fpc = NULL;
	Oid				result = 0;

	fpc = (Form_pg_class)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGCLASS, reloid);
	if(fpc)
	{
		result = fpc->relowner;
	}
	return result;
}

char*
getuserNameByUseroid(Oid useroid)
{
	Form_pg_authid	fpa = NULL;
	char			*result = NULL;
	if(0 == useroid)
		return NULL;

	fpa = (Form_pg_authid)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGAUTHID, useroid);
	if(fpa)
		result = fpa->rolname.data;
	return result;
}

Oid
getnsoidByReloid(Oid reloid)
{
	Form_pg_class	fpc = NULL;
	Oid				result = 0;

	fpc = (Form_pg_class)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGCLASS, reloid);
	if(fpc)
		result = fpc->relnamespace;
	return result;
}

char*
getnsNameByOid(Oid schoid)
{

	Form_pg_namespace	fpn = NULL;
	char				*result = NULL;

	if(0 == schoid)
		return NULL;
	
	fpn = (Form_pg_namespace)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGNAMESPACE, schoid);
	if(fpn)
		result = fpn->nspname.data;
	return result;
}

char*
getnsNameByReloid(Oid reloid)
{
	Oid		nsoid = 0;
	char	*result = NULL;
	
	nsoid = getnsoidByReloid(reloid);
	result = getnsNameByOid(nsoid);
	return result;
}

int
getRelationNameByOid(Oid reloid, NameData* relname)
{
	Form_pg_class	fpc = NULL;
	int			result = 0;

	fpc = (Form_pg_class)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGCLASS, reloid);
	if(fpc)
	{
		if(RELKIND_RELATION == fpc->relkind || RELKIND_TOASTVALUE == fpc->relkind)
		{
			/*get a result*/
			memcpy(relname->data, fpc->relname.data, strlen(fpc->relname.data));
			result = 1;
		}
		else
			/*current relation not a table*/
			result = -1;
	}
	return result;
}

Oid
getRelationOidByRelfileid(Oid relNodeid)
{
	PgDataDic 			*pdd = NULL;
	Form_pg_class		fpc = NULL;
	char 				*serchPtr = NULL;
	Oid					result = 0;
	int					*oidPtr = NULL;
	SysClassLevel 		*scl = NULL;
	Oid					search_tabid = PG_LOGMINER_IMPTSYSCLASS_PGCLASS;

	scl = getImportantSysClass();
	
	pdd = (PgDataDic *)DataDictionaryCache;

	while(NULL != (serchPtr = logminer_getnext(search_tabid ,scl)))
	{
		fpc = serchPtr + sizeof(Oid);
		oidPtr = serchPtr;
		if(relNodeid == fpc->relfilenode)
		{
			result = *oidPtr;
			break;
		}
	}
	pdd->sdc[search_tabid].curdata = pdd->sdc[search_tabid].data;
	return result;
}

TupleDesc
GetDescrByreloid(Oid reloid)
{
	PgDataDic 			*pdd = NULL;
	TupleDesc			tupdesc = NULL;
	char 				*serchPtr = NULL;
	SysClassLevel 		*scl = NULL;
	Form_pg_attribute	fpa = NULL;
	Form_pg_class		fpc = NULL;
	int					relnatts = 0;
	int					atts_loop = 0;
	int					pg_class_tabid = 0;
	int					pg_attribute_tabid = 0;
	MemoryContext 		oldcxt = NULL;

	scl = getImportantSysClass();
	pdd = (PgDataDic *)DataDictionaryCache;

	pg_class_tabid = PG_LOGMINER_IMPTSYSCLASS_PGCLASS;
	pg_attribute_tabid = PG_LOGMINER_IMPTSYSCLASS_PGATTRIBUTE;

	fpc = (Form_pg_class)logminer_getFormByOid(pg_class_tabid, reloid);
	if(!fpc)
		/*should not happen ,checked before*/
		ereport(ERROR,(errmsg("Did not find reloid %d in dictionary",reloid)));
	else
	{
		relnatts = fpc->relnatts;
	}
	tupdesc = CreateTemplateTupleDesc(relnatts, false);
	while(NULL != (serchPtr = logminer_getnext(pg_attribute_tabid,scl)))
	{
		fpa = (Form_pg_attribute)(serchPtr + sizeof(Oid));
		if(reloid == fpa->attrelid && 0 < fpa->attnum)
		{
			memcpy(tupdesc->attrs[fpa->attnum - 1], fpa, ATTRIBUTE_FIXED_PART_SIZE);
			tupdesc->attrs[fpa->attnum - 1]->attnotnull = false;
			tupdesc->attrs[fpa->attnum - 1]->atthasdef = false;
			atts_loop++;
		}
	}
	pdd->sdc[pg_attribute_tabid].curdata = pdd->sdc[pg_attribute_tabid].data;
	return tupdesc;
}


void
freetupdesc(TupleDesc tupdesc)
{
	if(tupdesc)
		pfree(tupdesc);
}


bool
tableIftoastrel(Oid reloid)
{
	Form_pg_class		pgc = NULL;
	
	pgc =  (Form_pg_class)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGCLASS,reloid);
	if(!pgc)
	{
		return false;
	}
	if(RELKIND_TOASTVALUE == pgc->relkind)
		return true;
	return false;
}

bool
getTypeOutputFuncFromDic(Oid type, Oid *typOutput, bool *typIsVarlena)
{
	Form_pg_type	pt;
	pt = (Form_pg_type)logminer_getFormByOid(PG_LOGMINER_IMPTSYSCLASS_PGTYPE, type);
	if(!pt)
		return false;

	*typOutput = pt->typoutput;
	*typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);
	return true;
}

