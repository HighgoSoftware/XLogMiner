/*-------------------------------------------------------------------------
 *
 * xlogminer_contents.h
 *
 * contrib/xlogminer/xlogminer_contents.h
 *
 *-------------------------------------------------------------------------
*/

#ifndef PG_XLOGMINER_CONTENTS_H
#define PG_XLOGMINER_CONTENTS_H
#include "postgres.h"

#define	PG_XLOGMINER_CONTENTS_SPACE_ADDSTEP			100
#define	PG_XLOGMINER_CONTENTS_STEPCOMMIT_STEP		1000


typedef struct FormData_xlogminer_contents
{
	uint32			sqlno;
	uint32			xid;	
	int				virtualxid;
	TimestampTz		timestamp;
	char*			record_database;
	char*			record_user;
	char*			record_tablespace;
	char*			record_schema;
	char*			op_type;
	char*			op_text;
	char*			op_undo;
}FormData_xlogminer_contents;
typedef FormData_xlogminer_contents *Form_xlogminer_contents;



#define Natts_xlogminer_contents									10
#define Anum_xlogminer_contents_xid									1
#define Anum_xlogminer_contents_virtualxid							2
#define Anum_xlogminer_contents_timestamp							3
#define Anum_xlogminer_contents_record_database						4
#define Anum_xlogminer_contents_record_user							5
#define Anum_xlogminer_contents_record_tablespace					6
#define Anum_xlogminer_contents_record_schema						7
#define Anum_xlogminer_contents_op_type								8
#define Anum_xlogminer_contents_op_text								9
#define Anum_xlogminer_contents_op_undo								10

/*
 * functions prototype
 */
void InsertXlogContentsTuple(Form_xlogminer_contents fxc);
void UpdateXlogContentsTuple(Form_xlogminer_contents fxc);
void addSQLspace(void);
void cleanSQLspace(void);
void freeSQLspace(void);



#endif
