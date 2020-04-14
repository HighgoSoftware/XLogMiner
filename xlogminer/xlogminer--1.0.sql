/*
 * Abstract:
 * Create function for xlogminer
 *
 * Authored by lichuancheng@highgo.com ,20170524
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Identification: 
 * xlogminer--1.0.sql 
 */
CREATE OR REPLACE FUNCTION pg_minerXlog(starttime text, endtime text, startxid int, endxid int)
RETURNS text AS
'MODULE_PATHNAME','pg_minerXlog'
LANGUAGE C CALLED ON NULL INPUT;

CREATE OR REPLACE FUNCTION xlogminer_temp_table_check()
RETURNS void AS
$BODY$
DECLARE
	rd "varchar";
	checksql "varchar";
	temptablename "varchar";
	tp "varchar";
BEGIN
	temptablename := 'xlogminer_contents';
	tp :='t';
	SELECT * into rd FROM pg_catalog.pg_class WHERE relname = 'xlogminer_contents' AND relpersistence = 't';
	IF FOUND THEN
		TRUNCATE TABLE xlogminer_contents;
	ELSE
		CREATE temp TABLE xlogminer_contents(xid bigint,virtualxid int,timestampTz timestampTz,record_database text, record_user text,record_tablespace text,record_schema text, op_type text,op_text text,op_undo text);
	END IF;
END;
$BODY$
LANGUAGE 'plpgsql' VOLATILE; 


CREATE OR REPLACE FUNCTION xlogminer_start(starttime text, endtime text, startxid int, endxid int)
RETURNS text AS 
$BODY$
	select xlogminer_temp_table_check();
	select pg_minerXlog($1,$2,$3,$4);
$BODY$
LANGUAGE 'sql';


CREATE OR REPLACE FUNCTION xlogminer_build_dictionary(in path text)
RETURNS text AS
'MODULE_PATHNAME','xlogminer_build_dictionary'
LANGUAGE C CALLED ON NULL INPUT;

CREATE OR REPLACE FUNCTION xlogminer_load_dictionary(in path text)
RETURNS text AS
'MODULE_PATHNAME','xlogminer_load_dictionary'
LANGUAGE C CALLED ON NULL INPUT;

CREATE OR REPLACE FUNCTION xlogminer_stop()
RETURNS text AS
'MODULE_PATHNAME','xlogminer_stop'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION xlogminer_xlogfile_add(in path text)
RETURNS text AS
'MODULE_PATHNAME','xlogminer_xlogfile_add'
LANGUAGE C CALLED ON NULL INPUT;

CREATE OR REPLACE FUNCTION xlogminer_xlogfile_remove(in path text)
RETURNS text AS
'MODULE_PATHNAME','xlogminer_xlogfile_remove'
LANGUAGE C CALLED ON NULL INPUT;

CREATE OR REPLACE FUNCTION xlogminer_xlogfile_list()
RETURNS setof record  AS
'MODULE_PATHNAME','xlogminer_xlogfile_list'
LANGUAGE C VOLATILE STRICT;

