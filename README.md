XLogMiner
=====
an Open-Source SQL miner on PostgreSQL WAL log

# What is XLogMiner
XLogMiner is used for parsing the SQL statement out from the PostgreSQL WAL (write ahead logs) logs, and it can also generate the corresponding "undo SQL".

# Configurations requirements

You need configure the WAL level to "logical", and setup the table to "full" mode. For example, the blow statement is setting the table "t1" to "full" mode.

```sql
	alter table t1 replica identity FULL;
```

# User Guide
## Scenario I: Execute the parsing in the owner database of the WAL files
### 1. Create extension xlogminer
	create extension xlogminer;

### 2. Add target WAL files
```sql
	-- Add WAL file or directory
	select xlogminer_xlogfile_add('/opt/test/wal');
	-- Note: the parameter can be file name or directory name.
```

### 3. Remove WAL files
```sql
	-- remove WAL file or directory
	select xlogminer_xlogfile_remove('/opt/test/wal');
	-- Note: the parameter can be file name or directory name.
```

### 4. List WAL files
```sql
	-- List WAL files
	select xlogminer_xlogfile_list();
```

### 5. Execute the xlogminer
```sql
	select xlogminer_start(’START_TIMSTAMP’,’STOP_TIMESTAMP’,’START_XID’,’STOP_XID’)
```
* **START_TIMESTAMP**：Specify the start time condition of the records in the return results, the xlogminer will start parsing from this time value. If this value is NULL, the earlist records will be displayed from the WAL lists. If this time value is not included in the xlog lists, a.k.a all the records are ealier then this value, the NULL will be returned.
* **STOP_TIMESTAMP**：Specify the ending time condition of the records in the results, the xlogminer will stop parsing when the result is later than this time. If this parameter is NULL, then all the records after **START_TIMESTAMP** will parsed and displyed in the WAL logs.
* **START_XID**：Similiar with **START_TIMESTAMP**, specify the starting **XID** value
* **STOP_XID**：Similiar with **STOP_TIMESTAMP**，specify the ending **XID** value	

:warning: **Only one of these two group parameters can be provided, or error will be retured**
	
### 6. Check the parsing result
```sql
	select * from xlogminer_contents;
```

### 7. Stop the xlogminer
This function is used to free the memory and stop the WAL parsing. No parameters available.
```sql
	select xlogminer_stop();
```


## Scenario II: Parsing the WAL logs from the database which is not the owner of these WAL logs
:warning: It is required to have the same version of the target PostgreSQL database and the source database.

### On production database

#### 1. Create xlogminer extension
```sql
	create extension xlogminer;
```
	
#### 2. Build the dictionary
```sql
	select xlogminer_build_dictionary('/opt/proc/store_dictionary');
	-- Note: the parameter can be file name or directory name.
```


### On testing database

#### 1. Create xlogminer extension
```sql
	create extension xlogminer;
```

#### 2. Load database dictionary
```sql
	select xlogminer_load_dictionary('/opt/test/store_dictionary');
	-- Note: the parameter can be file name or directory name.
```
:bulb:	the parameter can be file name or directory name.
	
#### 3. Add WAL files
```sql
	-- Add WAL files
	select xlogminer_xlogfile_add('/opt/test/wal');
	-- Note: the parameter can be file name or directory name.
```

#### 4. remove xlog WAL files
```sql
	-- Remove WAL files
	select xlogminer_xlogfile_remove('/opt/test/wal');
	-- Note:the parameter can be file name or directory name.
```
#### 5. List xlog WAL files	
```sql
	-- list WAL files
	select xlogminer_xlogfile_list();
	-- Note:the parameter can be file name or directory name.
```

	
#### 6. Execute the parsing
```sql
	select xlogminer_start(’START_TIMSTAMP’,’STOP_TIMESTAMP’,’START_XID’,’STOP_XID’)
```

* **START_TIMESTAMP**：Specify the start time condition of the records in the return results, the xlogminer will start parsing from this time value. If this value is NULL, the earlist records will be displayed from the WAL lists. If this time value is not included in the xlog lists, a.k.a all the records are ealier then this value, the NULL will be returned.
* **STOP_TIMESTAMP**：Specify the ending time condition of the records in the results, the xlogminer will stop parsing when the result is later than this time. If this parameter is NULL, then all the records after **START_TIMESTAMP** will parsed and displyed in the WAL logs.
* **START_XID**：Similiar with **START_TIMESTAMP**, specify the starting **XID** value
* **STOP_XID**：Similiar with **STOP_TIMESTAMP**，specify the ending **XID** value	

#### 7. Check the parsing result
```sql
	select * from xlogminer_contents;
```

### 8.Stop the xlogminer
This function is used to free the memory and stop the WAL parsing. No parameters available.
```sql
	select xlogminer_stop();
```

:warning: **注意**：For security consideration, xlogminer_contents is a temporary table generated by xlogminer automatically, it is not visible when session disconnected and then re-connect, and it is also not visible to other sessions. 
     If you want to keep the paring result, you can use the below SQL string to write the results to a regular table.
```sql	 
create xxx as select * from  xlogminer_contents;
```

# Limitations
1. Only DML statements will be parsed in this version, DDL statement not supported.
2. The DML statemes would be parsed out when the below DDL related operations were executed:
   Deleting/Truncating table, table space modification and column type modification etcs.
3. The parsing result is depending on the latest database dictionary. For example, after user1 created table t1, the table owner was modified to user2, then all the parsing results related to table t1 will be marked with user2.
4. The SQL statements happened would be parsed out, if corresponding WAL logs are missing at that time.
5. The "ctid" attribute is the value of the change "at that time". If there are "ctid" changes due to vacuum and other operations, this value will be inaccurate. For the possibility of duplicate rows of data, we need to use this value to determine the corresponding undo tuple number, it does not mean that you can execute the undo statement directly.
6. If the table is not set to full mode, then the "update" and "delete" statement will not be resolved. (Of course, this affects the use of this software, the next version will make improvements to this problem）
7. If the database log level is not set to logical, there will be unpredictable lost of the SQL statements