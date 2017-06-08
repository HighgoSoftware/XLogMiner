XLogMiner
=====

# 什么是XLogMiner
XLogMiner是从PostgreSQL的WAL(write ahead logs)日志中解析出执行的SQL语句的工具，并能生成出对应的undo SQL语句。

# 配置要求
## 需要将数据库日志级别配置为logical模式, 并将表设置为full模式。例如，下面的语句将表t1设置为full模式:

```sql
alter table t1 replica identity FULL;
```

## PG版本支持
目前主要是在PostgreSQL 9.5.x上开发测试的，在9.6版本中仅做过简单验证，如果使用过程中发现问题欢迎向我们反馈。

# 编译安装
1. 将xlogminer目录放置到编译通过的PG工程的"../contrib/"目录下
2. 进入xlogminer目录
3. 执行make;make install;

# 使用方法
## 场景一：从WAL日志产生的数据库中直接执行解析
### 1. 创建xlogminer的extension
	create extension xlogminer;

### 2. Add xlog日志文件
```sql
-- 增加wal文件：
select xlogminer_xlogfile_add('/opt/test/wal');
-- 注：参数可以为目录或者文件
```

### 3. Remove xlog日志文件
```sql
-- 移除wal文件：
select xlogminer_xlogfile_remove('/opt/test/wal');
-- 注：参数可以为目录或者文件
```

### 4. List xlog日志文件
```sql
-- 列出wal文件：
select xlogminer_xlogfile_list();
```

### 5. 执行解析
```sql
select xlogminer_start(’START_TIMSTAMP’,’STOP_TIMESTAMP’,’START_XID’,’STOP_XID’)
---如果分析全部日志：
select xlogminer_start('null','null',0,0);
```

* **START_TIMESTAMP**：指定输出结果中最早的记录条目，即从该时间开始输出分析数据；若该参数值为空，则以分析日志列表中最早数据开始输出；若该参数值指定时间没有包含在所分析xlog列表中，即通过分析发现全部早于该参数指定时间，则返回空值。	
* **STOP_TIMESTAMP**：指定数据结果中最晚的记录条目，即输出结果如果大于该时间，则停止分析，不需要继续输出；如果该参数值为空，则从**START_TIMESTAMP**开始的所有日志都进行分析和输出。	
* **START_XID**：作用与**START_TIMESTAMP**相同，指定开始的**XID**值；	
* **STOP_XID**：作用与**STOP_TIMESTAMP**相同，指定结束的**XID**值	

:warning: **两组参数只能有一组为有效输入，否则报错。**



### 6. 解析结果查看
```sql
select * from xlogminer_contents;
```

### 7. 结束xlogminer操作
该函数作用为释放内存，结束日志分析，该函数没有参数。
```sql
select xlogminer_stop();
```


## 场景二：从非WAL产生的数据库中执行WAL日志解析
:warning: 要求执行解析的PostgreSQL数据库和被解析的为同一版本

### 于生产数据库

#### 1.创建xlogminer的extension
```sql
create extension xlogminer;
```
	
#### 2.生成数据字典
```sql
select xlogminer_build_dictionary('/opt/proc/store_dictionary');
-- 注：参数可以为目录或者文件
```


### 于测试数据库

#### 1. 创建xlogminer的extension
```sql
create extension xlogminer;
```

#### 2. load数据字典
```sql
select xlogminer_load_dictionary('/opt/test/store_dictionary');
-- 注：参数可以为目录或者文件
```
	
#### 3. add xlog日志文件
```sql
-- 增加wal文件：
select xlogminer_xlogfile_add('/opt/test/wal');
-- 注：参数可以为目录或者文件
```

#### 4. remove xlog日志文件
```sql
-- 移除wal文件：
select xlogminer_xlogfile_remove('/opt/test/wal');
-- 注：参数可以为目录或者文件
```
#### 5. list xlog日志文件	
```sql
-- 列出wal文件：
select xlogminer_xlogfile_list();
-- 注：参数可以为目录或者文件
```

	
#### 6. 执行解析
```sql
select xlogminer_start(’START_TIMSTAMP’,’STOP_TIMESTAMP’,’START_XID’,’STOP_XID’)
```
* **START_TIMESTAMP**：指定输出结果中最早的记录条目，即从该时间开始输出分析数据；若该参数值为空，则以分析日志列表中最早数据开始输出；若该参数值指定时间没有包含在所分析xlog列表中，即通过分析发现全部早于该参数指定时间，则返回空值。	
* **STOP_TIMESTAMP**：指定数据结果中最晚的记录条目，即输出结果如果大于该时间，则停止分析，不需要继续输出；如果该参数值为空，则从START_TIMESTAMP开始的所有日志都进行分析和输出。	
* **START_XID**：作用与START_TIMESTAMP相同，指定开始的XID值；	
* **STOP_XID**：作用与STOP_TIMESTAMP相同，指定结束的XID值	
	两组参数只能有一组为有效输入，否则报错。	

#### 7. 解析结果查看
```sql
select * from xlogminer_contents;
```

### 8.结束xlogminer操作,该函数作用为释放内存，结束日志分析，该函数没有参数。
```sql
select xlogminer_stop();
```

:warning: **注意**：xlogminer_contents是xlogminer自动生成的临时表，因此当session断开再重新进入或其他session中解析数据不可见。这么做主要是基于安全考虑。
      如果希望保留解析结果，可利用create xxx as select * from  xlogminer_contents;写入普通表中。

# 使用限制
1. 本版本只解析DML语句，不处理DDL语句
2. 执行了删除表、truncate表、更改表的表空间、更改表字段的类型，这样的DDL语句后，发生DDL语句之前的此表相关的DML语句不会再被解析。
3. 解析结果依赖于最新的数据字典。（举例：创建表t1,所有者为user1，但是中间将所有者改为user2。那解析结果中，所有t1相关操作所有者都将标示为user2）
4. wal日志如果发生缺失，在缺失的wal日志中发生提交的数据，都不会在解析结果中出现
5. 解析结果中undo字段的ctid属性是发生变更“当时”的值，如果因为vacuum等操作导致ctid发生变更，这个值将不准确。对于有可能存在重复行的数据，我们需要通过这个值确定undo对应的tuple条数，不代表可以直接执行该undo语句。
6. 若没有将表设置为full模式，那么update、delete语句将无法被解析。（当然这很影响使用，下一版本就会对这个问题作出改进）
7. 若没有将数据库日志级别设置为logical，解析结果会有无法预料的语句丢失
8. 执行了表字段drop的DDL语句后,发生DDL语句之前的这个字段相关的值都会被解析为encode（'AD976BC56F'，hex）的形式，另外自定义类型也会解析为这种形式

# 联系我们
发现bug或者有好的建议可以通过邮箱（opensource@highgo.com）联系我们。
