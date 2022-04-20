
# TPCH

This readme describes how to obtain the TPC-H numbers.
We use scale factor = 100.

## Read-only queries (Q1-Q22)

```sql
ALTER SESSION SET USE_CACHED_RESULT = FALSE ;
```
Simply insert and execute the query in the snowflake web interface.
The statistics can be obtained from the history tab in the query tree view.
To reduce manual labor the json file containing the tree can be downloaded, saved in ``data.json`` and then analyzed with ``analyzer.js``.

### Create new snowset copy
```sql
create table "BENCHMARK_DB"."TPCH100".CUSTOMER as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."CUSTOMER");
create table "BENCHMARK_DB"."TPCH100".LINEITEM as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."LINEITEM");
create table "BENCHMARK_DB"."TPCH100".NATION as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."NATION");
create table "BENCHMARK_DB"."TPCH100".ORDERS as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS");
create table "BENCHMARK_DB"."TPCH100".PART as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."PART");
create table "BENCHMARK_DB"."TPCH100".PARTSUPP as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."PARTSUPP");
create table "BENCHMARK_DB"."TPCH100".REGION as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."REGION");
create table "BENCHMARK_DB"."TPCH100".SUPPLIER as (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."SUPPLIER");
```

### Create data for refresh functions
- Run queries, then go to this history where the query tree is displayed, this page downloads a file with the statistics of the query .. this can be parsed to obtain the runtimes
- For update queries: generate them with the tpch dbgen tool
- Remove the trailing | .. either manually or:
```shell script
cat lineitem.tbl.u1 | awk '{print substr($0, 0, length($0) -1)}' > lineitem_no_trail.tbl.u1
cat orders.tbl.u1 | awk '{print substr($0, 0, length($0) -1)}' > orders_no_trail.tbl.u1
cat delete.1 | awk '{print substr($0, 0, length($0) -1)}' > delete_no_trail.tbl.u1
```

## Upload Dataset

- Connect to snowflake with ``snowsql -a [account_name]`` (install with ``brew``)
- `[account_name]` is the subdomain of the snowflake URL for your instance: `https://[account_name].snowflakecomputing.com`
- Username is the one from the webinterface (no mail) and usual password
```shell script
snowsql -a [account_name]
```
- List files: ``list @~;``
- Delete files: ``rm @~/staged/orders.tbl.u1.gz;``
```sql
put file:////tmp/tpch-dbgen/orders_no_trail.tbl.u1 @~/staged;
put file:////tmp/tpch-dbgen/lineitem_no_trail.tbl.u1 @~/staged;
put file:////tmp/tpch-dbgen/delete_no_trail.tbl.u1 @~/staged;
```

### Refresh Function 1
```sql
begin;
    copy into lineitem from @~/staged/lineitem_no_trail.tbl.u1.gz file_format = (type = csv field_delimiter = '|' skip_header = 0)  FORCE = TRUE;
    copy into orders from @~/staged/orders_no_trail.tbl.u1.gz file_format = (type = csv field_delimiter = '|' skip_header = 0)  FORCE = TRUE;
commit;
```

### Refresh Function 2
```sql
create table antiorders(o_orderkey NUMBER(38,0));
copy into antiorders from @~/staged/delete_no_trail.tbl.u1.gz file_format = (type = csv field_delimiter = '|' skip_header = 0)  FORCE = TRUE;
begin;
    delete from orders where o_orderkey in (select o_orderkey from antiorders);
    delete from lineitem where l_orderkey in (select o_orderkey from antiorders);
commit;
```
