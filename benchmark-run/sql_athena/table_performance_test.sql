
-- CSV table:     Time in queue: 0.186 sec; Run time: 2.321 sec; Data scanned: 724.66 MB

-- Iceberg table: Time in queue: 0.164 sec; Run time: 7.79 sec; Data scanned: 36.88 MB
CREATE TABLE cbench.lineitem_iceberg_0
(L_ORDERKEY bigint, L_PARTKEY bigint, L_SUPPKEY bigint, L_LINENUMBER bigint, L_QUANTITY DECIMAL(12,2), L_EXTENDEDPRICE DECIMAL(12,2), L_DISCOUNT DECIMAL(12,2), L_TAX DECIMAL(12,2), L_RETURNFLAG string, L_LINESTATUS string, L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT string, L_SHIPMODE string, L_COMMENT string)
LOCATION 's3://perf-test-bucket/iceberg-folder/lineitem/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'compaction_bin_pack_target_file_size_bytes'='536870912' 
);
insert into lineitem_iceberg_0 (select * from lineitem_0);

-- Iceberg table partitioned:
CREATE TABLE cbench.lineitem_iceberg_0
(L_ORDERKEY bigint, L_PARTKEY bigint, L_SUPPKEY bigint, L_LINENUMBER bigint, L_QUANTITY DECIMAL(12,2), L_EXTENDEDPRICE DECIMAL(12,2), L_DISCOUNT DECIMAL(12,2), L_TAX DECIMAL(12,2), L_RETURNFLAG string, L_LINESTATUS string, L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT string, L_SHIPMODE string, L_COMMENT string)
PARTITIONED BY (L_SHIPDATE)
LOCATION 's3://perf-test-bucket/iceberg-folder/lineitem/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'compaction_bin_pack_target_file_size_bytes'='536870912'
);
insert into lineitem_iceberg_0 (select * from lineitem_0);
... [ErrorCode: INTERNAL_ERROR_QUERY_ENGINE] Amazon Athena experienced an internal error while executing this query. Please contact AWS support for further assistance. You will not be charged for this query. We apologize for the inconvenience.

-- Parquet table SNAPPY: Time in queue: 0.129 sec; Run time: 2.418 sec; Data scanned: 74.32 MB
-- Parquet table ZSTD: Time in queue: 0.165 sec; Run time: 2.886 sec; Data scanned: 44.83 MB
-- Parquet table GZIP: Time in queue: 0.114 sec; Run time: 2.905 sec; Data scanned: 47.50 MB
-- Parquet table LZ4: Error reading dictionary page
CREATE TABLE cbench.lineitem_parquet_0
WITH (
      format = 'Parquet',
      write_compression = 'LZ4',
      external_location = 's3://perf-test-bucket/parquet-folder/lineitem/')
AS SELECT *
FROM lineitem_0;
