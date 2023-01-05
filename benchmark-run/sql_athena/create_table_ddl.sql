CREATE TABLE region_:database_id:(r_regionkey bigint, r_name string, r_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/region' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE nation_:database_id:(n_nationkey bigint, n_name string, n_regionkey bigint, n_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/nation' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE customer_:database_id:(c_custkey bigint, c_name string, c_address string, c_nationkey bigint, c_phone string, c_acctbal decimal(12,2), c_mktsegment string, c_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/customer' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE lineitem_:database_id:(l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag string, l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/lineitem' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE orders_:database_id:(o_orderkey bigint, o_custkey bigint, o_orderstatus string, o_totalprice decimal(12,2), o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority bigint, o_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/orders' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE part_:database_id:(p_partkey bigint, p_name string, p_mfgr string, p_brand string, p_type string, p_size bigint, p_container string, p_retailprice decimal(12,2), p_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/part' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE partsupp_:database_id:(ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/partsupp' TBLPROPERTIES ('table_type'='ICEBERG');
:split:
CREATE TABLE supplier_:database_id:(s_suppkey bigint, s_name string, s_address string, s_nationkey bigint, s_phone string, s_acctbal decimal(12,2), s_comment string) LOCATION 's3://[TODO s3 bucket]/database_:database_id:/supplier' TBLPROPERTIES ('table_type'='ICEBERG');