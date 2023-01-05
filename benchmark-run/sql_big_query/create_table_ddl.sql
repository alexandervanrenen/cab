CREATE OR REPLACE TABLE :dataset_name:.region_:database_id:(r_regionkey bigint, r_name string(25), r_comment string(152));
:split:
CREATE OR REPLACE TABLE :dataset_name:.nation_:database_id:(n_nationkey bigint, n_name string(25), n_regionkey bigint, n_comment string(152));
:split:
CREATE OR REPLACE TABLE :dataset_name:.customer_:database_id:(c_custkey bigint, c_name string(25), c_address string(40), c_nationkey bigint, c_phone string(15), c_acctbal decimal(12,2), c_mktsegment string(10), c_comment string(117));
:split:
CREATE OR REPLACE TABLE :dataset_name:.lineitem_:database_id:(l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag string(1), l_linestatus string(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string(25), l_shipmode string(10), l_comment string(44));
:split:
CREATE OR REPLACE TABLE :dataset_name:.orders_:database_id:(o_orderkey bigint, o_custkey bigint, o_orderstatus string(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority string(15), o_clerk string(15), o_shippriority bigint, o_comment string(79));
:split:
CREATE OR REPLACE TABLE :dataset_name:.part_:database_id:(p_partkey bigint, p_name string(55), p_mfgr string(25), p_brand string(10), p_type string(25), p_size bigint, p_container string(10), p_retailprice decimal(12,2), p_comment string(23));
:split:
CREATE OR REPLACE TABLE :dataset_name:.partsupp_:database_id:(ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment string(199));
:split:
CREATE OR REPLACE TABLE :dataset_name:.supplier_:database_id:(s_suppkey bigint, s_name string(25), s_address string(40), s_nationkey bigint, s_phone string(15), s_acctbal decimal(12,2), s_comment string(101));