begin;

insert into :orders (
    select o_orderkey + 8,
           o_custkey,
           o_orderstatus,
           (select sum(L_QUANTITY * P_RETAILPRICE * (1+L_TAX) * (1-L_DISCOUNT)) from :lineitem, :part where l_orderkey = o_orderkey and P_PARTKEY = L_PARTKEY), o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
    from :orders
    where :1 <= o_orderkey and o_orderkey < :2
);

delete from :orders where :1 <= o_orderkey and o_orderkey < :2 and mod(o_orderkey, 32) between :3 and :4;

commit;