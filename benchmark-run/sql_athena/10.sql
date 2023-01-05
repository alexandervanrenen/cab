select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    :lineitem,
    (
        select *
        from :orders, :customer
        where c_custkey = o_custkey
          and o_orderdate >= date_parse(:1, '%Y-%m-%d')
          and o_orderdate < date_add('month', 3, date_parse(:1, '%Y-%m-%d'))
    ),
    :nation
where l_orderkey = o_orderkey
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;