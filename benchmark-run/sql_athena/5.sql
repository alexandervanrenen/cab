select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    :customer,
    :orders,
    :lineitem,
    :supplier,
    :nation,
    :region
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = :1
  and o_orderdate >= date_parse(:2, '%Y-%m-%d')
  and o_orderdate < date_add('year', 1, date_parse(:2, '%Y-%m-%d'))
group by
    n_name
order by
    revenue desc;