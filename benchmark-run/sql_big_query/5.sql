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
  and o_orderdate >= DATE :2
  and o_orderdate < DATE_ADD(DATE :2, interval 1 year)
group by
    n_name
order by
    revenue desc;