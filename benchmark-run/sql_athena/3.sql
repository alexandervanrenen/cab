select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from :lineitem,
     (select *
      from :customer, :orders
      where c_mktsegment = :1
       and c_custkey = o_custkey
     )
where
  l_orderkey = o_orderkey
  and o_orderdate < date_parse(:2, '%Y-%m-%d')
  and l_shipdate > date_parse(:2, '%Y-%m-%d')
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;