select
    o_orderpriority,
    count(*) as order_count
from
    :lineitem, :orders
where
        o_orderdate >= date_parse(:1, '%Y-%m-%d')
  and o_orderdate < date_add('month', 3, date_parse(:1, '%Y-%m-%d'))
  and l_orderkey = o_orderkey
  and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority;