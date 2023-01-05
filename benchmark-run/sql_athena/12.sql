select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    :orders,
    :lineitem
where
        o_orderkey = l_orderkey
  and l_shipmode in (:1, :2)
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date_parse(:3, '%Y-%m-%d')
  and l_receiptdate < date_add('year', 1, date_parse(:3, '%Y-%m-%d'))
group by
    l_shipmode
order by
    l_shipmode;
