select
    sum(l_extendedprice * l_discount) as revenue
from
    :lineitem
where
        l_shipdate >= date_parse(:1, '%Y-%m-%d')
  and l_shipdate < date_add('year', 1, date_parse(:1, '%Y-%m-%d'))
  and l_discount between (cast(:2 as decimal(12,2)) / 100) - 0.01 and (cast(:2 as decimal(12,2)) / 100) + 0.01
  and l_quantity < :3;