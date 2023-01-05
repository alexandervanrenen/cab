select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    :part,
    :lineitem
where
        l_partkey = p_partkey
  and l_shipdate >= date_parse(:1, '%Y-%m-%d')
  and l_shipdate < date_add('month', 1, date_parse(:1, '%Y-%m-%d'));