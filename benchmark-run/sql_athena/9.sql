select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            :orders,
            (select *
             from
                 :lineitem,
                 (select *
                  from :partsupp,
                       :part,
                       :supplier,
                       :nation
                  where
                          s_nationkey = n_nationkey
                    and p_name like '%' || :1 || '%'
                    and ps_partkey = ps_partkey
                    and s_suppkey = ps_suppkey)
             where
                     s_suppkey = l_suppkey
               and ps_suppkey = l_suppkey
               and ps_partkey = l_partkey
               and p_partkey = l_partkey)
        where
                o_orderkey = l_orderkey
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;