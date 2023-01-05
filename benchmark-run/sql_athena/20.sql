select
    s_name,
    s_address
from
    :supplier,
    :nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            :partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    :part
                where
                        p_name like :1 || '%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                :lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date_parse(:2, '%Y-%m-%d')
              and l_shipdate < date_add('year', 1, date_parse(:2, '%Y-%m-%d'))
        )
    )
  and s_nationkey = n_nationkey
  and n_name = :3
order by
    s_name;