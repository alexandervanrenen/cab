with revenue as (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        :lineitem
    where
            l_shipdate >= date_parse(:1, '%Y-%m-%d')
      and l_shipdate < date_add('month', 3, date_parse(:1, '%Y-%m-%d'))
    group by
        l_suppkey)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    :supplier,
    revenue
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        revenue
)
order by
    s_suppkey;