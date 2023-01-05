select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    lineitem_19,
    (select * from
                  customer_19,
                  orders_19
     where
             o_orderkey in (
             select
                 l_orderkey
             from
                 lineitem_19
             group by
                 l_orderkey having
                     sum(l_quantity) > :1
         )
       and c_custkey = o_custkey
    )
where o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;