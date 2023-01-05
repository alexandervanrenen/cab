select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    :region,
    :nation,
    :supplier,
    (select *
        from
            :partsupp,
            :part
    where p_partkey = ps_partkey)
where
  s_suppkey = ps_suppkey
  and p_size = :1
  and p_type like '%' || :2
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = :3
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
        :partsupp,
        :supplier,
        (select * from :nation, :region where n_regionkey = r_regionkey and r_name = :3)
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;