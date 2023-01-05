select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone, 1, 2) as cntrycode,
            c_acctbal
        from
            :customer
        where
                cast(substring(c_phone, 1, 2) as int) in
                (:1, :2, :3, :4, :5, :6, :7)
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                :customer
            where
                    c_acctbal > 0.00
              and cast(substring(c_phone, 1, 2) as int) in
                  (:1, :2, :3, :4, :5, :6, :7)
        )
          and not exists (
                select
                    *
                from
                    :orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;