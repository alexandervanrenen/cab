-- ---------------------------------------------------------------------------------------------------------------------
-- Create a helper view with some useful attributes for queries

drop view if exists classification;
create view classification as
(
with with_0 as (select *,
                       (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso +
                        profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso) as profAll,
                       usercputime + systemcputime                                                     as cputime,
                       persistentreadbytescache + persistentreadbytess3                                as persistentread,
                       persistentwritebytess3                                                          as persistentwrite,
                       case when persistentWriteBytesS3 = 0 then 1 else 0 end                          as readonly,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0
                               then 1
                           else 0 end                                                                  as writeonly,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0
                               then 1
                           else 0 end                                                                  as readwrite,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0
                               then 'writeonly'
                           when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0
                               then 'readwrite'
                           else 'readonly' end                                                         as wtype
                from queries)

select *,
       case
           -- Also read writeWrite-only queries
           when profXtScanRso + profDmlRso + profProjRso + proffilterrso >= 0.75 * profAll and
                persistentwritebytess3 > 0 -- Writes
               and ioremoteexternalreadbytes > 0 then 'exload'
           when profDmlRso + profresrso >= 0.75 * profAll and persistentwritebytess3 > 0 then 'dml'

           -- Read-write queries
           when (readwrite = 1 or readonly = 1) and profscanrso + proffilterrso + profprojrso >= 0.75 * profall
               then 'rw-scan'
           when (readwrite = 1 or readonly = 1) and profsortrso = 0 and profaggrso = 0 and profhjrso > 0
               then 'rw-rest' -- join
           when (readwrite = 1 or readonly = 1) and profsortrso = 0 and profaggrso > 0 and profhjrso = 0
               then 'rw-rest' -- agg
           when (readwrite = 1 or readonly = 1) and profsortrso > 0 and profaggrso = 0 and profhjrso = 0
               then 'rw-rest' -- sort

       -- Read-only queries
           when readonly = 1 and profScanRso + profFilterRso + profprojrso >= 0.75 * profAll then 'ro-scan'

           when (readwrite = 1 or readonly = 1) then 'rw-rest'
           when readonly = 1 then 'ro-rest'
           when writeonly = 1 then 'rw-rest'

           else 'other' end as qtype
from with_0);

-- ---------------------------------------------------------------------------------------------------------------------
-- SAME helper view for queries_te

drop view if exists classification_te;
create view classification_te as
(
with with_0 as (select *,
                       (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso +
                        profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso) as profAll,
                       usercputime + systemcputime                                                     as cputime,
                       persistentreadbytescache + persistentreadbytess3                                as persistentread,
                       persistentwritebytess3                                                          as persistentwrite,
                       case
                           when persistentWriteBytesS3 = 0 -- readonly does not need to read
                               then 1
                           else 0 end                                                                  as readonly,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0
                               then 1
                           else 0 end                                                                  as writeonly,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0
                               then 1
                           else 0 end                                                                  as readwrite,
                       case
                           when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0
                               then 'writeonly'
                           when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0
                               then 'readwrite'
                           else 'readonly' end                                                         as wtype
                from queries_te)

select *,
       case
           -- Also read writeWrite-only queries
           when profXtScanRso + profDmlRso + profProjRso + proffilterrso >= 0.75 * profAll and
                persistentwritebytess3 > 0 -- Writes
               and ioremoteexternalreadbytes > 0 then 'exload'
           when profDmlRso + profresrso >= 0.75 * profAll and persistentwritebytess3 > 0 then 'dml'

           -- Read-write queries
           when (readwrite = 1 or readonly = 1) and profscanrso + proffilterrso + profprojrso >= 0.75 * profall
               then 'rw-scan'
           when (readwrite = 1 or readonly = 1) and profsortrso = 0 and profaggrso = 0 and profhjrso > 0 then 'rw-join'
           when (readwrite = 1 or readonly = 1) and profsortrso = 0 and profaggrso > 0 and profhjrso = 0 then 'rw-agg'
           when (readwrite = 1 or readonly = 1) and profsortrso > 0 and profaggrso = 0 and profhjrso = 0 then 'rw-sort'

           -- Read-only queries
           when readonly = 1 and profScanRso + profFilterRso + profprojrso >= 0.75 * profAll then 'ro-scan'

           when (readwrite = 1 or readonly = 1) then 'rw-rest'
           when readonly = 1 then 'ro-rest'
           when writeonly = 1 then 'wo-rest'

           else 'other' end as qtype
from with_0);
