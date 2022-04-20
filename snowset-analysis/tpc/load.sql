create table tpc_data
(
    Workload                   text   not null,
    Type                       text   not null,
    QueryId                    text   not null,
    Time                       bigint not null,
    Steps                      int    not null,
    TableScan                  bigint not null,
    Filter                     bigint not null,
    JoinFilter                 bigint not null,
    hJoin                      bigint not null,
    Aggregate                  bigint not null,
    Sort                       bigint not null,
    SortWithLimit              bigint not null,
    Result                     bigint not null,
    Projection                 bigint not null,
    ExternalScan               bigint not null,
    TryDeduplicate             bigint not null,
    dmlInsert                  bigint not null,
    dmlDelete                  bigint not null,
    withClause                 bigint not null,
    withReference              bigint not null,
    unionAll                   bigint not null,
    groupingSets               bigint not null,
    localStop                  bigint not null,
    windowFunction             bigint not null,
    limitOp                    bigint not null,
    topK                       bigint not null,
    cartesianJoin              bigint not null,
    generator                  bigint not null,
    Processing                 bigint not null,
    RemoteDiskIO               bigint not null,
    LocalDiskIO                bigint not null,
    Initialization             bigint not null,
    Synchronization            bigint not null,
    NetworkCommunication       bigint not null,
    Numberofrowsdeleted        bigint not null,
    Scanprogress               real   not null,
    BytesScanned               bigint not null,
    PercentageScannedFromCache real   not null,
    BytesWritten               bigint not null,
    BytesSentOverTheNetwork    bigint not null,
    Partitionsscanned          bigint not null,
    Partitionstotal            bigint not null,
    Byteswrittentoresult       bigint not null,
    Numberofrowsinserted       bigint not null,
    Externalbytesscanned       bigint not null,
    Rows                       bigint not null
);

create or replace view tpc_queries as
(
SELECT tpc_data.queryid,
       tpc_data.workload,
       (tpc_data.workload || ' '::text) || tpc_data.type                    AS type,
       tpc_data."time"                                                      AS durationtotal,
       1                                                                    AS servercount,
       tpc_data.bytesscanned                                                AS scanbytes,
       tpc_data.byteswritten                                                AS writebytes,
       tpc_data.tablescan                                                   AS profscanrso,
       tpc_data.externalscan                                                AS profxtscanrso,
       tpc_data.projection                                                  AS profprojrso,
       tpc_data.sort + tpc_data.sortwithlimit                               AS profsortrso,
       tpc_data.filter                                                      AS proffilterrso,
       tpc_data.result                                                      AS profresrso,
       tpc_data.dmldelete + tpc_data.dmlinsert                              AS profdmlrso,
       tpc_data.hjoin                                                       AS profhjrso,
       0                                                                    AS profbufrso,
       0                                                                    AS profflatrso,
       tpc_data.joinfilter                                                  AS profbloomrso,
       tpc_data.aggregate + tpc_data.trydeduplicate + tpc_data.groupingsets AS profaggrso,
       0                                                                    AS profbandrso,
       0                                                                    AS profpercentilerso,
       0                                                                    AS asprofudtfrso,
       0                                                                    AS memoryused,
       tpc_data.windowfunction                                              as profwindow
FROM tpc_data);

drop view if exists classification_tpc;
create view classification_tpc as
(
with with_0 as (select *,
                       (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso +
                        profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso) as profAll,
                       scanbytes                                                                       as persistentread,
                       writebytes                                                                      as persistentwrite,
                       case when writebytes = 0 then 1 else 0 end                                      as readonly,
                       case
                           when scanbytes = 0 and writebytes > 0 then 1
                           else 0 end                                                                  as writeonly,
                       case
                           when scanbytes > 0 and writebytes > 0 then 1
                           else 0 end                                                                  as readwrite,
                       case
                           when scanbytes = 0 and writebytes > 0 then 'writeonly'
                           when scanbytes > 0 and writebytes > 0 then 'readwrite'
                           else 'readonly' end                                                         as wtype
                from tpc_queries)

select *,
       case
           -- Also read writeWrite-only queries
           when profXtScanRso + profDmlRso + profProjRso + proffilterrso >= 0.75 * profAll and
                writebytes > 0 -- Writes
               and QueryId like 'RF%' then 'exload'
           when profDmlRso + profresrso >= 0.75 * profAll and writebytes > 0 then 'dml'

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
