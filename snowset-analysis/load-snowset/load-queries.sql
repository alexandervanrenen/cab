-- TODO: read and test this code .. right now it is a best effort copy and paste thing from various snippets ..

-- ---------------------------------------------------------------------------------------------------------------------
-- Load data
-- Turn wal_ minimal, archive mode off .. senders 0

begin;
create table queries
(
    queryId                         bigint    not null,
    warehouseId                     bigint    not null,
    databaseId                      bigint,
    createdTime                     timestamp not null,
    endTime                         timestamp not null,
    durationTotal                   bigint    not null,
    durationExec                    bigint    not null,
    durationControlPlane            bigint    not null,
    durationCompiling               bigint    not null,
    compilationTime                 bigint    not null,
    scheduleTime                    bigint    not null,
    serverCount                     bigint    not null,
    execTime                        bigint    not null,
    warehouseSize                   bigint    not null,
    perServerCores                  bigint    not null,
    persistentReadBytesS3           bigint    not null,
    persistentReadRequestsS3        bigint    not null,
    persistentReadBytesCache        bigint    not null,
    persistentReadRequestsCache     bigint    not null,
    persistentWriteBytesCache       bigint    not null,
    persistentWriteRequestsCache    bigint    not null,
    persistentWriteBytesS3          bigint    not null,
    persistentWriteRequestsS3       bigint    not null,
    intDataWriteBytesLocalSSD       bigint    not null,
    intDataWriteRequestsLocalSSD    bigint    not null,
    intDataReadBytesLocalSSD        bigint    not null,
    intDataReadRequestsLocalSSD     bigint    not null,
    intDataWriteBytesS3             bigint    not null,
    intDataWriteRequestsS3          bigint    not null,
    intDataReadBytesS3              bigint    not null,
    intDataReadRequestsS3           bigint    not null,
    intDataWriteBytesUncompressed   bigint    not null,
    ioRemoteExternalReadBytes       bigint    not null,
    ioRemoteExternalReadRequests    bigint    not null,
    intDataNetReceivedBytes         bigint    not null,
    intDataNetSentBytes             bigint    not null,
    intDataNetSentRequests          bigint    not null,
    intDataNetSentBytesUncompressed bigint    not null,
    producedRows                    bigint    not null,
    returnedRows                    bigint    not null,
    fileStolenCount                 bigint    not null,
    remoteSeqScanFileOps            bigint    not null,
    localSeqScanFileOps             bigint    not null,
    localWriteFileOps               bigint    not null,
    remoteSkipScanFileOps           bigint    not null,
    remoteWriteFileOps              bigint    not null,
    filesCreated                    bigint    not null,
    scanAssignedBytes               bigint    not null,
    scanAssignedFiles               bigint    not null,
    scanBytes                       bigint    not null,
    scanFiles                       bigint    not null,
    scanOriginalFiles               bigint    not null,
    userCpuTime                     bigint    not null,
    systemCpuTime                   bigint    not null,
    profIdle                        bigint    not null,
    profCpu                         bigint    not null,
    profPersistentReadCache         bigint    not null,
    profPersistentWriteCache        bigint    not null,
    profPersistentReadS3            bigint    not null,
    profPersistentWriteS3           bigint    not null,
    profIntDataReadLocalSSD         bigint    not null,
    profIntDataWriteLocalSSD        bigint    not null,
    profIntDataReadS3               bigint    not null,
    profIntDataWriteS3              bigint    not null,
    profRemoteExtRead               bigint    not null,
    profRemoteExtWrite              bigint    not null,
    profResWriteS3                  bigint    not null,
    profFsMeta                      bigint    not null,
    profDataExchangeNet             bigint    not null,
    profDataExchangeMsg             bigint    not null,
    profControlPlaneMsg             bigint    not null,
    profOs                          bigint    not null,
    profMutex                       bigint    not null,
    profSetup                       bigint    not null,
    profSetupMesh                   bigint    not null,
    profTeardown                    bigint    not null,
    profScanRso                     bigint    not null,
    profXtScanRso                   bigint    not null,
    profProjRso                     bigint    not null,
    profSortRso                     bigint    not null,
    profFilterRso                   bigint    not null,
    profResRso                      bigint    not null,
    profDmlRso                      bigint    not null,
    profHjRso                       bigint    not null,
    profBufRso                      bigint    not null,
    profFlatRso                     bigint    not null,
    profBloomRso                    bigint    not null,
    profAggRso                      bigint    not null,
    profBandRso                     bigint    not null,
    profPercentileRso               bigint    not null,
    profUdtfRso                     bigint    not null,
    memoryUsed                      bigint    not null
);
copy queries from '/tmp/queries.csv';
commit;

-- ---------------------------------------------------------------------------------------------------------------------
-- Load day table (contains all queries for one specific day, this makes it easier to interactively work on the data)
-- Turn wal_ minimal, archive mode off .. senders 0
begin;
create table day as (select *
                     from queries
                     where date_trunc('day', createdtime) = '2018-02-22'::date);
commit;

-- ---------------------------------------------------------------------------------------------------------------------
-- Create indexes to speed up processing

create unique index on queries (queryid);
create index on queries (warehouseid);
create index on queries (databaseid);
create unique index on day (queryid);
create index on day (warehouseid);
create index on day (databaseid);
