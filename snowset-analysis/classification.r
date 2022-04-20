library(ggplot2)
library(tidyr)
library(grid)
library(gridExtra)
library(sqldf)

require("RPostgreSQL")
drv = dbDriver("PostgreSQL")
con = dbConnect(drv, dbname = "snowset",
                host = "127.0.0.1", port = 25432,
                user = "postgres")

AnalyzeQueries = function(qres) {

        qres$id = c(1:nrow(qres))

        # -----------------------------------------------------------------------------
        # Operator composition of individual queries
        # -----------------------------------------------------------------------------
        
        df = sqldf("select id, durationtotal, scan, dml, sort, externalscan, result, projection, aggregation, filter, hjoin, buffer, flatten, bloomfilter from qres");
        df = gather(df, condition, value, scan:bloomfilter, factor_key=TRUE)
        df$condition = factor(df$condition)
        # df$condition = with(df, reorder(condition, -value))
        df$normalized_value = df$value / 1000

        plot_operator = ggplot(df, aes(fill=condition, y=normalized_value, x=id)) + 
                geom_bar(stat="identity", position = "fill") +
                scale_fill_brewer(palette="Paired") +
                theme_minimal() +
                theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
                labs(y="CPU seconds", x="Queries [id]", fill = "Operator") + theme(plot.title = element_text(hjust = 0.5))

        # -----------------------------------------------------------------------------
        # Reads composition of individual queries
        # -----------------------------------------------------------------------------
        
        df = sqldf("select id, durationtotal, read_s3, read_cache, read_int_ssd, read_int_s3, read_external, read_network from qres");
        df = gather(df, condition, value, read_s3:read_network, factor_key=TRUE)
        df$condition = factor(df$condition)
        # df$condition = with(df, reorder(condition, -value))
        df$normalized_value = df$value / 1000 / 1000 / 1000

        plot_read = ggplot(df, aes(fill=condition, y=normalized_value, x=id)) + 
                geom_bar(stat="identity") +
                scale_fill_brewer(palette="Paired") +
                theme_minimal() +
                theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
                labs(y="Bytes [GB]", x="Querys [id]", fill = "Reads") + theme(plot.title = element_text(hjust = 0.5))
        
        # -----------------------------------------------------------------------------
        # Write composition of individual queries
        # -----------------------------------------------------------------------------
        
        df = sqldf("select id, durationtotal, write_s3, write_cache, write_int_ssd_comp, write_int_s3, write_int_ssd_raw from qres");
        df = gather(df, condition, value, write_s3:write_int_ssd_raw, factor_key=TRUE)
        df$condition = factor(df$condition)
        # df$condition = with(df, reorder(condition, -value))
        df$normalized_value = df$value / 1000 / 1000 / 1000
        
        plot_write = ggplot(df, aes(fill=condition, y=normalized_value, x=id)) + 
                geom_bar(stat="identity") +
                scale_fill_brewer(palette="Paired") +
                theme_minimal() +
                theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
                labs(y="Bytes [GB]", x="Querys [id]", fill = "Writes") + theme(plot.title = element_text(hjust = 0.5))
        
        # -----------------------------------------------------------------------------
        # Rows
        # -----------------------------------------------------------------------------
        
        df = sqldf("select id, durationtotal, scanBytes from qres");
        df = gather(df, condition, value, scanbytes:scanbytes, factor_key=TRUE)
        df$condition = factor(df$condition)
        # df$condition = with(df, reorder(condition, -value))
        df$normalized_value = df$value
        df$normalized_value = df$value / 1000 / 1000 / 1000
        
        plot_rows = ggplot(df, aes(fill=condition, y=normalized_value, x=id)) + 
                geom_bar(stat="identity") +
                scale_fill_brewer(palette="Paired") +
                theme_minimal() +
                theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
                labs(y="scanBytes", x="Querys [id]", fill = "Bytes [GB]") + theme(plot.title = element_text(hjust = 0.5))
        
        grid.arrange(plot_operator, plot_read, plot_write, plot_rows, nrow = 2, top = textGrob(paste("Report")))
        # ggsave(paste("classification-accesses", topname, ".pdf", sep = ""), plot = plot_all, scale = 1, units = c("in", "cm", "mm", "px"))
}

# -----------------------------------------------------------------------------
# Top queries of specific warehouse
# -----------------------------------------------------------------------------

topname = 1
count = 30
offset = 0
qres = dbGetQuery(con, paste("select queryid,
                                     durationtotal,
                                     profScanRso as scan,
                                     profXtScanRso as externalscan,
                                     profProjRso as projection,
                                     profSortRso as sort,
                                     profFilterRso as filter,
                                     profResRso as result,
                                     profDmlRso as dml,
                                     profHjRso as hjoin,
                                     profBufRso as buffer,
                                     profFlatRso as flatten,
                                     profBloomRso as bloomfilter,
                                     profAggRso as aggregation,
                             
                                     returnedRows,
                                     producedRows,
                             
                                     persistentWriteBytesS3 as write_s3,
                                     persistentWriteBytesCache as write_cache,
                                     intDataWriteBytesLocalSSD as write_int_ssd_comp,
                                     intDataWriteBytesS3 as write_int_s3,
                                     intDataWriteBytesUncompressed as write_int_ssd_raw,
                             
                                     persistentReadBytesS3 as read_s3,
                                     persistentReadBytesCache as read_cache,
                                     intDataReadBytesLocalSSD as read_int_ssd,
                                     intDataReadBytesS3 as read_int_s3,
                                     ioremoteexternalreadbytes as read_external,
                                     intDataNetReceivedBytes as read_network
                             from day
                             where topname = ", topname, "
                             order by durationtotal desc
                             limit ", count, " offset ", offset))
AnalyzeQueries(qres)


# -----------------------------------------------------------------------------
# Query type: Load queries
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select queryid,
                                     durationtotal,
                                     profScanRso as scan,
                                     profXtScanRso as externalscan,
                                     profProjRso as projection,
                                     profSortRso as sort,
                                     profFilterRso as filter,
                                     profResRso as result,
                                     profDmlRso as dml,
                                     profHjRso as hjoin,
                                     profBufRso as buffer,
                                     profFlatRso as flatten,
                                     profBloomRso as bloomfilter,
                                     profAggRso as aggregation,

                                     returnedRows,
                                     producedRows,
                             
                                     persistentWriteBytesS3 as write_s3,
                                     persistentWriteBytesCache as write_cache,
                                     intDataWriteBytesLocalSSD as write_int_ssd_comp,
                                     intDataWriteBytesS3 as write_int_s3,
                                     intDataWriteBytesUncompressed as write_int_ssd_raw,
                             
                                     persistentReadBytesS3 as read_s3,
                                     persistentReadBytesCache as read_cache,
                                     intDataReadBytesLocalSSD as read_int_ssd,
                                     intDataReadBytesS3 as read_int_s3,
                                     ioremoteexternalreadbytes as read_external,
                                     intDataNetReceivedBytes as read_network
                             from day
                             where returnedrows = 1
                             and persistentWriteBytesS3 > 0
                             and  (profXtScanRso + profDmlRso) >= 0.99 * (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso + profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso)
                             order by durationtotal desc
                             limit 20 offset 0"))
AnalyzeQueries(qres)

# -----------------------------------------------------------------------------
# Scan queries
# -----------------------------------------------------------------------------

topname = 5
count = 30
offset = 0
qres = dbGetQuery(con, paste("select queryid,
                                     durationtotal,
                                     profScanRso as scan,
                                     profXtScanRso as externalscan,
                                     profProjRso as projection,
                                     profSortRso as sort,
                                     profFilterRso as filter,
                                     profResRso as result,
                                     profDmlRso as dml,
                                     profHjRso as hjoin,
                                     profBufRso as buffer,
                                     profFlatRso as flatten,
                                     profBloomRso as bloomfilter,
                                     profAggRso as aggregation,
                             
                                     returnedRows,
                                     producedRows,
                             
                                     persistentWriteBytesS3 as write_s3,
                                     persistentWriteBytesCache as write_cache,
                                     intDataWriteBytesLocalSSD as write_int_ssd_comp,
                                     intDataWriteBytesS3 as write_int_s3,
                                     intDataWriteBytesUncompressed as write_int_ssd_raw,
                             
                                     persistentReadBytesS3 as read_s3,
                                     persistentReadBytesCache as read_cache,
                                     intDataReadBytesLocalSSD as read_int_ssd,
                                     intDataReadBytesS3 as read_int_s3,
                                     ioremoteexternalreadbytes as read_external,
                                     intDataNetReceivedBytes as read_network
                             from day
                             where  profScanRso + profFilterRso >= 0.9 * (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso + profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso)
                             order by durationtotal desc
                             limit 20 offset 0"))
AnalyzeQueries(qres)

# -----------------------------------------------------------------------------
# Analyze specific warehouse
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select queryid,
                                     durationtotal,
                                     profScanRso as scan,
                                     profXtScanRso as externalscan,
                                     profProjRso as projection,
                                     profSortRso as sort,
                                     profFilterRso as filter,
                                     profResRso as result,
                                     profDmlRso as dml,
                                     profHjRso as hjoin,
                                     profBufRso as buffer,
                                     profFlatRso as flatten,
                                     profBloomRso as bloomfilter,
                                     profAggRso as aggregation,
                                     (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso + profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso) as profAll,
                             
                                     returnedRows,
                                     producedRows,
                                     scanBytes,
                             
                                     persistentWriteBytesS3 as write_s3,
                                     persistentWriteBytesCache as write_cache,
                                     intDataWriteBytesLocalSSD as write_int_ssd_comp,
                                     intDataWriteBytesS3 as write_int_s3,
                                     intDataWriteBytesUncompressed as write_int_ssd_raw,
                             
                                     persistentReadBytesS3 as read_s3,
                                     persistentReadBytesCache as read_cache,
                                     intDataReadBytesLocalSSD as read_int_ssd,
                                     intDataReadBytesS3 as read_int_s3,
                                     ioremoteexternalreadbytes as read_external,
                                     intDataNetReceivedBytes as read_network
                              from classification
                              where readonly = 1
                                  and profDmlRso > 0
                             limit 100;"))
AnalyzeQueries(qres)


qres = dbGetQuery(con, paste("select * from (select queryid,
                                     durationtotal,
                                     (case when (userCpuTime + systemCpuTime) / 1000 <= 0 then 0 else ceil(log((userCpuTime + systemCpuTime) / 1000)) end) as timeBucket,
                                     ROW_NUMBER() OVER (PARTITION BY (case when (userCpuTime + systemCpuTime) / 1000 <= 0 then 0 else ceil(log((userCpuTime + systemCpuTime) / 1000)) end) ORDER BY (userCpuTime + systemCpuTime) desc) AS rownum,
                                     profScanRso as scan,
                                     profXtScanRso as externalscan,
                                     profProjRso as projection,
                                     profSortRso as sort,
                                     profFilterRso as filter,
                                     profResRso as result,
                                     profDmlRso as dml,
                                     profHjRso as hjoin,
                                     profBufRso as buffer,
                                     profFlatRso as flatten,
                                     profBloomRso as bloomfilter,
                                     profAggRso as aggregation,
                                     (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso + profDmlRso + profHjRso + profBufRso + profFlatRso + profBloomRso + profAggRso) as profAll,
                             
                                     returnedRows,
                                     producedRows,
                                     scanBytes,
                             
                                     persistentWriteBytesS3 as write_s3,
                                     persistentWriteBytesCache as write_cache,
                                     intDataWriteBytesLocalSSD as write_int_ssd_comp,
                                     intDataWriteBytesS3 as write_int_s3,
                                     intDataWriteBytesUncompressed as write_int_ssd_raw,
                             
                                     persistentReadBytesS3 as read_s3,
                                     persistentReadBytesCache as read_cache,
                                     intDataReadBytesLocalSSD as read_int_ssd,
                                     intDataReadBytesS3 as read_int_s3,
                                     ioremoteexternalreadbytes as read_external,
                                     intDataNetReceivedBytes as read_network
                              from classification
                              where readonly = 1
                                  and profDmlRso > 0;
                             ) X
                             -- where rownum between 0 and 50 and timeBucket between 2 and 6
                             limit 50"))

AnalyzeQueries(qres)

qres$id = c(1:nrow(qres))

# -----------------------------------------------------------------------------
# Operator composition of individual queries
# -----------------------------------------------------------------------------

df = sqldf("select id, timeBucket, durationtotal, scan, dml, sort, externalscan, result, projection, aggregation, filter, hjoin, buffer, flatten, bloomfilter from qres");
df = gather(df, condition, value, scan:bloomfilter, factor_key=TRUE)
df$condition = factor(df$condition)
# df$condition = with(df, reorder(condition, -value))
df$normalized_value = df$value / 1000

ggplot(df, aes(fill=condition, y=normalized_value, x=factor(id))) + 
        geom_bar(stat="identity", position = "fill") +
        scale_fill_brewer(palette="Paired") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
        labs(y="CPU seconds", x="Queries [id]", fill = "Operator") + theme(plot.title = element_text(hjust = 0.5)) +
        facet_grid(rows = ~timebucket, scales = "free")

