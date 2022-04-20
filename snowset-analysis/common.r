library(ggplot2)
library(parsedate)
library(tidyr)
library(sqldf)
library(grid)
library(gridExtra)

require("RPostgreSQL")
drv = dbDriver("PostgreSQL")
if(is.null(con)) {
   con = dbConnect(drv, dbname = "snowset",
                   host = "localhost", port = 5432,
                   user = "postgres")
}

# -----------------------------------------------------------------------------
# Query duration distribution
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select durationtotal / 1000 / 60 as duration, count(*) as querycount from day group by durationtotal / 1000 / 60"))
df = qres

ggplot(df, aes(y=querycount, x=duration)) + 
   geom_line(stat="identity") +
   scale_fill_brewer(palette="Paired") +
   theme_minimal() + scale_y_log10() +
   labs(y="Query Count", x="Duration [s]", title = "Query Time Distribution") + theme(plot.title = element_text(hjust = 0.5))
ggsave("common-query-time-distribution.pdf", plot = last_plot(), scale = 1, units = c("in", "cm", "mm", "px"))


# -----------------------------------------------------------------------------
# Load query usage per warehouse
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select cast(a.ratio as decimal(10,1)) as ratio, count(*) as cnt
from (select warehouseid, sum(CASE WHEN (returnedrows = 1
                                           and persistentWriteBytesS3 > 0
                                           and  (profXtScanRso + profDmlRso) >= 0.99 * (profScanRso + profXtScanRso + profProjRso + profSortRso + profFilterRso + profResRso + profDmlRso + profHjRso +
                                                                                                profBufRso + profFlatRso + profBloomRso + profAggRso)) THEN 1 ELSE 0 END) * 1.0 / count(*) as ratio
        from day
        group by warehouseid
        order by ratio desc) a
group by cast(a.ratio as decimal(10,1))"))
ggplot(qres, aes(y=cnt, x=ratio)) + 
   geom_bar(stat="identity") +
   theme_minimal() +
   theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
   labs(y="Rows", x="Querys [id]", fill = "Rows") + theme(plot.title = element_text(hjust = 0.5))


# -----------------------------------------------------------------------------
# Read-only warehouses
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select cast(a.ratio as decimal(10,1)) as ratio, count(*) as cnt
from (select warehouseid, sum(CASE WHEN (persistentWriteBytesS3 = 0) THEN 1 ELSE 0 END) * 1.0 / count(*) as ratio
        from day
        group by warehouseid
        order by ratio desc) a
group by cast(a.ratio as decimal(10,1))"))
ggplot(qres, aes(y=cnt, x=ratio)) + 
   geom_bar(stat="identity") +
   theme_minimal() +
   theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
   labs(y="Rows", x="Querys [id]", fill = "Rows") + theme(plot.title = element_text(hjust = 0.5))


# -----------------------------------------------------------------------------
# Write-only warehouses
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, paste("select cast(a.ratio as decimal(10,1)) as ratio, count(*) as cnt
from (select warehouseid, sum(CASE WHEN (persistentReadBytesS3 = 0 and persistentReadBytesCache = 0) THEN 1 ELSE 0 END) * 1.0 / count(*) as ratio
        from day
        group by warehouseid
        order by ratio desc) a
group by cast(a.ratio as decimal(10,1))"))
ggplot(qres, aes(y=cnt, x=ratio)) + 
   geom_bar(stat="identity") +
   theme_minimal() +
   theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
   labs(y="Rows", x="Querys [id]", fill = "Rows") + theme(plot.title = element_text(hjust = 0.5))


