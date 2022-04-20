
# -----------------------------------------------------------------------------
# Biggest databases
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, "select ROW_NUMBER() OVER(order by sum(cputime) desc) as name,
                                     sum(profScanRso) as scan,
                                     sum(profFilterRso) as filter,
                                     sum(profBloomRso) as bloomfilter,
                                     sum(profHjRso) as hjoin,
                                     sum(profAggRso) as aggregation,
                                     sum(profSortRso) as sort,
                                     sum(profResRso) as result,
                                     sum(profProjRso) as projection,
                                     sum(profXtScanRso) as externalscan,
                                     sum(profDmlRso) as dml,
                                     sum(profBufRso) as buffer,
                                     sum(profFlatRso) as flatten
                             from classification
                             where databaseid is not null
                             group by databaseid
                             order by sum(cputime) desc
                             limit 10;");
qres = sqldf("select name, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres");

# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$condition = factor(df$condition)
df$value = df$value / 1000 / 3600 / 24

p_large_dbs = ggplot(df, aes(fill=condition, y=value, x=factor(name))) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   #   theme_minimal() +
   theme(axis.text.x = element_text(angle = 60, vjust = 0.5)) + theme(legend.position = "none") +
   labs(y="CPU days", x="Databases", fill = "Operator", title = "Biggest Databases") + theme(plot.title = element_text(hjust = 0.5)); p_large_dbs

# -----------------------------------------------------------------------------
# Biggest warehouses
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, "select ROW_NUMBER() OVER(order by sum(cputime) desc) as name,
                                     sum(profScanRso) as scan,
                                     sum(profFilterRso) as filter,
                                     sum(profBloomRso) as bloomfilter,
                                     sum(profHjRso) as hjoin,
                                     sum(profAggRso) as aggregation,
                                     sum(profSortRso) as sort,
                                     sum(profResRso) as result,
                                     sum(profProjRso) as projection,
                                     sum(profXtScanRso) as externalscan,
                                     sum(profDmlRso) as dml,
                                     sum(profBufRso) as buffer,
                                     sum(profFlatRso) as flatten
                             from classification
                             where warehouseid is not null
                             -- and databaseid = (select databaseid from classification group by databaseid order by sum(cputime) desc limit 1)
                             group by warehouseid
                             order by sum(cputime) desc
                             limit 10;");
qres = sqldf("select name, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres");

# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$condition = factor(df$condition)
df$value = df$value / 1000 / 3600 / 24

p_large_vws = ggplot(df, aes(fill=condition, y=value, x=factor(name))) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   #   theme_minimal() +
   theme(axis.text.x = element_text(angle = 60, vjust = 0.5)) + theme(legend.position = "none") +
   labs(y="CPU days", x="Warehouses", fill = "Operator", title = "Biggest Warehouses") + theme(plot.title = element_text(hjust = 0.5)); p_large_vws

# -----------------------------------------------------------------------------
# Operator distribution over time per DB
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, "
with biggest as (select databaseid
                 from classification
                 where databaseid is not null
                 group by databaseid
                 order by sum(cputime) desc
                 limit 5)
select a.databaseid,
       a.qhour                                   as qhour,
       dense_rank() OVER (order by a.databaseid) as name,
       coalesce(sum(profScanRso), 0)             as scan,
       coalesce(sum(profFilterRso), 0)           as filter,
       coalesce(sum(profBloomRso), 0)            as bloomfilter,
       coalesce(sum(profHjRso), 0)               as hjoin,
       coalesce(sum(profAggRso), 0)              as aggregation,
       coalesce(sum(profSortRso), 0)             as sort,
       coalesce(sum(profResRso), 0)              as result,
       coalesce(sum(profProjRso), 0)             as projection,
       coalesce(sum(profXtScanRso), 0)           as externalscan,
       coalesce(sum(profDmlRso), 0)              as dml,
       coalesce(sum(profBufRso), 0)              as buffer,
       coalesce(sum(profFlatRso), 0)             as flatten
from (select databaseid, qhour
      from biggest,
           (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23)) as x(qhour)) a
         left outer join classification_te on a.databaseid = classification_te.databaseid and extract(hour from timeslice) = a.qhour
group by 1, 2;
");
qres = sqldf("select name, qhour, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres");

# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$condition = factor(df$condition)
df$value = df$value / 1000 / 3600 / 24

p_large_dbs_time = ggplot(df, aes(fill=condition, y=value, x=qhour)) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   #   theme_minimal() +
   labs(y="CPU days", x="Time of Day", fill = "Operator", title = "Largest Databases") + theme(legend.position = "none") +
   facet_grid(rows = ~name, scales = "free"); p_large_dbs_time

# -----------------------------------------------------------------------------
# Operator distribution over time per VW
# -----------------------------------------------------------------------------

qres = dbGetQuery(con, "
with biggest as (select warehouseid
                 from classification
                 where warehouseid is not null
                 group by warehouseid
                 order by sum(cputime) desc
                 limit 5)
select a.warehouseid,
       a.qhour                                    as qhour,
       dense_rank() OVER (order by a.warehouseid) as name,
       coalesce(sum(profScanRso), 0)              as scan,
       coalesce(sum(profFilterRso), 0)            as filter,
       coalesce(sum(profBloomRso), 0)             as bloomfilter,
       coalesce(sum(profHjRso), 0)                as hjoin,
       coalesce(sum(profAggRso), 0)               as aggregation,
       coalesce(sum(profSortRso), 0)              as sort,
       coalesce(sum(profResRso), 0)               as result,
       coalesce(sum(profProjRso), 0)              as projection,
       coalesce(sum(profXtScanRso), 0)            as externalscan,
       coalesce(sum(profDmlRso), 0)               as dml,
       coalesce(sum(profBufRso), 0)               as buffer,
       coalesce(sum(profFlatRso), 0)              as flatten
from (select warehouseid, qhour
      from biggest,
           (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23)) as x(qhour)) a
         left outer join classification_te on a.warehouseid = classification_te.warehouseid and extract(hour from timeslice) = a.qhour
group by 1, 2;
");
qres = sqldf("select name, qhour, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres");

# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$condition = factor(df$condition)
df$value = df$value / 1000 / 3600 / 24

p_large_vws_time = ggplot(df, aes(fill=condition, y=value, x=qhour)) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   #   theme_minimal() +
   labs(y="CPU days", x="Time of Day", fill = "Operator", title = "Largest Warehouses") + theme(legend.position = "none") +
   facet_grid(rows = ~name, scales = "free"); p_large_vws_time

# -----------------------------------------------------------------------------
# Combine them all
# -----------------------------------------------------------------------------

Sys.sleep(1)
p = grid.arrange(p_all, p_large_dbs, p_large_vws, p_large_dbs_time, p_large_vws_time, top = textGrob("Operator Usage"), 
             widths = c(1, 1, 1, 1, 1, 1),
             layout_matrix = rbind(c(1, 1, 2, 2, 3, 3), c(4, 4, 4, 5, 5, 5))
); p

ggsave("databases.pdf", plot = p, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)
