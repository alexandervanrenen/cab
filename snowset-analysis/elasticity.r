
qres = dbGetQuery(con, "
with largewarehouses as (
  select * from (VALUES (8959409253890846472),(1890180489335197062),(2098601964786088346),(572303244404687653),(7249649289217758973)) as warehouses(warehouseid)
)
, smallwarehouses as (
  select * from (VALUES (5222934471478722795),(6234414756982242140),(900848050815607546),(1145075591686716503),(3674929441415926268)) as warehouses(warehouseid)
)
, warehouses as ( select * from largewarehouses union all select * from smallwarehouses)

, timebins as (
  select t.timeslice
  from generate_series((select date_trunc('hour', min(timeslice)) from week_te) + interval '1 hour'
                     , (select date_trunc('hour', max(timeslice)) from week_te) - interval '1 hour'
                     , interval '1 hour') t(timeslice)
)
, xbins as(
  select *
  from timebins, warehouses
)

select x.warehouseid::text,
       x.timeslice as qhour,
       coalesce(sum(q.usercputime + q.systemcputime), 0) as cputime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 = 0 then q.usercputime + q.systemcputime else 0 end), 0) as readtime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as writetime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as readwritetime,
       coalesce(max(warehouseSize), 0) as warehouseSize
from xbins x left outer join queries_te q on x.timeslice=q.timeslice and x.warehouseid=q.warehouseid
group by 1, 2

union all

select 'All'::text as warehouseid,
       q.timeslice as qhour,
       coalesce(sum(q.usercputime + q.systemcputime), 0) as cputime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 = 0 then q.usercputime + q.systemcputime else 0 end), 0) as readtime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as writetime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as readwritetime,
       0 as warehouseSize
from timebins t left outer join queries_te q on t.timeslice=q.timeslice
group by 2

union all

select 'All - L'::text as warehouseid,
       q.timeslice as qhour,
       coalesce(sum(q.usercputime + q.systemcputime), 0) as cputime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 = 0 then q.usercputime + q.systemcputime else 0 end), 0) as readtime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache = 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as writetime,
       coalesce(sum(case when persistentReadBytesS3 + persistentReadBytesCache > 0 and persistentWriteBytesS3 > 0 then q.usercputime + q.systemcputime else 0 end), 0) as readwritetime,
       0 as warehouseSize
from timebins t left outer join queries_te q on t.timeslice=q.timeslice
where q.warehouseid not in (select warehouseid from largewarehouses)
group by 2
");

# Read/write over time --------------------------------------------------------------------------------------------------------------------------------
df = sqldf("select warehouseid, qhour, cputime, readtime, writetime, readwritetime from qres");
df = gather(df, condition, value, readtime:readwritetime, factor_key=TRUE)
df$condition = factor(df$condition, levels=c("readtime","readwritetime","writetime"), ordered = TRUE);
df$value = df$value / 1000 / 1000 / 3600
df$warehouseid = mapvalues(factor(df$warehouseid),
                           from=c("All - L","8959409253890846472","1890180489335197062","2098601964786088346","572303244404687653","7249649289217758973",
                                  "All", "5222934471478722795","6234414756982242140","900848050815607546","1145075591686716503","3674929441415926268"),
                           to=c("All - L*", "L1","L2","L3","L4","L5","All","R1","R2","R3","R4","R5"));
df$warehouseid = factor(df$warehouseid, levels=c("All - L*","L1","L2","L3","L4","L5","All","R1","R2","R3","R4","R5"), ordered = TRUE)

p1 = ggplot(df, aes(y=value, x = qhour, color=condition)) +
   geom_line() +
   scale_y_continuous(expand = c(0, 0), limits = c(0, NA)) +
   scale_color_manual(values = c(color2, color1, color4),
                     breaks= c("readtime", "readwritetime", "writetime"),
                     labels = c("Read-only", "Read/write", "Write-only")) +
   theme_minimal() +
   theme(
      axis.title.x = element_blank(),
      axis.text.x = element_text(angle = 68, hjust = 1, size = 10),
      axis.ticks = element_line(),
      axis.line = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      panel.grid.minor = element_blank(),
      legend.position=c(0.05, 0.78),
   ) +
   labs(y="CPU Hours", color = "Query Types") +
   facet_wrap(vars(warehouseid), scales = "free_y", ncol = 6); p1

g <- ggplotGrob(p1)
rm_grobs <- g$layout$name %in% c("panel-1-1", "axis-t-1-1", "axis-b-1-1", "axis-l-1-1", "strip-t-1-1", "axis-r-1-1")
g$grobs[rm_grobs] <- NULL
g$layout <- g$layout[!rm_grobs, ]
p_final = as.ggplot(g); p_final;

ggsave("elastisity.pdf", plot = p_final, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 3.0)


# Operators --------------------------------------------------------------------------------------------------------------------------------
# df = sqldf("select warehouseid, qhour, cputime, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml, readtime, writetime, readwritetime from qres");
# df = gather(df, condition, value, scan:dml, factor_key=TRUE)
# df$value = df$value / 1000 / 3600 / 24
# df$warehouseid = mapvalues(factor(df$warehouseid),
#                            from=c("All","All - L","8959409253890846472","1890180489335197062","2098601964786088346","572303244404687653","7249649289217758973",
#                                  "5222934471478722795","6234414756982242140","4462984305470491572","1145075591686716503","3674929441415926268"),
#                           to=c("All","All - L","L1","L2","L3","L4","L5","R1","R2","R3","R4","R5"));
# df$warehouseid = factor(df$warehouseid, levels=c("All","All - L","L1","L2","L3","L4","L5","R1","R2","R3","R4","R5"), ordered = TRUE)
#
# p1 = ggplot(df, aes(fill=condition, y=value, x=qhour)) +
#   geom_col() +
#   scale_fill_brewer(palette="Paired") +
#   theme_minimal() +
#   labs(y="CPU days", x="", fill = "Operator", title = "Largest Databases") +
#   facet_wrap(. ~warehouseid, scales = "free_y"); p1
# ggsave("elastisity-op.pdf", plot = p, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)

# Line --------------------------------------------------------------------------------------------------------------------------------
# df = sqldf("select warehouseid, qhour, cputime, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml, readtime, writetime, readwritetime from qres");
# df = gather(df, condition, value, cputime:cputime, factor_key=TRUE)
# df$condition = factor(df$condition);
# df$value = df$value / 1000 / 1000 / 3600
# df$warehouseid = mapvalues(factor(df$warehouseid),
#                           from=c("All","All - L","8959409253890846472","1890180489335197062","2098601964786088346","572303244404687653","7249649289217758973",
#                                  "5222934471478722795","6234414756982242140","4462984305470491572","1145075591686716503","3674929441415926268"),
#                           to=c("All","All - L*","L1","L2","L3","L4","L5","R1","R2","R3","R4","R5"));
# df$warehouseid = factor(df$warehouseid, levels=c("All","All - L* and R*","L1","L2","L3","L4","L5","R1","R2","R3","R4","R5"), ordered = TRUE)
#
# p = ggplot(df, aes(fill=condition, y=value, x=qhour)) +
#   geom_line() +
#   theme_minimal() +
#   expand_limits(y=c(0)) +
#   labs(y="CPU Hours") +
#   facet_wrap(. ~warehouseid, scales = "free_y"); p
# ggsave("elastisity-line.pdf", plot = p, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)
