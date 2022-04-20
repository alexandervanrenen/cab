
qres = dbGetQuery(con, "select extract(hour from timeslice) as hour,
                                     sum(profScanRso) as scan,
                                     sum(profXtScanRso) as externalscan,
                                     sum(profProjRso) as projection,
                                     sum(profSortRso) as sort,
                                     sum(profFilterRso) as filter,
                                     sum(profResRso) as result,
                                     sum(profDmlRso) as dml,
                                     sum(profHjRso) as hjoin,
                                     sum(profBufRso) as buffer,
                                     sum(profFlatRso) as flatten,
                                     sum(profBloomRso) as bloomfilter,
                                     sum(profAggRso) as aggregation
                             from day_te
                             group by extract(hour from timeslice);")
qres = sqldf("select hour, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres")

# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$value = df$value / 1000 / 3600 / 24

df$condition = factor(df$condition)

ggplot(df, aes(fill=condition, y=value, x=hour)) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   theme_minimal() +
   labs(y="CPU days", x="All Warehouses", fill = "Operator", title = "Operator Usage") + theme(plot.title = element_text(hjust = 0.5))
ggsave("operators-over-time.pdf", plot = last_plot(), scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)


# --------------------------------------------------------------------------------


qres = dbGetQuery(con, "select extract(hour from timeslice) as hour, warehouseid,
                                     sum(profScanRso) as scan,
                                     sum(profXtScanRso) as externalscan,
                                     sum(profProjRso) as projection,
                                     sum(profSortRso) as sort,
                                     sum(profFilterRso) as filter,
                                     sum(profResRso) as result,
                                     sum(profDmlRso) as dml,
                                     sum(profHjRso) as hjoin,
                                     sum(profBufRso) as buffer,
                                     sum(profFlatRso) as flatten,
                                     sum(profBloomRso) as bloomfilter,
                                     sum(profAggRso) as aggregation
                             from day_te
                             where warehouseid = 218951437439014535 or warehouseid = 3107699131175752691 or warehouseid = 3262131279839123138 or warehouseid = 564443968101426778 or warehouseid = 2465200608953065006 or warehouseid = 1936465165596393751 or warehouseid= 4055862551282749544 or warehouseid = 3107699131175752691 or warehouseid = 4883209776626374466
                             group by extract(hour from timeslice), warehouseid;")
qres = sqldf("select hour, warehouseid, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres")
# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$value = df$value / 1000 / 3600 / 24

df$condition = factor(df$condition)
df$warehouseid = factor(df$warehouseid)

ggplot(df, aes(fill=condition, y=value, x=hour)) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   theme_minimal() +
   labs(y="CPU days", x="All Warehouses", fill = "Operator", title = "Operator Usage") + theme(plot.title = element_text(hjust = 0.5)) +
   facet_wrap(~warehouseid)
ggsave("all-warehouses-1-selected.pdf", plot = last_plot(), scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)



# --------------------------------------------------------------------------------


qres = dbGetQuery(con, "select extract(hour from timeslice) as hour, warehouseid,
                                     sum(profScanRso) as scan,
                                     sum(profXtScanRso) as externalscan,
                                     sum(profProjRso) as projection,
                                     sum(profSortRso) as sort,
                                     sum(profFilterRso) as filter,
                                     sum(profResRso) as result,
                                     sum(profDmlRso) as dml,
                                     sum(profHjRso) as hjoin,
                                     sum(profBufRso) as buffer,
                                     sum(profFlatRso) as flatten,
                                     sum(profBloomRso) as bloomfilter,
                                     sum(profAggRso) as aggregation
                             from day_te
                             where warehouseid=652600826611385061 or
warehouseid=1890180489335197062 or
warehouseid=2098601964786088346 or
warehouseid=2417460482991738329 or
warehouseid=2831213012224265896 or
warehouseid=3464746777318584173 or
warehouseid=6658612806091131055 or
warehouseid=7249649289217758973 or
warehouseid=8959409253890846472
                             group by extract(hour from timeslice), warehouseid;")
qres = sqldf("select hour, warehouseid, scan, filter, aggregation, hjoin, sort, bloomfilter, result, buffer, flatten, projection, externalscan, dml from qres")
# Transform from wide -> long
df = gather(qres, condition, value, scan:dml, factor_key=TRUE)
df$value = df$value / 1000 / 3600 / 24

df$condition = factor(df$condition)
df$warehouseid = factor(df$warehouseid)

ggplot(df, aes(fill=condition, y=value, x=hour)) + 
   geom_col() +
   scale_fill_brewer(palette="Paired") +
   theme_minimal() +
   labs(y="CPU days", x="All Warehouses", fill = "Operator", title = "Operator Usage") + theme(plot.title = element_text(hjust = 0.5)) +
   facet_wrap(~warehouseid)
ggsave("top-warehouses-by-cputime.pdf", plot = last_plot(), scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)


