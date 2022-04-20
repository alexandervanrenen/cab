
levels = c("exload", "dml", "rw-scan", "rw-agg", "rw-join", "rw-sort", "rw-rest", "ro-scan", "ro-rest")

# Get the overall time and count
# ------------------------------
qres1 = dbGetQuery(con, "(select 'Snowset' as workload, qtype, sum(cputime) as cputime, count(*) as cnt
                          from classification
                          group by qtype)
                         union all
                          (select workload, qtype, 0 as cputime, count(*) as cnt
                           from classification_tpc where type = 'Normal'
                           group by workload, qtype)
                   ")
df1 = gather(qres1, condition, value, cputime:cnt, factor_key=TRUE)
df1 = sqldf("select * from df1 where not (workload = 'TPC-H' and condition = 'cputime')");
df1 = sqldf("select * from df1 where not (workload = 'TPC-DS' and condition = 'cputime')");
df1 = sqldf("select workload || '-' || condition as name, value, qtype from df1");
df1$qtype = factor(df1$qtype, levels = levels, ordered = TRUE)

p1=ggplot(df1, aes(fill=qtype, y=value, x=name)) +
   geom_col(position = "fill") +
   scale_y_continuous(labels = scales::percent) +
   scale_fill_brewer(palette="Paired", name="Query Types", breaks= c("exload", "dml", "rw-scan", "rw-rest"),
                     labels = c("External Load", "DML", "Scan", "OLAP")) +
   labs(y="Ratio") +
   theme_minimal() + theme(axis.text.x = element_text(angle = 60, vjust = 0.5), text = element_text(size=20)) + theme(legend.position = "none", axis.title.x=element_blank()); p1

ggsave("query-types-bars.pdf", plot = p1, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)


# Get the count per time bucket
# -----------------------------
qres2 = dbGetQuery(con, paste("select (usercputime + systemcputime) / 1000000.0 as cputime,
                                    qtype
                              from classification
                              "))

df2 = qres2
df2$qtype = factor(df2$qtype, levels = levels, ordered = TRUE)

p2 = ggplot(df2, aes(x=cputime, fill=qtype)) + 
   geom_histogram(bins=30) +
   scale_x_log10(breaks = c(0.01, 1,60,60*60,60*60*24), labels=c("10ms","1s","1min","1h","1d"), limits = c(NA, 2*24*60*60)) + 
   scale_y_continuous(breaks = c(0, 2e6, 4e6, 6e6), labels=c("0", "2M", "4M", "6M")) +
   scale_fill_brewer(palette="Paired", name="Query Types", breaks= c("exload", "dml", "rw-scan", "rw-rest"),
                     labels = c("External Load", "DML", "Scan Query", "OLAP")) +
   labs(y="#Queries", x = "CPU Time") +
   theme_minimal() + theme(text = element_text(size=20)); p2

p=plot_grid(p1, p2, align = "h", rel_widths = c(1, 5)); p

ggsave("query-types.pdf", plot = p, scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)
