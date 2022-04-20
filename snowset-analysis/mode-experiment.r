
cool_theme = theme_minimal() +
   theme(
      axis.ticks = element_line(),
      axis.line.y = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      axis.line.x = element_line(),
      panel.grid.minor = element_blank()
   )

# Q509 Latencies 
si_df <- read.csv(file = '../benchmark-results/snowflake_individual_1h_38s_4tb/all.csv')
ss_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_32s_4tb/all.csv')
df = sqldf("select * from (select * from si_df) x union all (select * from ss_df)")
df = gather(df, key, value, start:start_delay, factor_key=TRUE)
df = sqldf("select * from df where key = 'query_duration' or key = 'query_duration_with_queue'")
df$warehouse_size = mapvalues(factor(df$warehouse_size), from=c(32, 38), to=c("SC", "TC"));
df$key = mapvalues(factor(df$key), from=c("query_duration", "query_duration_with_queue"), to=c("w/o queue", "w/ queue"));
df$value = df$value / 1000
df_meds = ddply(df, .(warehouse_size,key), summarise, med = median(value))
df_meds$med = sqldf("select (case when med>1000 then round(med) else med end) as med from df_meds")$med
p1 = ggplot(data = df) + cool_theme + geom_boxplot(aes(x=warehouse_size, y=value), outlier.size = 0.1) + facet_grid(~key) +
      scale_y_log10(labels = function(y) sprintf("%g", y)) +
      geom_text(data = df_meds, aes(x = warehouse_size, y = med, label = round(med, 1)), size = 2, vjust = -0.5) +
      labs(y="Query Latency [s]", x="Overview"); p1
p2 = ggplot(data = df) + cool_theme + geom_boxplot(aes(x=factor(query_stream_id), y=value), outlier.size = 0.1) + facet_grid(key ~ warehouse_size) +
   scale_y_log10(labels = function(y) sprintf("%g", y)) +
   theme(axis.title.y = element_blank()) +
   scale_x_discrete(breaks = levels(factor(c(0,5,10,15,19)))) +
   labs(x="By Warehouse"); p2
p3 = ggplot(data = df) + cool_theme + geom_boxplot(aes(x=factor(query_stream_id), y=value), outlier.size = 0.1) + facet_grid(key ~ warehouse_size) +
   theme(axis.title.y = element_blank()) + 
   scale_x_discrete(breaks = levels(factor(c(0,5,10,15,19)))) +
   labs(x="By Warehouse"); p3

all = grid.arrange(p1, p2, p3, widths=c(0.25, 0.375, 0.375)); all;
ggsave("warehouse-mode-experiment.pdf", plot = all, scale = 1, device = cairo_pdf, width = paper_width, height = 3.0)
