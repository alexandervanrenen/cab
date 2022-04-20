
# Q500 Load time
load_df <- read.csv(file = '../benchmark-results/load.csv')
df = gather(load_df, condition, hours, copy_snowflake:runtime, factor_key=TRUE)
df$hours = df$hours / 3600 / 1000 / 2
ggplot(data = df) + geom_col(aes(x=condition, y=hours))

# Q501 Latencies 
s2_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_2s_1tb/all.csv')
s4_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_4s_1tb/all.csv')
s8_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_8s_1tb/all.csv')
s16_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_16s_1tb/all.csv')
all_df = sqldf("select * from (select * from s2_df) a union all (select * from s4_df) union all (select * from s8_df) union all (select * from s16_df)")
all_df = gather(all_df, key, value, start:start_delay, factor_key=TRUE)
all_df = sqldf("select * from all_df where key = 'query_duration' or key = 'query_duration_with_queue'")
all_df$value = all_df$value / 1000
all_df$key = mapvalues(factor(all_df$key), from=c("query_duration", "query_duration_with_queue"), to=c("w/o queue", "w/ queue"));
all_df$warehouse_size = mapvalues(factor(all_df$warehouse_size),
                              from=c("2", "4", "8", "16"),
                              to=c("Small\n(2 nodes)\n≈ 5.20$/h", "Medium\n(4 nodes)\n≈ 10.40$/h", "Large\n(8 nodes)\n≈ 20.80$/h", "X-Large\n(16 nodes)\n≈ 41.60$/h"));
df_meds = ddply(all_df, .(key, warehouse_size), summarise, med = median(value))
df_meds$med = sqldf("select (case when med>100 then round(med) else med end) as med from df_meds")$med
pl = ggplot(data = all_df) + geom_boxplot(aes(x=key, y=value), outlier.size = 0.1) +
   theme_minimal() +
   geom_text(data = df_meds, aes(x = key, y = med, label = round(med, 1)), size = 2, vjust = -0.5) +
   facet_grid(~factor(warehouse_size)) + scale_y_log10(labels = function(y) sprintf("%g", y)) +
   theme(
      axis.text.x = element_text(angle = 90, hjust = 1, size = 10),
      axis.ticks = element_line(),
      axis.line.y = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      axis.line = element_line(),
      panel.grid.minor = element_blank(),
      legend.position=c(0.05, 0.78),
   ) +
   labs(y="Query Latency [s]", x="Warehouse Performance"); pl

# Q502 Arrival times
df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_2s_1tb/all.csv')
df = gather(df, key, value, start:start_delay, factor_key=TRUE)
df = sqldf("select * from df where key = 'relative_start' and warehouse_size = 2")
df$value = df$value / 1000 / 60
pa = ggplot(data = df) + geom_histogram(aes(x=value), binwidth = 1) +
   theme_minimal() +
   theme(
      axis.ticks = element_line(),
      axis.line = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      panel.grid.minor = element_blank()
   ) + 
   scale_y_continuous(expand = c(0, 0), limits = c(0, NA)) +
   labs(y="Query Arrivals [per 1min]", fill = "", x="Benchmark Runtime [min]"); pa

# Q503 Queuing times
s2q_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_2s_1tb/queue.csv')
s4q_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_4s_1tb/queue.csv')
s8q_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_8s_1tb/queue.csv')
s16q_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_16s_1tb/queue.csv')
df = sqldf("select * from (select * from s2q_df) a union all (select * from s4q_df) union all (select * from s8q_df) union all (select * from s16q_df)")
df = gather(df, key, value, running:queued, factor_key=TRUE)
df$warehouse_size = mapvalues(factor(df$warehouse_size),
                              from=c("2", "4", "8", "16"),
                              to=c("Small (2 nodes)\n≈ 5.20$/h", "Medium (4 nodes)\n≈ 10.40$/h", "Large (8 nodes)\n≈ 20.80$/h", "X-Large (16 nodes)\n≈ 41.60$/h"));
pq = ggplot(data = df) + geom_col(aes(x=time_offset, y=value, fill=key)) + facet_wrap(~warehouse_size, ncol = 2) +
   theme_minimal() +
   theme(
      axis.ticks = element_line(),
      axis.line = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      panel.grid.minor = element_blank(),
      legend.position="top"
   ) + 
   scale_y_continuous(expand = c(0, 0), limits = c(0, NA)) +
   labs(y="Load", fill = "", x="Benchmark Runtime [min]"); pq

# prices = read.csv(file = '/tmp/prices.csv')
# pt = tableGrob(prices)
all = grid.arrange(pa, pq, pl, widths=c(0.25, 0.35, 0.4)); all;
ggsave("warehouse-size-experiment.pdf", plot = all, scale = 1, device = cairo_pdf, width = paper_width, height = 3.0)

# Q501 Latencies per tenant
s2_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_2s_1tb/all.csv')
s4_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_4s_1tb/all.csv')
s8_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_8s_1tb/all.csv')
s16_df <- read.csv(file = '../benchmark-results/snowflake_shared_1h_16s_1tb/all.csv')
all_df = sqldf("select * from (select * from s2_df) a union all (select * from s4_df) union all (select * from s8_df) union all (select * from s16_df)")
all_df = gather(all_df, key, value, start:start_delay, factor_key=TRUE)
all_df = sqldf("select * from all_df where key = 'query_duration_with_queue'")
all_df$value = all_df$value / 1000
all_df$warehouse_size = mapvalues(factor(all_df$warehouse_size),
                                  from=c("2", "4", "8", "16"),
                                  to=c("Small\n(2 nodes)", "Medium\n(4 nodes)", "Large\n(8 nodes)", "X-Large\n(16 nodes)"));
pi = ggplot(data = all_df) + geom_boxplot(aes(x=factor(query_stream_id), y=value), outlier.size = 0.1) + facet_grid(~ warehouse_size) +
   theme_minimal() +
   theme(
      axis.ticks = element_line(),
      axis.line.y = element_line(arrow = grid::arrow(length = unit(0.1, "cm")), size = 0.3),
      axis.line = element_line(),
      panel.grid.minor = element_blank(),
      legend.position=c(0.05, 0.78),
   ) +
   scale_y_continuous(expand = c(0, 0), limits = c(0, 500)) +
   scale_x_discrete(breaks = levels(factor(c(0,10,19)))) +
   labs(y="Query Latency [s]", x="Warehouses"); pi
ggsave("warehouse-size-experiment-tenant-breakdown.pdf", plot = pi, scale = 1, device = cairo_pdf, width = paper_width / 2, height = 2.0)
