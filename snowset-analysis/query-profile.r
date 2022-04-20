
table = "queries"

get_density = function(qres, xmax) {
   plot_for_buckets = ggplot(data=qres, aes(x=value)) + geom_density() + scale_x_log10() + expand_limits(x=c(0.001, xmax)); plot_for_buckets
   extracted_data = ggplot_build(plot_for_buckets)
   x=extracted_data$data[[1]]$x
   x=10^x
   y=extracted_data$data[[1]]$y
   y2=(y*x) / 3600 / 24
   fake = data.frame(x, y, y2)
   fake2 = sqldf("select x, y / (select sum(y) from fake) as y, y2 / (select sum(y2) from fake) as y2 from fake")
   return(fake2)
}

plot_style = function() {
   return(ggplot() +
      theme_minimal() +
      theme(axis.ticks = element_line(),
            legend.position="none",
            panel.grid.minor.y = element_blank(),
            panel.grid.minor.x = element_blank(),
            axis.line = element_line(arrow = grid::arrow(length = unit(0.1, "cm")))
            )
      );
}

# Duration plot
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

qres_snowset = dbGetQuery(con, paste("select durationTotal / 1000.0 as value from ", table, ";"));
res_snowset = get_density(qres_snowset, 5*60*60)
qres_tpch = dbGetQuery(con, "select durationTotal / 1000.0 as value from tpc_queries where type = 'Normal' and workload = 'TPC-H'");
res_tpch = get_density(qres_tpch, 5*60*60)
qres_tpcds = dbGetQuery(con, "select durationTotal / 1000.0 as value from tpc_queries where type = 'Normal' and workload = 'TPC-DS'");
res_tpcds = get_density(qres_tpcds, 5*60*60)

p_duration_1 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y, color="Snowset")) +
   geom_line(data=res_tpch, aes(x=x, y=y, color="TPC-H (SF=100)")) +
   geom_line(data=res_tpcds, aes(x=x, y=y, color="TPC-DS (SF=100)")) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="Density", x="") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(0.001, 1,60,60*60), labels=c("1ms","1s","1min","1h"), limits = c(NA, 5*60*60))

p_duration_2 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y2, color="Snowset")) +
   geom_line(data=res_tpch, aes(x=x, y=y2, color="TPC-H (SF=100)")) +
   geom_line(data=res_tpcds, aes(x=x, y=y2, color="TPC-DS (SF=100)")) +
   scale_color_manual(name = "Datasets", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="Density * X", x="Duration [log]") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(0.001, 1,60,60*60), labels=c("1ms","1s","1min","1h"), limits = c(NA, 5*60*60))

p_duration = grid.arrange(p_duration_1, p_duration_2, nrow = 2)

# CPU time plots
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

qres_snowset = dbGetQuery(con, paste("select (usercputime + systemcputime) / 1000000.0 as value from ", table, ";"));
res_snowset = get_density(qres_snowset, 365*24*60*60)

p_cputime_1 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y, color="Snowset")) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(0.001, 1,60,60*60,60*60*24,30*60*60*24,365*60*60*24), labels=c("1ms","1s","1min","1h","1d","1m","1y"), limits = c(0.001, 365*24*60*60))

p_cputime_2 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y2, color="Snowset")) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="CPU Time [log]") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(0.001, 1,60,60*60,60*60*24,30*60*60*24,365*60*60*24), labels=c("1ms","1s","1min","1h","1d","1m","1y"), limits = c(0.001, 365*24*60*60))
p_cputime = grid.arrange(p_cputime_1, p_cputime_2, nrow = 2)

# Read size plot
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

qres_snowset = dbGetQuery(con, paste("select persistentReadBytesS3+persistentReadBytesCache as value from ", table, ";"));
res_snowset = get_density(qres_snowset, 2e15)
qres_tpch = dbGetQuery(con, "select scanbytes as value from tpc_queries where type = 'Normal' and workload = 'TPC-H'");
res_tpch = get_density(qres_tpch, 2e15)
qres_tpcds = dbGetQuery(con, "select scanbytes as value from tpc_queries where type = 'Normal' and workload = 'TPC-DS'");
res_tpcds = get_density(qres_tpcds, 2e15)

p_read_1 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y, color="Snowset")) +
   geom_line(data=res_tpch, aes(x=x, y=y, color="TPC-H (SF=100)")) +
   geom_line(data=res_tpcds, aes(x=x, y=y, color="TPC-DS (SF=100)")) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1KB","1MB","1GB","1TB","1PB"), limits = c(1e3, 2e15))

p_read_2 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y2, color="Snowset")) +
   geom_line(data=res_tpch, aes(x=x, y=y2, color="TPC-H (SF=100)")) +
   geom_line(data=res_tpcds, aes(x=x, y=y2, color="TPC-DS (SF=100)")) +
   scale_color_manual(name = "Datasets", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="Read Size [log]") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1KB","1MB","1GB","1TB","1PB"), limits = c(1e3, 2e15))

p_read = grid.arrange(p_read_1, p_read_2, nrow = 2)

# Database size plot
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

qres_snowset = dbGetQuery(con, paste("select max(scanbytes) as value from ", table," group by databaseid"));
res_snowset = get_density(qres_snowset, 100e15)

p_dbsize_1 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y, color="Snowset")) +
   geom_vline(xintercept=22800000000, color=color2) +
   geom_vline(xintercept=53291000000, color=color3) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1KB","1MB","1GB","1TB","1PB"), limits = c(50, 100e15))

p_dbsize_2 = plot_style() +
   geom_line(data=res_snowset, aes(x=x, y=y2, color="Snowset")) +
   geom_vline(xintercept=22800000000, color=color2) +
   geom_vline(xintercept=53291000000, color=color3) +
   scale_color_manual(name = "Datasets:", values = c("Snowset" = color1, "TPC-H (SF=100)" = color2, "TPC-DS (SF=100)" = color3)) +
   labs(y="", x="Active DB Size [log]") +
   scale_y_continuous(breaks = c(0), labels=c("0")) +
   scale_x_log10(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1KB","1MB","1GB","1TB","1PB"), limits = c(50, 100e15))

p_dbsize = grid.arrange(p_dbsize_1, p_dbsize_2, nrow = 2)

# Combine everything for world domination 8)
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

legend = cowplot::get_legend(p_dbsize_2 + theme(legend.position="top"));
my_layout <- rbind(c(1,1,1,1), c(2:5), c(2:5), c(2:5), c(2:5), c(2:5))
all = grid.arrange(legend, p_duration, p_cputime, p_read, p_dbsize, layout_matrix = my_layout); all

ggsave("query-profile.pdf", plot = all, scale = 1, device = cairo_pdf, width = paper_width, height = 2.3)
