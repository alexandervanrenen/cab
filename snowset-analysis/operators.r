# Notes:
# 1. no bandjoin (0.001%) and userdefinedtables (0) and percentile (0) .. maybe check on hole data set

# -----------------------------------------------------------------------------
# Compare operator usage in snowset vs tpch
# -----------------------------------------------------------------------------
table = "classification"
qres = dbGetQuery(con, paste("(select 'Snowset' as name,
                                     'all' as accessType,
                                     'all' as nodeCount,
                                     sum(profScanRso) as scan, sum(profFilterRso) as filter, sum(profBloomRso) as bloomfilter, sum(profHjRso) as hjoin, sum(profAggRso) as aggregation, sum(profSortRso) as sort, sum(profResRso) as result, sum(profProjRso) as projection, sum(profXtScanRso) as externalscan, sum(profDmlRso) as dml, sum(profBufRso) as buffer, sum(profFlatRso) as flatten, 0 as window
                             from ", table, ")
                        union all
                             (select 'Snowset' as name,
                                     'all' as accessType,
                                     (case when warehousesize=1 then '1' else '16' end) as nodeCount,
                                     sum(profScanRso) as scan, sum(profFilterRso) as filter, sum(profBloomRso) as bloomfilter, sum(profHjRso) as hjoin, sum(profAggRso) as aggregation, sum(profSortRso) as sort, sum(profResRso) as result, sum(profProjRso) as projection, sum(profXtScanRso) as externalscan, sum(profDmlRso) as dml, sum(profBufRso) as buffer, sum(profFlatRso) as flatten, 0 as window
                             from ", table, "
                             where warehousesize=1 or warehousesize=16
                             group by 3)
                        union all
                             (select 'Snowset' as name,
                                     wtype as accessType,
                                     (case when warehousesize=1 then '1' else '16' end) as nodeCount,
                                     sum(profScanRso) as scan, sum(profFilterRso) as filter, sum(profBloomRso) as bloomfilter, sum(profHjRso) as hjoin, sum(profAggRso) as aggregation, sum(profSortRso) as sort, sum(profResRso) as result, sum(profProjRso) as projection, sum(profXtScanRso) as externalscan, sum(profDmlRso) as dml, sum(profBufRso) as buffer, sum(profFlatRso) as flatten, 0 as window
                             from ", table, "
                             where warehousesize=1 or warehousesize=16
                             group by 2, 3)
                        union all
                             (select workload as name,
                                     'all' as accessType,
                                     (case when type='Normal' then '1' else '16' end) as nodeCount,
                                     sum(profScanRso) as scan, sum(profFilterRso) as filter, sum(profBloomRso) as bloomfilter, sum(profHjRso) as hjoin, sum(profAggRso) as aggregation, sum(profSortRso) as sort, sum(profResRso) as result, sum(profProjRso) as projection, sum(profXtScanRso) as externalscan, sum(profDmlRso) as dml, sum(profBufRso) as buffer, sum(profFlatRso) as flatten, sum(profwindow) as window
                             from tpc_queries
                             group by 1, 3)
                        union all
                             (select workload as name,
                                     (case when writebytes>0 then 'readwrite' else 'readonly' end) as accessType,
                                     (case when type='Normal' then '1' else '16' end) as nodeCount,
                                     sum(profScanRso) as scan, sum(profFilterRso) as filter, sum(profBloomRso) as bloomfilter, sum(profHjRso) as hjoin, sum(profAggRso) as aggregation, sum(profSortRso) as sort, sum(profResRso) as result, sum(profProjRso) as projection, sum(profXtScanRso) as externalscan, sum(profDmlRso) as dml, sum(profBufRso) as buffer, sum(profFlatRso) as flatten, sum(profwindow) as window
                             from tpc_queries
                             group by 1, 2, 3);"));
df = sqldf("select name, accessType, nodeCount, scan+filter as scan, aggregation, hjoin, sort, bloomfilter, projection, externalscan, dml, \"window\"+result+buffer+flatten as other from qres");

sqldf("select name, accessType, nodeCount, scan, (scan+ aggregation+ hjoin+ sort+ bloomfilter+ projection+ externalscan+ dml+ other) as everything from df where accessType = 'readonly' and nodeCount = '1'");

full_plot = function(df) {
   return(ggplot(df, aes(fill=factor(condition), y=value, x=name)) +
             theme_minimal() +
             theme(axis.ticks = element_line(),
                   axis.text.x = element_text(angle = 45, hjust = 0.8, vjust = 0.9),
                   text = element_text(size=20),
                   legend.position="none",
                   panel.grid.minor.y = element_blank(),
                   panel.grid.minor.x = element_blank(),
                   panel.grid.major.x = element_blank(),
                   axis.line = element_line()
             ) +
             scale_fill_brewer(palette="Paired", name="", breaks= c("scan", "aggregation", "hjoin", "sort", "bloomfilter", "projection", "externalscan", "dml", "other"),
                     labels = c("Scan + Filter", "Aggregation", "Join", "Sort", "Bloom Filter", "Projection", "External Scan", "DML", "Other")) +
             scale_y_continuous(labels = scales::percent, expand = c(0, 0), limits = c(0, NA))
   );
}

# Plot the distribution for the entire snowset (left column)
df1 = sqldf("select * from df where accesstype = 'all' and nodecount = 'all' and name = 'Snowset'");
df1 = gather(df1, condition, value, scan:other, factor_key=TRUE)
p1 = full_plot(df1) +
   geom_col(position = "fill") +
   expand_limits(y = c(0, 1)) +
   theme(plot.title = element_text(size = 16)) +
   labs(y="", x="") + theme(plot.title = element_text(hjust = 0.5)); p1

# Plot all the facets (everything else)
df2 = sqldf("select * from df where not(accesstype = 'all' and nodecount = 'all' and name = 'Snowset')");
df2 = gather(df2, condition, value, scan:other, factor_key=TRUE)
df2$name = factor(df2$name, levels = c("Snowset", "TPC-H", "TPC-DS"), ordered = TRUE);
facet_names = c(`1`="1 Node", `16`="16 Nodes", `all`="All", `readwrite`="Read/write", `readonly`="Read-only", `writeonly`="Write-only");
p2 = full_plot(df2) +
   geom_col(position = "fill") +
   labs(y="", x="") + theme(plot.title = element_text(hjust = 0.5)) +
   theme(panel.spacing.y = unit(2, "lines")) +
   facet_grid(rows=vars(nodecount), cols=vars(accesstype), labeller = as_labeller(facet_names), scales = "free_x"); p2

legend = cowplot::get_legend(p2 + theme(legend.position="right") + guides(fill=guide_legend(ncol=5, title.position = "left")));
my_layout <- rbind(c(3), c(1,2,2,2,2), c(1,2,2,2,2), c(1,2,2,2,2), c(1,2,2,2,2), c(1,2,2,2,2), c(1,2,2,2,2))
all = grid.arrange(p1, p2, legend, layout_matrix = my_layout); all

ggsave("operators-all.pdf", plot = all, scale = 1, device = cairo_pdf, width = paper_width, height = 5.0)



# -----------------------------------------------------------------------------
# Print operator distribution of a single query
# -----------------------------------------------------------------------------
qres = dbGetQuery(con, "select queryid::text, profScanRso as scan,
                                     profFilterRso as filter,
                                     profBloomRso as bloomfilter,
                                     profHjRso as hjoin,
                                     profAggRso as aggregation,
                                     profSortRso as sort,
                                     profResRso as result,
                                     profProjRso as projection,
                                     profXtScanRso as externalscan,
                                     profDmlRso as dml,
                                     profBufRso as buffer,
                                     profFlatRso as flatten
                             from longqueries;");
df = sqldf("select queryid, scan+filter as scan, aggregation, hjoin, sort, bloomfilter, projection, externalscan, dml, result+buffer+flatten as other from qres");

# Transform from wide -> long
df = gather(df, condition, value, scan:other, factor_key=TRUE)
df$condition = factor(df$condition)
df$queryid = factor(df$queryid, levels=c("496287533070165649",
                                         "923191307008314907",
                                         "1002954158585007193",
                                         "1940822868242618645",
                                         "1972712633403353304",
                                         "3493047743085174687",
                                         "4670710389602982751",
                                         "7710818579630789542",
                                         "7803336158646177195",
                                         "8088249991957925645",
                                         "8413111880117953104",
                                         "8910673874828560539",
                                         "9034856162185730671"), ordered= TRUE)

ggplot(df, aes(fill=condition, y=value, x=queryid)) + 
   geom_col(position = "fill") +
   scale_fill_brewer(palette="Paired", name="Operators") +
   theme(axis.text.x = element_text(angle = 90, vjust = 0.5))



qres = dbGetQuery(con, "select count(distinct warehouseid) as warehousecount from day where databaseid is not null group by databaseid;");
ggplot(data=qres, aes(x=warehousecount)) + geom_density() + scale_x_log10(breaks=c(1,2,3,4,5,6,7))



qres = dbGetQuery(con, "select queryid,
                  profsortrso sort,
                  profaggrso  agg,
                  profhjrso hjoin
from classification
where intdatawritebyteslocalssd > 0;");

get_density_sort = function(qres, xmax) {
   plot_for_buckets = ggplot(data=qres, aes(x=sort)) + geom_density() + scale_x_log10() + expand_limits(x=c(0.001, xmax)); plot_for_buckets
   extracted_data = ggplot_build(plot_for_buckets)
   x=extracted_data$data[[1]]$x
   x=10^x
   y=extracted_data$data[[1]]$y
   y2=(y*x)
   fake = data.frame(x, y, y2)
   fake2 = sqldf("select x, y / (select sum(y) from fake) as y, y2 / (select sum(y2) from fake) as y2 from fake")
   return(fake2)
}
res_sort = get_density_sort(qres, 1e9);

get_density_agg = function(qres, xmax) {
   plot_for_buckets = ggplot(data=qres, aes(x=agg)) + geom_density() + scale_x_log10() + expand_limits(x=c(0.001, xmax)); plot_for_buckets
   extracted_data = ggplot_build(plot_for_buckets)
   x=extracted_data$data[[1]]$x
   x=10^x
   y=extracted_data$data[[1]]$y
   y2=(y*x)
   fake = data.frame(x, y, y2)
   fake2 = sqldf("select x, y / (select sum(y) from fake) as y, y2 / (select sum(y2) from fake) as y2 from fake")
   return(fake2)
}
res_agg = get_density_agg(qres, 1e9);

get_density_join = function(qres, xmax) {
   plot_for_buckets = ggplot(data=qres, aes(x=hjoin)) + geom_density() + scale_x_log10() + expand_limits(x=c(0.001, xmax)); plot_for_buckets
   extracted_data = ggplot_build(plot_for_buckets)
   x=extracted_data$data[[1]]$x
   x=10^x
   y=extracted_data$data[[1]]$y
   y2=(y*x)
   fake = data.frame(x, y, y2)
   fake2 = sqldf("select x, y / (select sum(y) from fake) as y, y2 / (select sum(y2) from fake) as y2 from fake")
   return(fake2)
}
res_join = get_density_join(qres, 1e9);


ggplot(data=qres) +
   theme(legend.position="top") +
   geom_line(data=res_sort, aes(x=x, y=y2), color="red") +
   geom_line(data=res_agg, aes(x=x, y=y2), color="blue") +
   geom_line(data=res_join, aes(x=x, y=y2), color="black") +
   scale_x_log10(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1K","1M","1G","1T","1E"))
#   expand_limits(x=c(0.001, xmax))



# Q125 Plot the density of the materialization fraction of each query (with scan bytes)
qres123 = dbGetQuery(con, "select (intdatawritebyteslocalssd + intDataWriteBytesS3) * 1.0 / persistentread as fraction
from classification
where persistentread > 1e9 and (intdatawritebyteslocalssd + intDataWriteBytesS3) > 0;")
ggplot(data=qres123, aes(x=fraction)) +
   geom_density() +
   scale_x_log10(breaks = c(0.01, 0.1, 1, 10, 100), labels=c("0.01", "0.1", "1", "10", "100"), limit=c(0.0001, 100000))

# Q126 Plot the density of the materialization fraction of each query (with persistent reads)
qres = dbGetQuery(con, "select intdatawritebyteslocalssd * 1.0 / (persistentReadBytesS3 + persistentReadBytesCache) as fraction
from queries
where (persistentReadBytesS3 + persistentReadBytesCache) > 0;")
ggplot(data=qres, aes(x=fraction)) +
   geom_density() +
   scale_x_log10(breaks = c(0.01, 0.1, 1, 10, 100), labels=c("0.01", "0.1", "1", "10", "100"), limit=c(0.0001, 100000))

# Q127 Plot the density of the materialization fraction of each query (with scan bytes)
qres123 = dbGetQuery(con, "select intdatawritebyteslocalssd * 1.0 / scanBytes as fraction
from queries
where scanBytes > 0;")
ggplot(data=qres123, aes(x=fraction)) +
   geom_density() +
   scale_x_log10(breaks = c(0.01, 0.1, 1, 10, 100), labels=c("0.01", "0.1", "1", "10", "100"), limit=c(0.0001, 100000))

# Q123 Plot which warehouse is accessed from the top 100 warehouses
qres_dbs = dbGetQuery(con, "with X as (select warehouseid, count(distinct databaseid) as databasecount
           from day
           where databaseid is not null
           group by warehouseid)
select d.warehouseid, d.databaseid, count(*) as cnt, RANK() OVER (
    PARTITION BY d.warehouseid
    ORDER BY d.databaseid
) as databaserank
from X x,
     day d
where x.warehouseid = d.warehouseid
and databasecount = 2
and d.databaseid is not null
group by d.warehouseid, d.databaseid
order by count(*) desc limit 100;");
ggplot(data = qres_dbs, aes(x=factor(warehouseid), y=cnt, fill=factor(databaserank))) + geom_bar(position="stack", stat="identity")


# Q124
qres_overlap = dbGetQuery(con, "with Hours as (select * from (values(0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23)) as Hours(hour))
   , Minutes as (select * from (values(0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59)) as Minutes(minute))
   , Time as (select * from Hours, Minutes)

select t.hour,t.minute, d.warehouseid::text, t.hour*60 + t.minute  as ts, count(*) as cnt
from time t, day d
where extract('minute' from d.createdtime) < t.minute
  and extract('minute' from endtime) >= t.minute
  and extract('hour' from d.createdtime) <= t.hour
  and extract('hour' from d.endtime) >= t.hour
  and (d.warehouseid = 437357594397039648 or
    d.warehouseid = 3010473402317748718 or
    d.warehouseid = 4702844907506457136 or
    d.warehouseid = 4430008900009444093 or
    d.warehouseid = 4261629082601729297 or
    d.warehouseid = 813827367824750201 or
    d.warehouseid = 9137748609271758813 or
    d.warehouseid = 7293830039889414915 or
    d.warehouseid = 6328605299045833917 or
    d.warehouseid = 3554176433012304146)
group by 1, 2, 3;");

ggplot(data = qres_overlap, aes(x=ts, y=cnt)) + geom_line() + facet_wrap(~warehouseid,  scales = "free_y")

# Q128
qres_start_dist = dbGetQuery(con, "select warehouseid::text || ' ' || cnt::text as warehouseid, extract(epoch from createdtime) - lag(extract(epoch from createdtime)) over w as time
from day, (select warehouseid as wid, count(*) as cnt from classification_day group by warehouseid order by sum(cputime) desc limit 1) x
where warehouseid = wid
window w as (partition by warehouseid order by extract(epoch from createdtime) rows 1 preceding);");
qres_start_dist[1,"time"] = 0
sd(qres_start_dist$time)
p1 = ggplot(data = qres_start_dist, aes(x=time)) + geom_histogram(bins = 200) +
   facet_wrap(~warehouseid, scales = "free_y") +
   theme(axis.text.x = element_text(angle = 60, vjust = 0.5)) +
   scale_x_continuous(limits = c(-1, 10))

df = data.frame(val = rlnorm(26000, mean=0.505, sd=1.7))
p2 = ggplot(data = df, aes(x=val)) + geom_histogram(bins = 200) + scale_x_continuous(limits = c(-1, 10)); p2

df = data.frame(val = rexp(26000, rate = 0.7))
p3 = ggplot(data = df, aes(x=val)) + geom_histogram(bins = 200) + scale_x_continuous(limits = c(-1, 10)); p3

grid.arrange(p1, p2, p3)

# Q131
qres = dbGetQuery(con, "with database_sizes as (
    select databaseid, max(scanbytes) as size
    from classification_day
    group by databaseid)

select log(d.size)::int                                   gr,
       count(*),
       round(sum((usercputime + systemcputime) / 1e6), 0) sumcpus
from classification_day c,
     database_sizes d
where c.databaseid = d.databaseid
  and d.size > 1e6
group by 1
order by 1;");

ggplot(data = qres, aes(x=gr, y = sumcpus)) + geom_col()


# ---
df = data.frame(val = rlnorm(26000, mean=24.5, sd=3.5))
ggplot(data = df, aes(x=val)) + geom_histogram(bins = 200) +
   geom_vline(xintercept=22800000000, color=color2) +
   geom_vline(xintercept=53291000000, color=color3) +
   scale_x_continuous(breaks = c(1e3,1e6,1e9,1e12,1e15), labels=c("1KB","1MB","1GB","1TB","1PB"), limits = c(50, 1e12))


# 1: Over-provisioning
samplingMethod = function(wanted_db_count, mu, sigma, cut_off) {
   idx = 1
   n = 1000000
   dist = rnorm(n, mean=mu, sd=sigma)
   sorted_dist = sort(dist)
   result = c()
   wanted_db_count = wanted_db_count + cut_off * 2
   for(i in (1+cut_off):(wanted_db_count - cut_off)) {
      width = n/(wanted_db_count+1);
      result[idx] = sorted_dist[round(width * i)]
      idx = idx + 1
   }
   return(result)
}

# 2: Math :)
kinfOfAnalyticalMethod = function(wanted_db_count, mu, sigma, cut_off) {
   erf = function(x) {
      integrand = function(t) {exp(-(t*t))}
      return(2/sqrt(pi) * integrate(integrand, lower = 0, upper=x)$value);
   }
   erf(1 / sqrt(2))
   
   findSigmaFactor = function(ratio) {
      guess = 1
      iteration_count = 0
      res = NA
      while(iteration_count < 1000) {
         res = erf(guess / sqrt(2)) / 2
         # print(sprintf("guess %f -> %f", guess, res))
         if(round(res, digits= 4) == round(ratio, digits=4)) {
            return (guess);
         }
         if (res > ratio) {
            guess = guess * 0.95;
         } else {
            guess = guess * 1.05;
         }
         iteration_count =iteration_count + 1 
      }
      return (guess);
   }

   if(wanted_db_count %% 2 == 0) {
      result = c()
   } else {
      result = c(mu)
   }
   idx = length(result) + 1
   for(i in (1:(wanted_db_count/2) )) {
      # print(sprintf("%i -> %f -> %f", i, i/(wanted_db_count+1), sigma * findSigmaFactor(i/wanted_db_count)))
      sf = findSigmaFactor(i/((wanted_db_count+cut_off*2)+1))
      result[idx] = mu + sigma * sf
      result[idx + 1] = mu - sigma * sf
      idx = idx + 2
   }
   return(result);
}

# Plot
sampling = samplingMethod(100, 24.5, 3.5, 5)
analytical = kinfOfAnalyticalMethod(100, 24.5, 3.5, 5)
real = rnorm(10000, mean=24.5, sd=3.5)
df = data.frame(sampling, analytical)
df2 = data.frame(real)

ggplot() +
#   geom_histogram(data = df, aes(x=sampling, y=..density..), color = "red") +
   geom_histogram(data = df, aes(x=analytical, y=..density..), color = "blue") +
   geom_density(data = df2, aes(x=real), color = "black")

max(exp(sampling)) / sum((exp(sampling)))

# Arrival times
# cumsum(times)
sigma = 2
mu = log(86400 / 1000) - (sigma*sigma)/2
times = exp(samplingMethod(1000, mu, sigma, 0))
df = data.frame(times)
ggplot(data = df) + geom_density(aes(x=times))
sum(times) / 86400


qres_start_dist = dbGetQuery(con, "select warehouseid::text || ' ' || cnt::text as warehouseid, extract(epoch from createdtime) - lag(extract(epoch from createdtime)) over w as time, createdtime
from day, (select warehouseid as wid, count(*) as cnt from classification_day where warehouseid = 4430008900009444093 group by warehouseid order by sum(cputime) desc limit 16) x
where warehouseid = wid
window w as (partition by warehouseid order by extract(epoch from createdtime) rows 1 preceding);");
qres_start_dist[1,"time"] = 0

sqldf("select distinct warehouseid from qres_start_dist")

x = sqldf("select extract(hour from createdtime), count(*) from qres_start_dist group by extract(hour from createdtime)")
hours = x$count
res = c()
for(i in 1:length(hours)) {
   rate = 3600 / hours[i]
   arrivals = rexp(hours[i], rate = rate)
   res = c(res, arrivals)
}

p1 = ggplot(data = qres_start_dist, aes(x=time)) + geom_histogram(bins=200) +
   facet_wrap(~warehouseid, scales = "free_y") +
   theme(axis.text.x = element_text(angle = 60, vjust = 0.5)) +
   scale_x_continuous(limits = c(-1, 10)); p1

rate = 86400 / length(qres_start_dist$time);
df = data.frame(exponential_dist = rexp(26000, rate = rate))
p3 = ggplot(data = df, aes(x=exponential_dist)) + geom_histogram(bins = 200) + scale_x_continuous(limits = c(-1, 10)); p3

df = data.frame(partitioned_exponential_dist = res)
p5 = ggplot(data = df, aes(x=partitioned_exponential_dist)) + geom_histogram(bins = 200) + scale_x_continuous(limits = c(-1, 10)); p5

grid.arrange(p1, p3, p5, ncol=1)



dist = sqldf("select distinct time from qres_start_dist where time > 0")
dist = log(dist$time)
dist = rnorm(100000, mean = 100, sd = 10)
ks.test(dist, "pexp", rate=0.7)






# Q203 cputime <-> profall correlation
qres = dbGetQuery(con, "select cputime, profall from classification_day where cputime > 1e6 and profall > 0 limit 100000;")
ggplot(data=qres) + geom_bin2d(aes(x=cputime, y=profall)) + scale_x_log10() + scale_y_log10()


#Q205
qres_db = dbGetQuery(con, "
    select warehouseid,
       round(sum(case when wtype = 'readonly' then 1 else 0 end) * 100.0 / count(*))  as readonly,
       round(sum(case when wtype = 'writeonly' then 1 else 0 end) * 100.0 / count(*)) as writeonly,
       round(sum(case when wtype = 'readwrite' then 1 else 0 end) * 100.0 / count(*)) as readwrite
from classification_day
where warehouseid in (select warehouseid from classification_day group by 1 order by sum(cputime) desc limit 100)
group by 1
order by 1
");

ggplot(data = qres_db) + geom_histogram(aes(x=readonly), bins = 10)
