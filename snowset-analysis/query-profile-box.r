
# -----------------------------------------------------------------------------
# Box plots
# -----------------------------------------------------------------------------

scale = "identity"
scale = "log10"

qres1 = dbGetQuery(con, "
(select 'snowset' as name, durationtotal
from day)
union all
(select workload as name, durationtotal
from tpc_queries where type = 'Normal')
")
df1 = qres1
p1 = ggplot(df1, aes(x=name, y=durationtotal)) + 
   geom_boxplot(outlier.shape=8, outlier.size=4) + 
   scale_fill_brewer(palette="Paired") +
   scale_y_continuous(trans=scale) +
   expand_limits(x=0) +
   labs(y="Duration") +
   theme_minimal(); p1

# -----------------------------------------------------------------------------

qres2 = dbGetQuery(con, "
(select 'snowset' as name, persistentReadBytesCache+persistentReadBytesS3 as scanbytes
from classification
limit 10000)
union all
(select workload as name, scanBytes
from tpc_queries where type = 'Normal')
")
df2 = qres2
p2 = ggplot(df2, aes(x=name, y=scanbytes)) + 
   geom_boxplot(outlier.shape=8, outlier.size=4) + 
   scale_fill_brewer(palette="Paired") +
   scale_y_continuous(trans=scale) +
   labs(y="Bytes Read") +
   theme_minimal(); p2

# -----------------------------------------------------------------------------

qres3 = dbGetQuery(con, "
(select 'snowset' as name, persistentWriteBytesS3 as writeBytes
from classification
limit 10000)
union all
(select workload as name, writeBytes as writeBytes
from tpc_queries where type = 'Normal')
")
df3 = qres3
p3 = ggplot(df3, aes(x=name, y=writebytes)) + 
   geom_boxplot(outlier.shape=8, outlier.size=4) + 
   scale_fill_brewer(palette="Paired") +
   scale_y_continuous(trans=scale) +
   labs(y="Bytes Written") +
   theme_minimal(); p3

all = grid.arrange(p1, p2, p3, nrow = 1); all
ggsave("query-profile-box.pdf", plot = all, scale = 1, device = cairo_pdf, width = paper_width, height = 5.0)
