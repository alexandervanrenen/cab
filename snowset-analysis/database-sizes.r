library(ggplot2)
library(parsedate)
library(tidyr)
library(sqldf)
library(grid)
library(gridExtra)

require("RPostgreSQL")
drv = dbDriver("PostgreSQL")
con = NULL
if(is.null(con)) {
   con = dbConnect(drv, dbname = "snowset",
                   host = "127.0.0.1", port = 25432,
                   user = "postgres")
}

qres = dbGetQuery(con, paste("select (select topcpu from day_topd where day_topd.databaseid = day.databaseid) as databaseid,
                                     max(scanbytes) as scanbytes,
                                     max(persistentreadbytess3 + persistentreadbytescache) as totalread
                              from day
                              group by databaseid
                              order by max(scanbytes) desc
                              limit 10;"))

# Transform from wide -> long
qres$databaseid = factor(qres$databaseid)
qres$databaseid = with(qres, reorder(databaseid, -scanbytes))
df = gather(qres, condition, value, scanbytes:scanbytes, factor_key=TRUE)

ggplot(df, aes(x=databaseid, y=value, fill=condition)) + 
   geom_col(position = position_dodge()) +
   scale_fill_brewer(palette="Paired") +
   theme_minimal()

ggsave("operators-all.pdf", plot = last_plot(), scale = 1, units = c("in", "cm", "mm", "px"), device = cairo_pdf, width = paper_width, height = 5.0)





