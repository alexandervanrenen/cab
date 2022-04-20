library(ggplot2)
library(parsedate)
library(tidyr)
library(sqldf)
library(grid)
library(gridExtra)
library(cowplot)
library(Cairo)
library(plyr)
library(lubridate)
library(ggplotify)
library(nimble)
library(splines2)

require("RPostgreSQL")
drv = dbDriver("PostgreSQL")
con = NULL
if(is.null(con)) {
   con = dbConnect(drv, dbname = "snowset",
                   host = "127.0.0.1", port = 25432,
                   user = "postgres")
}

paper_width = 8.47415

color1 = "#377EB8"
color2 = "#E41A1C"
color3 = "#349630"
color4 = "#1A1A1A"
color5 = "#371BE4"
color6 = "#863300"