# ./build.r
# This is the master file for ETL of data needed for marketing/sales reports
# Blake Abbenante
# 10/1/19

if (!require(tidyverse)) {
  install.packages('tidyverse') # load the base tidyverse libraries (dplyr, tidyr, etc.)
  require(tidyverse)
}
if (!require(janitor)) {
  install.packages('janitor') # functions for augmenting dataframes
  require(janitor)
}
if (!require(readr)) {
  install.packages('readr') # enhanced functions for loading data
  require(readr)
}
if (!require(RPostgreSQL)) {
  install.packages('RPostgreSQL') # connecting to PostGres instances
  require(RPostgreSQL)
}
if (!require(scales)) {
  install.packages('scales') # pretty labels
  require(scales)
}
if (!require(lubridate)) {
  install.packages('lubridate') # advanced date manipulation
  require(tidyverse)
}
if (!require(fuzzyjoin)) {
  install.packages('fuzzyjoin') # uncommon data joins
  require(fuzzyjoin)
}
if (!require(here)) {
  install.packages('here') # file referencing
  require(here)
}
if (!require(httr)) {
  install.packages('httr') # http posts
  require(httr)
}
if (!require(config)) {
  install.packages('config') # read a config file
  require(config)
}
if (!require(RiHana)) {
  devtools::install_github('RiHana') # hana functions
  require(RiHana)
}





## clear the console of all variables
rm(list = ls())
## free up unused memory
gc()


config<-config::get(file="~/OneDrive - CBRE, Inc/data/config/r_config.yaml")


hana_dates<-RiHana::get_relevant_date()

#calculate how many days in this time interval
n_days <- interval(hana_dates$yoy_date,hana_dates$yesterdays_date)/days(1)
reporting_date_range<- enframe(hana_dates$yoy_date + days(0:n_days)) %>% 
  mutate(start_week=floor_date(value,unit="week",week_start=1)) %>% 
  group_by(start_week) %>% 
  summarise(days=n()) %>% 
  rename(report_week=start_week)

#set air table API & base key
#Sys.setenv("AIRTABLE_API_KEY"=config$airtable_api)
#airtable <- airtabler::airtable("app2IyUOBxm88mSY4", "Tracking Codes") #base key can be found in the API docs
#DigitalTrackingCodes <- airtable$`Tracking Codes`$select_all()


ft_con <- dbConnect(odbc::odbc(), "fivetran", timeout = 10,bigint = c("numeric"))
edp_con <- dbConnect(odbc::odbc(), "EDP Landing", timeout = 10,bigint = c("numeric"))
seg_con <- dbConnect(odbc::odbc(), "public landing", timeout = 10,bigint = c("numeric"))


source("R/extract.R")

source("R/transform.R")

POST(url=config$my_webhook,body=get_slack_payload("Extraction Script","all good","successful"))




dbDisconnect(ft_con)
dbDisconnect(edp_con)
dbDisconnect(seg_con)

