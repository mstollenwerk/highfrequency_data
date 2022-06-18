rm(list = ls())
load("data_00_variables.RData")

# Libraries----
library(foreach)
library(doParallel)
no.cores <- (detectCores()-1)
registerDoParallel(cores = no.cores)
library(dplyr)
library(readr)
library(tidyr)
library(purrr)
library(lubridate)
library(zoo)
library(hms)

nobs_dta <- foreach(
  ii = list.files(paste0(dir_version,"03_processed_data/intraday_data"), full.names = TRUE),
  .packages = c("foreach", "dplyr", "readr", "tidyr", "purrr", "lubridate","zoo"),
  .combine = bind_rows
) %dopar% {

  read_rds(ii) %>%
    select(dt,symbol,close) %>%
    group_by(symbol) %>%
    summarise(date = unique(date(dt)), nobs = n())
} %>%
  write_rds("data_05_symbol_liquidity_dta.rds")

nobs_dta <- read_rds("data_05_symbol_liquidity_dta.rds")

# Rank by overall liquidity
nobs <- nobs_dta %>% 
  # Decimal pricing had been fully implemented in the NYSE on 2001-01-29
  filter(date > "2001-01-29") %>%
  group_by(symbol) %>% 
  summarise(nobs_overall = sum(nobs), ndays = n()) %>% 
  arrange(desc(ndays),desc(nobs_overall))

filter(nobs,ndays == max(ndays)) %>% 
  head(50) %>% 
  sample_n(5) %>% 
  select(symbol) %>% 
  unlist(use.names = FALSE) ->
  symbols