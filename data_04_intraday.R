rm(list = ls())
load("data_00_variables.RData")

# Libraries-----------------------------------------------------------------------------------------------------------------
library(foreach)
library(doParallel)
no.cores <- (detectCores() - 1)
registerDoParallel(cores = no.cores)
library(dplyr)
library(readr)
options(readr.default_locale = readr::locale(tz = "Europe/Berlin"))
library(tidyr)
library(purrr)
library(lubridate)

# Directories---------------------------------------------------------------------------------------------------------------
dir_rewritten_data <- paste0(dir_version,"02_rewritten_data/")

dir_processed_data <- paste0(dir_version,"03_processed_data/")
dir.create(dir_processed_data)

dir.create(paste0(dir_processed_data,"intraday_data/"))

# Processed Data--------------------------------------------------------------------------------------------------------------

#
#
# All Data
#
#
print("Processing: Intraday data.")

tic <- Sys.time()

foreach(
  ii = list.files(dir_rewritten_data),
  .packages = c("foreach", "dplyr", "readr", "tidyr", "purrr", "lubridate"),
  .combine = bind_rows
) %dopar% {

  list.files(paste0(dir_rewritten_data,ii)) %>%
    # Naming is needed since the .id in map_df uses the input list names.
    set_names(substr(.,1,nchar(.)-7)) %>%
    map_df(
      ~read_csv(
        paste0(dir_rewritten_data,ii,"/",.),
        col_names = c("date", "time", "open", "high", "low", "close",
                      "volume", "splits", "earnings", "dividends"),
        col_types = "iiddddddid"
      ),
      .id = "symbol"
    ) %>% 
    mutate(
      dt = ymd_hm( # dt column stands for date/time
        paste0(
          substring(date, 1, 4),
          "-",
          substring(date, 5, 6),
          "-",
          substring(date, 7, 8),
          " ",
          substring(time, 1, nchar(time) - 2),
          ":",
          substring(time, nchar(time) - 1, nchar(time))
        )
      )
    ) %>% 
	select(dt, everything()) %>%
    write_rds(
      paste0(dir_processed_data,"intraday_data/",ii,".rds"),
      compress = "gz"
    )
  
  if(delete_rewritten_data) {
    file.remove(paste0(dir_rewritten_data,ii))
  }

  time_elapsed <- difftime(Sys.time(), tic, units = "secs")
  write_csv( # Write progress
    tibble(
      date = ii,seconds_elapsed = round(time_elapsed,0)
    ),
    paste0(
      dir_processed_data, "intraday_data_progress.csv"
    ),
    append = TRUE
  )

}

invisible(file.remove(paste0(dir_processed_data, "intraday_data_progress.csv")))

time_elapsed <- difftime(Sys.time(), tic, units = "secs")
print(paste0("Processing: Intraday data. Done. Time elapsed: ", round(time_elapsed, digits = 0), " seconds."))
