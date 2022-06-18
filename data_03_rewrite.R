rm(list = ls())
load("data_00_variables.RData")

# Libraries-----------------------------------------------------------------------------------------------------------------
library(foreach)
library(doParallel)
no.cores <- (detectCores()-1)
registerDoParallel(cores = no.cores)
library(dplyr)
library(readr)
options(readr.default_locale=readr::locale(tz="Europe/Berlin"))
library(tidyr)
library(purrr)

# Directories---------------------------------------------------------------------------------------------------------------
dir_raw_data <- paste0(dir_version,"01_raw_data/")
dir_rewritten_data <- paste0(dir_version,"02_rewritten_data/")

dir.create(dir_rewritten_data)

# Rewritten Data--------------------------------------------------------------------------------------------------------------
# Daily Folders containing one .csv.gz for every stock
print("Rewriting data.")

tic <- Sys.time()

foreach(
  ii = list.files(dir_raw_data),
  .packages = c("foreach", "dplyr", "readr", "tidyr", "purrr"),
  .combine = bind_rows
) %dopar% {

  stock_symbol <- substring(ii, first = 7, last = nchar(ii) - 4)

  tbl_stock <-
    read_csv(
      paste0(dir_raw_data, ii),
      col_names = c("date", "time", "open", "high", "low", "close",
                    "volume", "splits", "earnings", "dividends"),
      col_types = "iiddddddid"
    )

  # If we want to keep the raw data then we compress it to .csv.gz and delete the original raw data. 
  # If not we only delete original raw data.
  if(!delete_raw_data) {
    write_csv(
      tbl_stock,
      path = paste0(
        dir_raw_data, "table_", stock_symbol, ".csv.gz"
      ),
      col_names = FALSE
    )
  }
  file.remove(paste0(dir_raw_data, ii))

  tbl_stock <- distinct(tbl_stock) # Idk why Onno did this originally, but it cannot hurt.

  stock_dates <- unique(tbl_stock$date)

  foreach(jj = stock_dates) %do% {

    dir.create(paste0(dir_rewritten_data, jj), showWarnings = FALSE)

    tbl_stock %>%
      filter(date == jj) %>%
      write_csv(
        path = paste0(dir_rewritten_data, jj, "/", stock_symbol, ".csv.gz"),
        col_names = FALSE
      )

  }

  time_elapsed <- difftime(Sys.time(), tic, units = "secs")
  write_csv( # Write progress
    tibble(
      symbol = stock_symbol,
      seconds_elapsed = round(time_elapsed,0)
    ),
    paste0(
      dir_version, "rewritten_data_progress.csv"
    ),
    append = TRUE
  )

}

invisible(file.remove(paste0(dir_version, "rewritten_data_progress.csv")))

time_elapsed <- difftime(Sys.time(), tic, units = "secs")
print(paste0("Rewriting done. Time elapsed: ", round(time_elapsed, digits=0), " seconds."))
