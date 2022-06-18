# Libraries----
library(foreach)
library(doParallel)
no.cores <- (detectCores() - 1)
registerDoParallel(cores = no.cores)
library(plyr)
library(dplyr)
library(readr)
library(tidyr)
library(purrr)
library(lubridate)
library(zoo)
library(hms)


# Helper Functions----
log_ret <- function(prices,window) {
  (log(prices) - log(dplyr::lag(prices,(window - 1))))
}

clean_mins <- function(previous_tick_fixed_clock,close_max_grid) {
  
  one_or_NA <- as.matrix(close_max_grid[,-1])
  one_or_NA[!is.na(one_or_NA)] <- 1
  
  # 1. ----
  # Make all entries NA until all symbols have been observed once.
  lengths_of_series_1 <- apply(one_or_NA, 2, function(x) rle(is.na(x))$lengths[1])
  values_of_series_1 <- apply(one_or_NA, 2, function(x) rle(is.na(x))$values[1])
  ii <- max(lengths_of_series_1*values_of_series_1)
  one_or_NA[sequence(ii),] <- NA
  
  # Make subsequent rows all NAs until for the first time 75% of symbols-mins  
  # are observed in the following 1 and 5 min interval. [Likely the opening minute.]
  row_NA_means <- rowMeans(is.na(one_or_NA))
  
  ii <- ii  + 1
  while ((mean(row_NA_means[ii:(ii + 4)]) >= .25) | (row_NA_means[ii] >= .25)) {
    one_or_NA[ii,] <- NA
    ii <- ii  + 1
  }
  
  # 2. ----
  # Now do the same as in 1. on upside down matrix to find the closing minute.
  one_or_NA <- apply(one_or_NA,2,rev)
  
  # Make all entries NA until all symbols have been observed once.
  lengths_of_series_1 <- apply(one_or_NA, 2, function(x) rle(is.na(x))$lengths[1])
  values_of_series_1 <- apply(one_or_NA, 2, function(x) rle(is.na(x))$values[1])
  ii <- max(lengths_of_series_1*values_of_series_1)
  one_or_NA[sequence(ii),] <- NA
  
  # Make subsequent rows all NAs until for the first time 75% of symbols-mins  
  # are observed in the following 1 and 5 min interval. [Likely the opening minute.]
  row_NA_means <- rowMeans(is.na(one_or_NA))
  
  ii <- ii  + 1
  while ((mean(row_NA_means[ii:(ii + 4)]) >= .25) | (row_NA_means[ii] >= .25)) {
    one_or_NA[ii,] <- NA
    ii <- ii  + 1
  }
  
  # Flip the matrix again upside down.
  one_or_NA <- apply(one_or_NA,2,rev)
  
  # 3. ----
  # Eliminate remaining mins if any symbol has 10 or more consecutive NAs
  lengths_of_series <- alply(one_or_NA, 2, function(x) rle(is.na(x))$lengths)
  values_of_series <- alply(one_or_NA, 2, function(x) rle(is.na(x))$values)

  NAind <- rep(FALSE, nrow(one_or_NA))
  for (ii in 1:length(lengths_of_series)) {
    NAind_ii <-
      rep(lengths_of_series[[ii]] >= 10 & values_of_series[[ii]], lengths_of_series[[ii]])
    NAind <- NAind | NAind_ii
  }

  one_or_NA[NAind,] <- NA
  # 
  # # 4.----
  # # If there are any mins remaining where more than 90% of symobls have NAs
  # # eliminate those.
  # row_NA_means <- rowMeans(is.na(one_or_NA))
  # one_or_NA[row_NA_means > .9,] <- NA
  
  # Make remaining rows (less than or equal to 20% NAs) all ones
  row_NA_means <- rowMeans(is.na(one_or_NA))
  one_or_NA[row_NA_means != 1,] <- 1
  
  # Create output
  previous_tick_fixed_clock[,-1] <- previous_tick_fixed_clock[,-1]*one_or_NA
  previous_tick_fixed_clock
}

# Main Function----
rcov_5min <- function(symbols,dir_version,dir_output) {
  
  tic <- print(Sys.time())
  
  list_files <- list.files(
    paste0(dir_version,"03_processed_data/intraday_data"),
    full.names = TRUE
  )
  
  dates <- 
    as_date(
      substring(
        list_files,
        nchar(list_files) - 11,
        nchar(list_files) - 3
      )
    )
  
  # on 2001-01-29 the nyse introduced decimal pricing.
  list_files <- list_files[dates >= "2001-01-29"]
  dates <- dates[dates >= "2001-01-29"]
  
  # list_files <- list_files[dates >= "2001-01-01"]
  # dates <- dates[dates >= "2001-01-01"]
  
  out_ <- foreach(
    ii = list_files,
    .packages = c("foreach", "dplyr", "readr", "tidyr", "purrr", "lubridate", "zoo", "hms", "plyr"),
    .export = c("log_ret", "clean_mins")
  ) %dopar% {
    
    close_dta <- 
      read_rds(ii) %>%
      select(dt,symbol,close) %>%
      filter(symbol %in% symbols)
    
    # If one symbol is not available in the day, skip this day
    if (any(!(symbols %in% unique(close_dta$symbol)))) {
      return()
    }
    
    complete_1min_grid <- 
      seq.POSIXt(
        as.POSIXlt(min(close_dta$dt), tz = "UTC"),
        as.POSIXlt(max(close_dta$dt), tz = "UTC"),
        by = "1 min"
      ) %>% 
      tibble(dt = .)
    
    close_max_grid <- 
      close_dta %>%
      pivot_wider(
        names_from = symbol,
        values_from = close
      ) %>%
      right_join(
        complete_1min_grid,
        by = "dt"
      )
    
    previous_tick_fixed_clock <-
      close_max_grid %>% 
      mutate(dt = dt + seconds(59) + milliseconds(999)) %>% 
      mutate_at(vars(-dt),na.locf,na.rm = FALSE)
    
    previous_tick_clean <- 
      clean_mins(
        previous_tick_fixed_clock,
        close_max_grid
      )
    
    log_ret_otc <- 
      previous_tick_clean %>% 
      drop_na() %>%
      # Filtering impossible open mins in case clean_mins fails.
      filter(as_hms(dt) >= as_hms("09:30:00")) %>% 
      filter(as_hms(dt) < as_hms("16:00:00")) %>% 
      filter(row_number() == 1 | row_number() == n()) %>% 
      mutate_at(vars(-dt), function(x) {(log(x) - log(lag(x)))}) %>% 
      filter(row_number() == n())
    
    log_ret_5min <- 
      previous_tick_clean %>% 
      mutate_at(vars(-dt), log_ret, 5) %>% 
      drop_na() %>% 
      # Filtering impossible open mins in case clean_mins fails.
      filter(as_hms(dt) >= as_hms("09:35:00")) %>% 
      filter(as_hms(dt) < as_hms("16:00:00"))
    
    rc_dta <- as.matrix(log_ret_5min[,-1])
    mins <- log_ret_5min[,1]
    
    if (nrow(rc_dta)<ncol(rc_dta)) stop(ii)
    
    # See Sheppard-Part in Handbook of Volatility Models for scaling, Formula 4.27
    # m = 390, n = 5. Dividing by (m - n - 1) i.e. the number of summands gives the 
    # "average n minute RCOV", dividing by "n" brings it to "average 1 minute RCOV",
    # multiplying by 252 * 24 * 60 brings it to "annualized trading hours RCOV" scale.
    rc <- t(rc_dta) %*% rc_dta / nrow(rc_dta) * 252 * 24 * 60 / 5
    vech_rc <- rc[!upper.tri(rc)]
    
    list(
      vech_rc = vech_rc,
      mins = mins,
      log_ret_otc = log_ret_otc,
      log_ret_5min = log_ret_5min
    )
  }
  names(out_) <- dates
  
  # Write data to csv files
  vech_rc <- t(sapply(out_, `[[`, 1))
  mins <- map_df(out_, function(x) {x[[2]]})
  log_ret_otc <- map_df(out_, function(x) {x[[3]]})
  log_ret_5min <- map_df(out_, function(x) {x[[4]]})
  
  dir.create(
    dir_output
  )
  
  # Write data
  write_csv(
    tibble(sort(symbols)),
    file = paste0(
      dir_output,
      "/symbols.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    tibble(dates = dates),
    file = paste0(
      dir_output,
      "/dates.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    log_ret_otc[,-1],
    file = paste0(
      dir_output,
      "/log_ret_otc.csv"
    ),
    col_names = FALSE
  )   
  
  write_csv(
    log_ret_5min,
    file = paste0(
      dir_output,
      "/log_ret_5min.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    mins,
    file = paste0(
      dir_output,
      "/mins.csv"
    ),
    col_names = FALSE
  ) 
  
  write_csv(
    as_tibble(vech_rc, .name_repair = "minimal"),
    file = paste0(
      dir_output,
      "/vech_rc.csv.gz"
    ),
    col_names = FALSE
  )
  
  toc <- print(Sys.time()) 
  print(paste0("Processing: Realized covariances. Done. Time elapsed: "))
  print(toc - tic) 
}

rcov_15min <- function(symbols,dir_version,dir_output) {
  
  tic <- print(Sys.time())
  
  list_files <- list.files(
    paste0(dir_version,"03_processed_data/intraday_data"),
    full.names = TRUE
  )
  
  dates <- 
    as_date(
      substring(
        list_files,
        nchar(list_files) - 11,
        nchar(list_files) - 3
      )
    )
  
  # on 2001-01-29 the nyse introduced decimal pricing.
  list_files <- list_files[dates >= "2001-01-29"]
  dates <- dates[dates >= "2001-01-29"]
  
  # list_files <- list_files[dates >= "2001-01-01"]
  # dates <- dates[dates >= "2001-01-01"]
  
  out_ <- foreach(
    ii = list_files,
    .packages = c("foreach", "dplyr", "readr", "tidyr", "purrr", "lubridate", "zoo", "hms", "plyr"),
    .export = c("log_ret", "clean_mins")
  ) %dopar% {
    
    close_dta <- 
      read_rds(ii) %>%
      select(dt,symbol,close) %>%
      filter(symbol %in% symbols)
    
    # If one symbol is not available in the day, skip this day
    if (any(!(symbols %in% unique(close_dta$symbol)))) {
      return()
    }
    
    complete_1min_grid <- 
      seq.POSIXt(
        as.POSIXlt(min(close_dta$dt), tz = "UTC"),
        as.POSIXlt(max(close_dta$dt), tz = "UTC"),
        by = "1 min"
      ) %>% 
      tibble(dt = .)
    
    close_max_grid <- 
      close_dta %>%
      pivot_wider(
        names_from = symbol,
        values_from = close
      ) %>%
      right_join(
        complete_1min_grid,
        by = "dt"
      )
    
    previous_tick_fixed_clock <-
      close_max_grid %>% 
      mutate(dt = dt + seconds(59) + milliseconds(999)) %>% 
      mutate_at(vars(-dt),na.locf,na.rm = FALSE)
    
    previous_tick_clean <- 
      clean_mins(
        previous_tick_fixed_clock,
        close_max_grid
      )
    
    log_ret_otc <- 
      previous_tick_clean %>% 
      drop_na() %>%
      # Filtering impossible open mins in case clean_mins fails.
      filter(as_hms(dt) >= as_hms("09:30:00")) %>% 
      filter(as_hms(dt) < as_hms("16:00:00")) %>% 
      filter(row_number() == 1 | row_number() == n()) %>% 
      mutate_at(vars(-dt), function(x) {(log(x) - log(lag(x)))}) %>% 
      filter(row_number() == n())
    
    log_ret_15min <- 
      previous_tick_clean %>% 
      mutate_at(vars(-dt), log_ret, 15) %>% 
      drop_na() %>% 
      # Filtering impossible open mins in case clean_mins fails.
      filter(as_hms(dt) >= as_hms("09:45:00")) %>% 
      filter(as_hms(dt) < as_hms("16:00:00"))
    
    rc_dta <- as.matrix(log_ret_15min[,-1])
    mins <- log_ret_15min[,1]
    
    if (nrow(rc_dta)<ncol(rc_dta)) stop(ii)
    
    # See Sheppard-Part in Handbook of Volatility Models for scaling, Formula 4.27
    # m = 390, n = 15. Dividing by (m - n - 1) i.e. the number of summands gives the 
    # "average n minute RCOV", dividing by "n" brings it to "average 1 minute RCOV",
    # multiplying by 252 * 24 * 60 brings it to "annualized trading hours RCOV" scale.
    rc <- t(rc_dta) %*% rc_dta / nrow(rc_dta) * 252 * 24 * 60 / 15
    vech_rc <- rc[!upper.tri(rc)]
    
    list(
      vech_rc = vech_rc,
      mins = mins,
      log_ret_otc = log_ret_otc,
      log_ret_15min = log_ret_15min
    )
  }
  names(out_) <- dates
  
  # Write data to csv files
  vech_rc <- t(sapply(out_, `[[`, 1))
  mins <- map_df(out_, function(x) {x[[2]]})
  log_ret_otc <- map_df(out_, function(x) {x[[3]]})
  log_ret_15min <- map_df(out_, function(x) {x[[4]]})
  
  dir.create(
    dir_output
  )
  
  # Write data
  write_csv(
    tibble(sort(symbols)),
    file = paste0(
      dir_output,
      "/symbols.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    tibble(dates = dates),
    file = paste0(
      dir_output,
      "/dates.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    log_ret_otc[,-1],
    file = paste0(
      dir_output,
      "/log_ret_otc.csv"
    ),
    col_names = FALSE
  )   
  
  write_csv(
    log_ret_15min,
    file = paste0(
      dir_output,
      "/log_ret_15min.csv"
    ),
    col_names = FALSE
  )
  
  write_csv(
    mins,
    file = paste0(
      dir_output,
      "/mins.csv"
    ),
    col_names = FALSE
  ) 
  
  write_csv(
    as_tibble(vech_rc, .name_repair = "minimal"),
    file = paste0(
      dir_output,
      "/vech_rc.csv.gz"
    ),
    col_names = FALSE
  )
  
  toc <- print(Sys.time()) 
  print(paste0("Processing: Realized covariances. Done. Time elapsed: "))
  print(toc - tic) 
}