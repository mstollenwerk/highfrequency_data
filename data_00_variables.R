rm(list = ls())

# Variables-----------------------------------------------------------------------------------------------------------------
delete_raw_data <- TRUE
delete_rewritten_data <- TRUE

dir_version <- "2021-02_SP500/" # !Has to be changed for new data! #

username <- "mktdata" # !Has to be changed for new data! #
password <- "F23AESs" # !Has to be changed for new data! #

download_url <- # !Has to be changed for new data! #
  paste0(
    "http://",
    username,":",
    password,
    "@37.35.106.38/orders/878389/order_878389.tar.gz"
  )

downloaded_dta_file_name <- paste0(dir_version, "order_878389.tar.gz") # !Has to be changed for new data! #

save.image("data_00_variables.RData")