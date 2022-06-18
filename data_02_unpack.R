rm(list = ls())
load("data_00_variables.RData")

# Directories---------------------------------------------------------------------------------------------------------------
dir_raw_data <- paste0(dir_version,"01_raw_data/")

dir.create(dir_raw_data)

# Raw Data------------------------------------------------------------------------------------------------------------------
# We receive table_symbol.csv data which is first compressed into a tar folder,
# then this file is compressed via gzip. So we cannot directly access a table_symbol.csv
# file from R.

print("Unpacking to Raw Data.")
tic <- Sys.time()

untar(downloaded_dta_file_name, exdir = dir_raw_data)
# Note that in the data_03_rewrite each table_symbol.csv file
# in dir_raw_data is again compressed to table_symbol.csv.gz.

time_elapsed <- difftime(Sys.time(), tic, units = "secs")

print(paste0("Unpacking to Raw Data done. Time elapsed: ", round(time_elapsed, digits=0), " seconds."))