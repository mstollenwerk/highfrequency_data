rm(list = ls())
load("data_00_variables.RData")

# Directories---------------------------------------------------------------------------------------------------------------
dir.create(dir_version)

# Download Data-------------------------------------------------------------------------------------------------------------

print("Downloading data.")

tic <- Sys.time()

download.file(
  url = download_url,
  destfile = downloaded_dta_file_name
)

time_elapsed <- difftime(Sys.time(), tic, units = "secs")

print(paste0("Downloading data done. Time elapsed: ", round(time_elapsed, digits=0), " seconds."))