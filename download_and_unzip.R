
library(tidyverse)
library(httr)
library(glue)
library(zip)

# Load the CSV file
csv_path <- 'get auxiliary data/dates_elections_spain.csv'
df <- read_csv(csv_path)

# Base URL for downloading files
base_url <- "https://infoelectoral.interior.gob.es/estaticos/docxl/apliextr/"

# Check for new rows with cod_elec 01 or 02
new_rows <- df %>% filter(cod_elec %in% c("01", "02"))

total_rows <- nrow(new_rows)

# Step 1: Download all files
for (i in 1:total_rows) {
  cod_elec <- new_rows$cod_elec[i]
  year <- new_rows$year[i]
  month <- new_rows$month[i]
  type_elec <- new_rows$type_elec[i]
  
  election_dir <- glue("{cod_elec}{year}{sprintf('%02d', month)}")
  file_url <- glue("{base_url}{election_dir}_MESA.zip")
  
  # Print progress report
  message(glue("Processing {i} of {total_rows}: {file_url}"))
  start_time <- Sys.time()
  
  # Download the file
  response <- GET(file_url)
  download_time <- Sys.time() - start_time
  message(glue("Download time for {file_url}: {download_time} seconds"))
  
  if (status_code(response) == 200) {
    zip_path <- glue("get auxiliary data/{election_dir}_MESA.zip")
    writeBin(content(response, "raw"), zip_path)
    message(glue("Downloaded {zip_path}"))
  } else {
    message(glue("Failed to download {file_url}"))
  }
  
  # Print progress report after each file
  message(glue("Completed {i} of {total_rows}"))
}

# Step 2: Unzip all downloaded files
for (i in 1:total_rows) {
  cod_elec <- new_rows$cod_elec[i]
  type_elec <- new_rows$type_elec[i]
  year <- new_rows$year[i]
  month <- new_rows$month[i]
  
  election_dir <- glue("{cod_elec}{year}{sprintf('%02d', month)}")
  zip_path <- glue("get auxiliary data/{election_dir}_MESA.zip")
  
  if (file.exists(zip_path)) {
    extract_dir <- glue("get auxiliary data/{cod_elec}-{type_elec}")
    dir.create(extract_dir, showWarnings = FALSE)
    unzip(zip_path, exdir = extract_dir)
    
    # Remove the zip file
    file.remove(zip_path)
    message(glue("Unzipped {zip_path} to {extract_dir}"))
  } else {
    message(glue("{zip_path} does not exist"))
  }
}
