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

for (i in 1:nrow(new_rows)) {
  cod_elec <- new_rows$cod_elec[i]
  year <- new_rows$year[i]
  month <- new_rows$month[i]
  type_elec <- new_rows$type_elec[i]
  
  election_dir <- glue("{cod_elec}{year}{sprintf('%02d', month)}")
  file_url <- glue("{base_url}{election_dir}_MESA.zip")
  
  # Download the file
  response <- GET(file_url)
  if (status_code(response) == 200) {
    zip_path <- glue("{election_dir}_MESA.zip")
    writeBin(content(response, "raw"), zip_path)
    
    # Unzip the file
    extract_dir <- glue("{cod_elec}-{type_elec}")
    dir.create(extract_dir, showWarnings = FALSE)
    unzip(zip_path, exdir = extract_dir)
    
    # Remove the zip file
    file.remove(zip_path)
    message(glue("Downloaded and extracted {zip_path} to {extract_dir}"))
  } else {
    message(glue("Failed to download {file_url}"))
  }
}
