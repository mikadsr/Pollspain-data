source("./import raw data/import_raw_data_functions.R")
#check, install if necessary, and load the required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr,purrr,glue,tidyr,readr,tibble)
###########################################################

# IMPORT CANDIDACIES POLL DATA----

## ----- poll data congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidacies_poll_congress <- congress_elec %>%
  rowwise() %>%
  reframe(import_raw_candidacies_poll_file(type_elec, year, month)) %>%
  select(-any_of("value"))

## ----- poll data senate -----
senate_elec <- dates_elections_spain %>%
  filter(cod_elec == "03" & year >= 1986) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidacies_poll_senate <- senate_elec %>%
  rowwise() %>%
  reframe(import_raw_candidacies_poll_file(type_elec, year, month)) %>%
  select(-any_of("value"))

## ----- use data -----
historical_raw_candidacies_poll <- bind_rows(historical_candidacies_poll_congress,
                                             historical_candidacies_poll_senate)


## ----- write_csv -----
#historical_raw_candidacies_poll %>%
#  split(historical_raw_candidacies_poll$type_elec) %>%
#  map(function(x) { split(x, x$date_elec) }) %>%
#  map(function(y) { 
#    map(y, function(x) {
#      election_info <- type_to_code_election(unique(x$type_elec))
#      date_elec <- unique(x$date_elec)
#      year <- substr(date_elec, 7, 10)  # Extract year (last 4 characters)
#      month <- substr(date_elec, 4, 5)  # Extract month (characters 4 and 5)
#      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
#      
#      # Create directory if it doesn't exist
#      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
#      
#      write_csv(x, file = glue("{output_dir}/raw_candidacies_poll_{unique(x$type_elec)}_{year}_{month}.csv"))
#    })
#  })


library(tidyverse)
library(glue)

# ----- write_rda -----
historical_raw_candidacies_poll %>%
  split(historical_raw_candidacies_poll$type_elec) %>%
  map(function(x) { split(x, x$date_elec) }) %>%
  map(function(y) { 
    map(y, function(x) {
      election_info <- type_to_code_election(unique(x$type_elec))
      date_elec <- unique(x$date_elec)
      year <- substr(as.character(date_elec), 1, 4)  # Extract year (first 4 characters)
      month <- sprintf("%02d", as.numeric(substr(as.character(date_elec), 6, 7)))  # Extract month (characters 6 and 7)
      day <- sprintf("%02d", as.numeric(substr(as.character(date_elec), 9, 10)))  # Extract day (characters 9 and 10)
      
      # Construct the directory path using the correct format
      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
      
      # Create directory if it doesn't exist
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      
      # Define the output file path for the RDA file
      output_file <- glue("{output_dir}/raw_candidacies_poll_{unique(x$type_elec)}_{year}_{month}.rda")
      
      # Save the data frame to an RDA file
      save(x, file = output_file)
    })
  })


# ----- rm -----
rm(list = c("historical_candidacies_poll_congress",
            "historical_candidacies_poll_senate",
            "historical_raw_candidacies_poll"))

rm(senate_elec, congress_elec)
