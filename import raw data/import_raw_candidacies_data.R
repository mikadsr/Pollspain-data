source("./import raw data/import_raw_data_functions.R")
#check, install if necessary, and load the required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, purrr, glue, readr, tibble, lubridate)
###########################################################

# IMPORT CANDIDACIES ----

## Function to import all candidacies
import_all_candidacies <- function(elections) {
  elections %>%
    rowwise() %>%
    reframe(import_raw_candidacies_file(type_elec, year, month)) %>%
    filter(!is.null(import_raw_candidacies_file))  # Remove NULL results
}

## ----- Candidacies congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  drop_na(year, month) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  select(type_elec, year, month, cod_elec)

historical_raw_candidacies_congress <- import_all_candidacies(congress_elec)

## ----- Candidacies senate -----
senate_elec <- dates_elections_spain %>%
  filter(cod_elec == "03" & year >= 1986) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_raw_candidacies_senate <- import_all_candidacies(senate_elec)

## ----- use data -----
historical_raw_candidacies <- bind_rows(historical_raw_candidacies_congress,
                                        historical_raw_candidacies_senate)

## ----- write_csv -----
#historical_raw_candidacies %>%
#  split(historical_raw_candidacies$type_elec) %>%
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
#      write_csv(x, file = glue("{output_dir}/raw_candidacies_{unique(x$type_elec)}_{year}_{month}.csv"))
#    })
#  })
#


# ----- save_rda -----
# Save RDA files function
historical_raw_candidacies %>%
  split(.$type_elec) %>%
  map(function(x) {
    split(x, x$date_elec)
  }) %>%
  map(function(y) {
    map(y, function(z) {
      # Debug: print the structure of z
      print("Structure of z:")
      str(z)
      
      election_info <- type_to_code_election(unique(z$type_elec))
      date_elec <- unique(z$date_elec)
      year <- year(date_elec)  # Extract year using lubridate
      month <- month(date_elec)  # Extract month using lubridate
      month <- sprintf("%02d", month)  # Ensure month is two digits
      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
      
      # Create directory if it doesn't exist
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      
      # Save the data frame as .rda file
      save(z, file = glue("{output_dir}/raw_candidacies_{unique(z$type_elec)}_{year}_{month}.rda"))
    })
  })

## ----- rm -----
rm(senate_elec, congress_elec, historical_raw_candidacies, historical_raw_candidacies_congress, historical_raw_candidacies_senate)
