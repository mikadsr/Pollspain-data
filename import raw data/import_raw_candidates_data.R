source("./import raw data/import_raw_data_functions.R")
#check, install if necessary, and load the required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, glue, purrr, stringr)
###########################################################

# IMPORT CANDIDATES ----
# Function to import all candidacies
import_all_candidates <- function(elections) {
  elections %>%
    rowwise() %>%
    reframe(import_raw_candidates_file(type_elec, year, month)) %>%
    select(-any_of("value"))
}

## ----- Candidates congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidates_congress <- import_all_candidates(congress_elec)

## ----- Candidates senate -----
senate_elec <- dates_elections_spain %>%
  filter(cod_elec == "03" & year >= 1986) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidates_senate <- import_all_candidates(senate_elec)

# ----- use data -----
historical_raw_candidates <- bind_rows(historical_candidates_congress,
                                       historical_candidates_senate)


## ----- write_csv -----
#historical_raw_candidates %>%
#  split(historical_raw_candidates$type_elec) %>%
#  map(function(x) { split(x, x$date_elec) }) %>%
#  map(function(y) { 
#    map(y, function(x) {
#      election_info <- type_to_code_election(unique(x$type_elec))
#      date_elec <- unique(x$date_elec)
#      year <- substr(date_elec, 1, 4)  # Extract year (last 4 characters)
#      month <- substr(date_elec, 6, 7)
#      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
#      
#      # Create directory if it doesn't exist
#      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
#      
#      write_csv(x, file = glue("{output_dir}/raw_candidates_{unique(x$type_elec)}_{year}_{month}.csv"))
#    })
#  })


# ----- save_rda -----
historical_raw_candidates %>%
  split(historical_raw_candidates$type_elec) %>%
  map(function(x) { split(x, x$date_elec) }) %>%
  map(function(y) { 
    map(y, function(x) {
      election_info <- type_to_code_election(unique(x$type_elec))
      date_elec <- unique(x$date_elec)
      year <- substr(date_elec, 1, 4)  # Extract year (first 4 characters)
      month <- substr(date_elec, 6, 7) # Extract month (characters 6 and 7)
      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
      
      # Create directory if it doesn't exist
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      
      # Save the data frame as .rda file
      save(x, file = glue("{output_dir}/raw_candidates_{unique(x$type_elec)}_{year}_{month}.rda"))
    })
  })

# ---- rm ----
rm(import_all_candidates, 
   congress_elec,
   senate_elect, 
   historical_raw_candidates, 
   historical_candidates_senate, 
   historical_candidates_congress)
