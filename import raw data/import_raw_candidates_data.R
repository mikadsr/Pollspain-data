library(readr)
library(dplyr)
library(glue)
library(httr)

# Function to import raw candidates files from MIR
import_raw_candidates_file <- function(type_elec, year, month,
                                       base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                       encoding = "Latin1",
                                       starts_before_2004 = c(1, 3, 7, 9, 10, 12, 13, 16, 22, 25, 26),
                                       ends_before_2004 = c(2, 6, 8, 9, 11, 12, 15, 21, 24, 25, 120),
                                       starts_after_2004 = c(1, 3, 7, 9, 10, 12, 13, 16, 22, 25, 26, 51, 76, 101, 110, 120),
                                       ends_after_2004 = c(2, 6, 8, 9, 11, 12, 15, 21, 24, 25, 50, 75, 100, 109, 119, 120)) {
  
  # Ensure type_elec is scalar
  type_elec <- as.character(type_elec)
  
  # Code of election
  cod_elec_info <- type_to_code_election(type_elec = type_elec)
  cod_elec <- cod_elec_info$cod_elec
  
  # Check: if elections required are allowed
  char_month <- str_pad(month, pad = "0", width = 2)
  join_result <- dates_elections_spain %>%
    inner_join(tibble(cod_elec = cod_elec, type_elec = type_elec, year = year, month = month),
               by = c("cod_elec", "type_elec", "year", "month")) %>%
    nrow()
  if (join_result == 0) {
    stop(glue("No {type_elec} elections are available in {char_month}-{year}"))
  }
  
  # Check: if base_url and encoding are valid
  if (!is.character(base_url) | !is.character(encoding)) {
    stop("Parameters 'base_url' and 'encoding' must be character")
  }
  
  # Build the url to the directory
  election_dir <- glue("{base_url}{cod_elec_info$dir}/{cod_elec_info$cod_elec}{year}{char_month}/")
  
  # Build the url to the .DAT file
  dat_file_name <- glue("04{cod_elec_info$cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data
  raw_file <- tryCatch(
    {
      read_lines(url, locale = locale(encoding = encoding))
    },
    error = function(e) {
      message(glue("No data found for election {type_elec} in {char_month}-{year}"))
      return(NULL)
    }
  )
  
  if (is.null(raw_file)) {
    return(NULL)
  }
  
  raw_file <- tibble(value = raw_file)
  
  # Determine which starts and ends to use
  if (year >= 2004) {
    starts <- starts_after_2004
    ends <- ends_after_2004
    col_names <- c("cod_elec", "year", "month", "round_number", "province_code", "district_code", 
                   "municipality_code", "id_candidacies", "order_number", "candidate_type", "candidate_name",
                   "candidate_surname1", "candidate_surname2", "candidate_sex", "candidate_id_card", "candidate_elected")
  } else {
    starts <- starts_before_2004
    ends <- ends_before_2004
    col_names <- c("cod_elec", "year", "month", "round_number", "province_code", "district_code", 
                   "municipality_code", "id_candidacies", "order_number", "candidate_type", "candidate_full_name")
  }
  
  # Read the fixed-width file
  data <- read_fwf(url, fwf_positions(starts, ends, col_names), col_types = cols(.default = "c"))
  
  # Debug: Check the structure of the data
  print("Structure of data after reading the .DAT file:")
  str(data)
  
  # Data cleaning steps
  if (year >= 2004) {
    cleaned_data <- data %>%
      mutate(
        candidate_sex = case_when(
          str_detect(candidate_sex, "F") ~ "female",
          str_detect(candidate_sex, "M") ~ "male",
          TRUE ~ candidate_sex
        ),
        candidate_elected = case_when(
          str_detect(candidate_elected, "N") ~ FALSE,
          str_detect(candidate_elected, "S") ~ TRUE,
          TRUE ~ NA
        ),
        candidate_full_name = paste(candidate_name, candidate_surname1, candidate_surname2)
      ) %>% 
      select(-c(candidate_name, candidate_surname1, candidate_surname2))
  } else {
    cleaned_data <- data %>%
      # Split remaining_data into two parts: before and after the first 0
      mutate(
        remaining_data_split = str_split_fixed(candidate_full_name, "0", 2),
        candidate_full_name = remaining_data_split[, 1],
        remaining_data_part2 = paste0("0", remaining_data_split[, 2])
      ) %>%
      # Apply case_when logic to the second part
      mutate(
        candidate_elected = case_when(
          str_detect(remaining_data_part2, "N") ~ FALSE,
          str_detect(remaining_data_part2, "S") ~ TRUE,
          TRUE ~ NA
        ),
        candidate_sex = NA_character_,
        candidate_id_card = NA_character_
      ) %>%
      # Remove the temporary columns used for splitting
      select(-remaining_data_split, -remaining_data_part2)
  }
  
  # Debug: Check the structure of the cleaned data
  print("Structure of cleaned data:")
  str(cleaned_data)
  
  # Ensure 'cod_elec' and 'type_elec' columns are present in cleaned_data
  cleaned_data <- cleaned_data %>%
    mutate(cod_elec = cod_elec, type_elec = type_elec, year = as.numeric(year), month = as.numeric(month))
  
  # Join with dates of elections
  candidates <- cleaned_data %>%
    left_join(dates_elections_spain %>% select(-topic),
              by = c("cod_elec", "type_elec", "year", "month")) %>%
    select(-year, -month, -day)
  
  # Remove existing date_elec if present before renaming
  if ("date_elec" %in% names(candidates)) {
    candidates <- candidates 
  }
  
  # relocate
  candidates <- candidates %>%
    relocate(date_elec, .after = type_elec)
  
  # Output
  return(candidates)
}

# Function to import all candidacies
import_all_candidates <- function(elections) {
  elections %>%
    rowwise() %>%
    reframe(import_raw_candidates_file(type_elec, year, month)) %>%
    select(-any_of("value"))
}

# ----- Candidates congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidates_congress <- import_all_candidates(congress_elec)

# ----- Candidates senate -----
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

library(tidyverse)
library(glue)

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
