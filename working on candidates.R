library(readr)
library(dplyr)
library(stringr)

# FOR CANDIDATE FILES FROM 2004 ONWARDS
library(readr)
library(dplyr)

# File URL
file_url <- "https://github.com/mikadsr/Pollspain-data/blob/main/02-congress/02201904/04021904.DAT?raw=true"

# Download the file to a temporary location
temp_file <- tempfile(fileext = ".DAT")
download.file(file_url, temp_file, mode = "wb")

# Define the column start and end positions, excluding columns 15, 16, and 17
starts <- c(1, 3, 7, 9, 10, 12, 13, 16, 22, 25, 26, 51, 76, 101, 110, 120)
ends <- c(2, 6, 8, 9, 11, 12, 15, 21, 24, 25, 50, 75, 100, 109, 119, 120)

# Create the column specifications
col_spec <- fwf_positions(start = starts, end = ends)

# Read the fixed-width format file
data <- read_fwf(temp_file, col_positions = col_spec, col_types = cols(.default = "c"))

# Assign variable names to the columns
col_names <- c("election_type", "year", "month", "round_number", "province_code", "district_code", 
               "municipality_code", "candidacy_code", "order_number", "candidate_type", "candidate_name",
               "candidate_surname1", "candidate_surname2", "candidate_sex", "candidate_id_card", "candidate_elected")

names(data) <- col_names

# Data cleaning steps
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
  ) |> 
  select(-c(candidate_name, candidate_surname1, candidate_surname2))

# Display the first few rows of the cleaned data
head(cleaned_data)


# FOR CANDIDATE FILES BEFORE 2004 ----
library(readr)
library(dplyr)
library(httr)

# Define the column specifications
starts <- c(1, 3, 7, 9, 10, 12, 13, 16, 22, 25, 26)
ends <- c(2, 6, 8, 9, 11, 12, 15, 21, 24, 25, 120)
col_names <- c("election_type", "year", "month", "round_number", "province_code", "district_code", 
               "municipality_code", "candidacy_code", "order_number", "candidate_type", "candidate_full_name")

# Read the fixed-width file
url <- "https://github.com/mikadsr/Pollspain-data/blob/main/03-senate/03200003/04030003.DAT?raw=true"
response <- httr::GET(url)
data <- read_fwf(httr::content(response, "raw"), fwf_positions(starts, ends, col_names))

# Clean the remaining_data column
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

print(cleaned_data)
