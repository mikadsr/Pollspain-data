#
# cod_elec, type_elec: type of election
# date_elec: date of election
# id_candidacies, abbrev_candidacies, name_candidacies: id,
# abbreviation and name of candidacies
# cod_candidacies_prov, cod_candidacies_ccaa, cod_candidacies_nat

# ----- packages -----

library(tidyverse)
library(glue)
library(lubridate)

# ----- import candidacies files (prefix 03) from MIR -----

# year, month: year and month of election to be unloaded.
# base_url: a common path from which data will be downloaded
# encoding: encoding format to be passed to read_lines() function
# starts,ends: cut-off points for splitting raw .DAT files into a
# dataset with several variables.
# Load required libraries
library(tidyverse)
library(glue)
library(lubridate)

# Ensure the dataset is loaded
data(dates_elections_spain)  # Use this if the dataset is part of a package

# Function to import raw candidacies files from MIR
import_raw_candidacies_file <- function(type_elec, year, month,
                                        agg_level = if_else(cod_elec == "01", "TOTA",
                                                            if_else(cod_elec == "04", "MUNI", "MESA")),
                                        base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                        encoding = "Latin1",
                                        starts = c(1, 3, 7, 9, 15, 65, 215, 221, 227),
                                        ends = c(2, 6, 8, 14, 64, 214, 220, 226, 232)) {
  
  # Code of election
  cod_elec <- type_to_code_election(type_elec = type_elec)
  
  # Check: if elections required are allowed
  char_month <- str_pad(month, pad = "0", width = 2)
  join_result <- dates_elections_spain |>
    inner_join(tibble(cod_elec, type_elec, year, month),
               by = c("cod_elec", "type_elec", "year", "month")) |>
    nrow()
  if (join_result == 0) {
    stop(glue("No {type_elec} elections are available in {char_month}-{year}"))
  }
  
  # Check: if base_url and encoding are valid
  if (!is.character(base_url) | !is.character(encoding)) {
    stop("Parameters 'base_url' and 'encoding' must be character")
  }
  
  # Check: if lengths of starts and ends are equal
  if (length(starts) != length(ends)) {
    stop("Length of vectors 'starts' and 'ends' must be equal")
  }
  
  # Build the url to the directory
  election_dir <- glue("{base_url}{cod_elec}-{type_elec}/{cod_elec}{year}{char_month}/")
  
  # Build the url to the .DAT file
  dat_file_name <- glue("03{cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data
  raw_file <- read_lines(url, locale = locale(encoding = encoding))
  raw_file <- tibble(value = raw_file)
  
  # Process variables following the instructions of register
  candidacies <- raw_file |>
    mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
           type_elec = type_elec,
           year = as.numeric(str_sub(value, starts[2], end = ends[2])),
           month = as.numeric(str_sub(value, start = starts[3], end = ends[3])),
           id_candidacies = str_trim(str_sub(value, start = starts[4], end = ends[4])),
           abbrev_candidacies = str_to_upper(str_trim(str_sub(value, start = starts[5], end = ends[5]))),
           name_candidacies = str_to_upper(str_trim(str_sub(value, start = starts[6], end = ends[6]))),
           cod_candidacies_prov = str_trim(str_sub(value, start = starts[7], end = ends[7])),
           cod_candidacies_ccaa = str_trim(str_sub(value, start = starts[8], end = ends[8])),
           cod_candidacies_nat = str_trim(str_sub(value, start = starts[9], end = ends[9]))) |>
    select(-value) |>
    select(where(~ !all(is.na(.))))
  
  # Join with dates of elections
  candidacies <- candidacies |>
    left_join(dates_elections_spain |> select(-topic),
              by = c("cod_elec", "type_elec", "year", "month")) |>
    select(-year, -month, -day)
  
  # Rename and relocate
  candidacies <- candidacies |>
    rename(date_elec = date) |>
    relocate(date_elec, .after = type_elec)
  
  # output
  return(candidacies)
}

# ----- Candidacies congress -----
congress_elec <- dates_elections_spain |>
  filter(cod_elec == "02") |>
  drop_na(year, month) |>
  select(type_elec, year, month)

historical_raw_candidacies_congress <- congress_elec |>
  rowwise() |>
  reframe(import_raw_candidacies_file(type_elec, year, month))

# ----- Candidacies senate -----
senate_elec <- dates_elections_spain |>
  filter(cod_elec == "03") |>
  drop_na(year, month) |>
  select(type_elec, year, month)

historical_raw_candidacies_senate <- senate_elec |>
  rowwise() |>
  reframe(import_raw_candidacies_file(type_elec, year, month))

# ----- use data -----
historical_raw_candidacies <- bind_rows(historical_raw_candidacies_congress,
                                        historical_raw_candidacies_senate)

usethis::use_data(historical_raw_candidacies, overwrite = TRUE, compress = "xz")

# ----- write_csv -----
historical_raw_candidacies |>
  split(historical_raw_candidacies$type_elec) |>
  map(function(x) { split(x, x$date_elec) }) |>
  map(function(y) { map(y, function(x) {
    write_csv(x, file = glue("./data/csv/candidacies/raw_candidacies_{unique(x$type_elec)}_{year(unique(x$date_elec))}_{month(unique(x$date_elec))}.csv"))
  })})

# ----- delete -----
rm(list = c("historical_raw_candidacies",
            "historical_raw_candidacies_congress",
            "historical_raw_candidacies_senate"))


# Test for candidacies data in congress elections in June 1986
congress_candidacies_test <- import_raw_candidacies_file("congress", 1986, 6)
print(congress_candidacies_test)