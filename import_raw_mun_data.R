# Load required libraries
library(tidyverse)
library(glue)
library(lubridate)

# Ensure the dataset is loaded
data(dates_elections_spain)  # Use this if the dataset is part of a package

# Function to import raw municipal data files from MIR
import_raw_mun_MIR_files <- function(type_elec, year, month,
                                     base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                     encoding = "Latin1",
                                     starts = c(1, 3, 7, 10, 12, 14, 17, 19, 120, 123,
                                                129, 137, 142, 150, 158),
                                     ends = c(2, 6, 8, 11, 13, 16, 18, 118, 122, 125,
                                              136, 141, 149, 157, 165)) {
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
  dat_file_name <- glue("05{cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data
  raw_file <- read_lines(url, locale = locale(encoding = encoding))
  raw_file <- tibble(value = raw_file)
  
  # Process variables following the instructions of register
  mun_file <- raw_file |>
    mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
           type_elec = type_elec,
           year = as.numeric(str_sub(value, starts[2], end = ends[2])),
           month = as.numeric(str_sub(value, starts[3], end = ends[3])),
           cod_MIR_ccaa = str_sub(value, start = starts[4], end = ends[4]),
           cod_INE_prov = str_sub(value, start = starts[5], end = ends[5]),
           cod_INE_mun = str_sub(value, start = starts[6], end = ends[6]),
           cod_mun_district = str_sub(value, start = starts[7], end = ends[7]),
           mun = str_trim(str_sub(value, start = starts[8], end = ends[8])),
           cod_mun_jud_district = str_trim(str_sub(value, start = starts[9], end = ends[9])),
           cod_mun_prov_council = str_sub(value, start = starts[10], end = ends[10]),
           pop_res_mun = as.numeric(str_sub(value, start = starts[11], end = ends[11])),
           n_poll_stations = as.numeric(str_sub(value, start = starts[12], end = ends[12])),
           census_INE_mun = as.numeric(str_sub(value, start = starts[13], end = ends[13])),
           census_counting_mun = as.numeric(str_sub(value, start = starts[14], end = ends[14])),
           census_CERE_mun = as.numeric(str_sub(value, start = starts[15], end = ends[15]))) |>
    select(-value) |>
    select(where(~ !all(is.na(.)))) |>
    mutate(id_MIR_mun = glue("{cod_MIR_ccaa}-{cod_INE_prov}-{cod_INE_mun}")) |>
    relocate(id_MIR_mun, .before = cod_MIR_ccaa) |>
    relocate(n_poll_stations, .before = pop_res_mun)
  
  # Remove municipal (non-electoral) district data
  mun_file <- mun_file |>
    filter(cod_mun_district == "99") |>
    distinct(mun, cod_INE_mun, .keep_all = TRUE) |>
    select(-cod_mun_district)
  
  # Join with dates of elections
  mun_file <- mun_file |>
    left_join(dates_elections_spain |> select(-topic),
              by = c("cod_elec", "type_elec", "year", "month")) |>
    relocate(date, .after = type_elec) |>
    rename(date_elec = date) |>
    select(-year, -month, -day)
  
  # output
  return(mun_file)
}

# ----- mun data congress -----
congress_elec <- dates_elections_spain |>
  filter(cod_elec == "02") |>
  drop_na(year, month) |>
  select(type_elec, year, month)

historical_raw_mun_data_congress <- congress_elec |>
  rowwise() |>
  reframe(import_raw_mun_MIR_files(type_elec, year, month)) |>
  select(-any_of("value"))

# ----- mun data senate -----
senate_elec <- dates_elections_spain |>
  filter(cod_elec == "03") |>
  drop_na(year, month) |>
  select(type_elec, year, month)

historical_raw_mun_data_senate <- senate_elec |>
  rowwise() |>
  reframe(import_raw_mun_MIR_files(type_elec, year, month)) |>
  select(-any_of("value"))

# ----- use data -----
historical_raw_mun_data <- bind_rows(historical_raw_mun_data_congress,
                                     historical_raw_mun_data_senate)

usethis::use_data(historical_raw_mun_data, overwrite = TRUE, compress = "xz")

# ----- write_csv -----
historical_raw_mun_data |>
  split(historical_raw_mun_data$type_elec) |>
  map(function(x) { split(x, x$date_elec) }) |>
  map(function(y) { map(y, function(x) {
    write_csv(x, file = glue("./data/csv/mun_data/raw_mun_data_{unique(x$type_elec)}_{year(unique(x$date_elec))}_{month(unique(x$date_elec))}.csv"))
  })})

# ----- delete -----
rm(list = c("historical_raw_mun_data_congress",
            "historical_raw_mun_data_senate",
            "historical_raw_mun_data"))


# Test for municipal data in congress elections in June 1986
congress_mun_test <- import_raw_mun_MIR_files("congress", 1986, 6)
print(congress_mun_test)