# Function to import raw candidates files from MIR
import_raw_candidates_file <- function(type_elec, year, month,
                                       base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                       encoding = "Latin1",
                                       starts = c(1, 3, 7, 9, 10, 12, 13, 16, 22, 25, 26,
                                                  51, 76, 101, 102, 104, 106, 110, 120),
                                       ends = c(2, 6, 8, 9, 11, 12, 15, 21, 24, 25, 50,
                                                75, 100, 101, 103, 105, 109, 119, 120)) {
  # Ensure type_elec is scalar
  type_elec <- as.character(type_elec)
  
  # Code of election
  cod_elec_info <- type_to_code_election(type_elec = type_elec)
  cod_elec <- cod_elec_info$cod_elec
  
  # Check: if elections required are allowed
  char_month <- str_pad(month, pad = "0", width = 2)
  join_result <- dates_elections_spain |>
    inner_join(tibble(cod_elec = cod_elec, type_elec = type_elec, year = year, month = month),
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
  
  # Process variables following the instructions of register
  candidates <- raw_file |>
    mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
           type_elec = type_elec,
           year = as.numeric(str_sub(value, starts[2], end = ends[2])),
           month = as.numeric(str_sub(value, start = starts[3], end = ends[3])),
           turn = as.numeric(str_sub(value, start = starts[4], end = ends[4])),
           cod_INE_prov = str_sub(value, start = starts[5], end = ends[5]),
           cod_mun_district = str_sub(value, start = starts[6], end = ends[6]),
           cod_INE_mun = str_sub(value, start = starts[7], end = ends[7]),
           id_candidacies = str_sub(value, start = starts[8], end = ends[8]),
           order = as.numeric(str_sub(value, start = starts[9], end = ends[9])),
           holder = str_sub(value, start = starts[10], end = ends[10]) == "T",
           name = str_trim(str_sub(value, start = starts[11], end = ends[11])),
           surname1 = str_trim(str_sub(value, start = starts[12], end = ends[12])),
           surname2 = str_trim(str_sub(value, start = starts[13], end = ends[13])),
           surname = str_trim(glue("{surname1} {surname2}")),
           sex = str_sub(value, start = starts[14], end = ends[14]),
           birth_day = str_sub(value, start = starts[15], end = ends[15]),
           birth_month = as.numeric(str_sub(value, start = starts[16], end = ends[16])),
           birth_year = as.numeric(str_sub(value, start = starts[17], end = ends[17])),
           birthdate = suppressWarnings(as_date(glue("{birth_year}-{birth_month}-{birth_day}"))),
           id_card = str_sub(value, start = starts[18], end = ends[18]),
           elected = str_sub(value, start = starts[19], end = ends[19]) == "S") |>
    select(-value, -surname1, -surname2, -contains("birth_")) |>
    select(where(~ !all(is.na(.))))
  
  # Join with dates of elections
  candidates <- candidates |>
    left_join(dates_elections_spain |> select(-topic),
              by = c("cod_elec", "type_elec", "year", "month")) |>
    select(-year, -month, -day)
  
  # Remove existing date_elec if present before renaming
  if ("date_elec" %in% names(candidates)) {
    candidates <- candidates |>
      select(-date_elec)
  }
  
  # Rename and relocate
  candidates <- candidates |>
    rename(date_elec = date) |>
    relocate(date_elec, .after = type_elec)
  
  # output
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
