# Load required libraries
library(tidyverse)
library(glue)
library(lubridate)

# ----- import candidacies poll data (prefix 10) from MIR -----

# Ensure the dataset is loaded
data(dates_elections_spain)  # Use this if the dataset is part of a package

# Function to import raw candidacies poll files from MIR
import_raw_candidacies_poll_file <- function(type_elec, year, month,
                                             base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                             encoding = "Latin1",
                                             starts = c(1, 3, 7, 9, 10, 12, 14, 17, 19, 23, 24, 30),
                                             ends = c(2, 6, 8, 9, 11, 13, 16, 18, 22, 23, 29, 36)) {
  
  # Code of election
  cod_elec <- type_to_code_election(type_elec = type_elec)$cod_elec
  
  # Check: if elections required are allowed
  char_month <- str_pad(month, pad = "0", width = 2)
  join_result <- dates_elections_spain %>%
    mutate(cod_elec = as.character(cod_elec)) %>%
    inner_join(tibble(cod_elec = as.character(cod_elec), type_elec, year, month),
               by = c("cod_elec", "type_elec", "year", "month")) %>%
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
  dat_file_name <- glue("10{cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data
  raw_file <- read_lines(url, locale = locale(encoding = encoding))
  raw_file <- tibble(value = raw_file)
  
  # Check if there exists file
  if (nrow(raw_file) == 0) {
    return(raw_file)
  } else {
    # Process variables following the instructions of register
    poll_stations <- raw_file %>%
      mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
             type_elec = type_elec,
             year = as.numeric(str_sub(value, starts[2], end = ends[2])),
             month = as.numeric(str_sub(value, starts[3], end = ends[3])),
             turn = as.numeric(str_sub(value, start = starts[4], end = ends[4])),
             cod_MIR_ccaa = str_trim(str_sub(value, start = starts[5], end = ends[5])),
             cod_INE_prov = str_trim(str_sub(value, start = starts[6], end = ends[6])),
             cod_INE_mun = str_trim(str_sub(value, start = starts[7], end = ends[7])),
             id_MIR_mun = glue("{cod_MIR_ccaa}-{cod_INE_prov}-{cod_INE_mun}"),
             cod_mun_district = str_trim(str_sub(value, start = starts[8], end = ends[8])),
             cod_sec = str_trim(str_sub(value, start = starts[9], end = ends[9])),
             cod_poll_station = str_trim(str_sub(value, start = starts[10], end = ends[10])),
             id_candidacies = str_trim(str_sub(value, start = starts[11], end = ends[11])),
             ballots = as.numeric(str_sub(value, start = starts[12], end = ends[12]))) %>%
      select(-value) %>%
      select(where(~ !all(is.na(.))))
    
    # Join with dates of elections
    poll_stations <- poll_stations %>%
      left_join(dates_elections_spain %>% select(-topic),
                by = c("cod_elec", "type_elec", "year", "month")) %>%
      select(-year, -month, -day)
    
    # Rename and relocate, ensuring "date_elec" is in yyyy-mm-dd format
    poll_stations <- poll_stations %>%
      mutate(date_elec = as.Date(date_elec)) %>%
      relocate(date_elec, .after = type_elec) %>%
      relocate(id_MIR_mun, .after = date_elec) %>%
      relocate(turn, .after = cod_poll_station)
    
    # Output
    return(poll_stations)
  }
}

# ----- poll data congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidacies_poll_congress <- congress_elec %>%
  rowwise() %>%
  reframe(import_raw_candidacies_poll_file(type_elec, year, month)) %>%
  select(-any_of("value"))

# ----- poll data senate -----
senate_elec <- dates_elections_spain %>%
  filter(cod_elec == "03" & year >= 1986) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_candidacies_poll_senate <- senate_elec %>%
  rowwise() %>%
  reframe(import_raw_candidacies_poll_file(type_elec, year, month)) %>%
  select(-any_of("value"))

# ----- use data -----
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


# ----- delete -----
rm(list = c("historical_candidacies_poll_congress",
            "historical_candidacies_poll_senate",
            "historical_raw_candidacies_poll"))

rm(senate_elec, congress_elec)
