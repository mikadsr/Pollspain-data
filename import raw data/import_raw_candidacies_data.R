# Function to import raw candidacies files from MIR
import_raw_candidacies_file <- function(type_elec, year, month,
                                        agg_level = if_else(type_to_code_election(type_elec)$cod_elec == "01", "TOTA",
                                                            if_else(type_to_code_election(type_elec)$cod_elec == "04", "MUNI", "MESA")),
                                        base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                        encoding = "Latin1",
                                        starts = c(1, 3, 7, 9, 15, 65, 215, 221, 227),
                                        ends = c(2, 6, 8, 14, 64, 214, 220, 226, 232)) {
  
  # Ensure type_elec is scalar
  type_elec <- as.character(type_elec)
  
  # Code of election
  cod_elec <- type_to_code_election(type_elec)
  
  # Check: if elections required are allowed
  char_month <- str_pad(month, pad = "0", width = 2)
  join_result <- dates_elections_spain |>
    inner_join(tibble(cod_elec = cod_elec$cod_elec, type_elec = type_elec, year = year, month = month),
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
  election_dir <- glue("{base_url}{cod_elec$dir}/{cod_elec$cod_elec}{year}{char_month}/")
  
  # Build the url to the .DAT file
  dat_file_name <- glue("03{cod_elec$cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data
  result <- tryCatch({
    raw_file <- read_lines(url, locale = locale(encoding = encoding))
    raw_file <- tibble(value = raw_file)
    
    # Process variables following the instructions of register
    candidacies <- raw_file |>
      mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
             type_elec = type_elec,
             year = as.numeric(str_sub(value, starts[2], end = ends[2])),
             month = as.numeric(str_sub(value, starts[3], end = ends[3])),
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
    
    # Rename and relocate, ensuring "date_elec" does not already exist
    if ("date_elec" %in% names(candidacies)) {
      candidacies <- candidacies |>
        relocate(date_elec, .after = type_elec)
    } else {
      candidacies <- candidacies |>
        rename(date_elec = date) |>
        relocate(date_elec, .after = type_elec)
    }
    
    return(candidacies)
  }, error = function(e) {
    message(glue("No data found for election {type_elec} in {char_month}-{year}"))
    return(NULL)
  })
  
  return(result)
}

# Function to import all candidacies
import_all_candidacies <- function(elections) {
  elections %>%
    rowwise() %>%
    reframe(import_raw_candidacies_file(type_elec, year, month)) %>%
    filter(!is.null(import_raw_candidacies_file))  # Remove NULL results
}

# ----- Candidacies congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  drop_na(year, month) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  select(type_elec, year, month, cod_elec)

historical_raw_candidacies_congress <- import_all_candidacies(congress_elec)

# ----- Candidacies senate -----
senate_elec <- dates_elections_spain %>%
  filter(cod_elec == "03" & year >= 1986) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_raw_candidacies_senate <- import_all_candidacies(senate_elec)

# ----- use data -----
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
library(tidyverse)
library(glue)

# ----- save_rda -----
historical_raw_candidacies %>%
  split(historical_raw_candidacies$type_elec) %>%
  map(function(x) { split(x, x$date_elec) }) %>%
  map(function(y) { 
    map(y, function(x) {
      election_info <- type_to_code_election(unique(x$type_elec))
      date_elec <- unique(x$date_elec)
      year <- substr(date_elec, 7, 10)  # Extract year (last 4 characters)
      month <- substr(date_elec, 4, 5)  # Extract month (characters 4 and 5)
      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
      
      # Create directory if it doesn't exist
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      
      # Save the data frame as .rda file
      save(x, file = glue("{output_dir}/raw_candidacies_{unique(x$type_elec)}_{year}_{month}.rda"))
    })
  })


# ----- rm -----
rm(senate_elec, congress_elec, historical_raw_candidacies, historical_raw_candidacies_congress, historical_raw_candidacies_senate)
