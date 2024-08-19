# Load required libraries
#check, install if necessary, and load the required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, glue, readr, stringr, tibble, tidyr, purr, lubridate)
###########################################################

#' @title Import Raw Candidacies Files from MIR
#'
#' @description This function imports raw candidacies files from the Spanish Ministry of the Interior (MIR) for a specified election type, year, and month. It constructs the appropriate URL, downloads the data, and processes it into a tidy format.
#'
#' @param type_elec Character scalar. The type of election, such as "congress", "senate", etc.
#' @param year Numeric scalar. The year of the election.
#' @param month Numeric scalar. The month of the election.
#' @param agg_level Character scalar. The aggregation level for the data. By default, it is determined based on the election type.
#' @param base_url Character scalar. The base URL to the directory where the data files are located. Defaults to "https://github.com/mikadsr/Pollspain-data/raw/main/".
#' @param encoding Character scalar. The character encoding used in the raw data file. Defaults to "Latin1".
#' @param starts Integer vector. The starting positions for each field in the raw data file. Defaults to positions for a typical MIR file.
#' @param ends Integer vector. The ending positions for each field in the raw data file. Defaults to positions for a typical MIR file.
#'
#' @return A tibble containing the processed candidacies data, including columns for election code, type, year, month, and identifiers for candidacies at various levels (province, CCAA, national). The data is also joined with additional election date information from `dates_elections_spain`.
#'
#' @details This function first verifies that the requested election data exists and is available for the specified year and month. It then constructs the URL to the data file, downloads the file, and processes it by extracting relevant fields. The function handles any potential errors, such as missing data, and returns a tidy tibble with the processed candidacies data.
#'
#' @note The function checks for valid base URL and encoding parameters and ensures that the lengths of the `starts` and `ends` vectors match. It also supports handling different aggregation levels depending on the type of election.
#'
#' @examples
#' \dontrun{
#' import_raw_candidacies_file("congress", 2023, 7)
#' }
#'
#' @author Mikaela DeSmedt
#' @keywords utils
#' @name import_raw_candidacies_file
#' @export

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
        mutate(date_elec = as.Date(date_elec)) |>
        relocate(date_elec, .after = type_elec)
    } else {
      candidacies <- candidacies |>
        rename(date_elec = date) |>
        mutate(date_elec = as.Date(date_elec)) |>
        relocate(date_elec, .after = type_elec)
    }
    
    return(candidacies)
  }, error = function(e) {
    message(glue("No data found for election {type_elec} in {char_month}-{year}"))
    return(NULL)
  })
  
  return(result)
}

#' @title Import Raw Candidacies Poll Files from MIR
#'
#' @description This function imports raw candidacies poll files from the MIR (Ministerio del Interior de España) for a specified election type, year, and month. The function constructs the URL to the relevant file, reads the file, and processes it to extract relevant variables. It also joins the data with election dates from a predefined dataset.
#'
#' @param type_elec A character string specifying the type of election (e.g., "congress", "senate").
#' @param year An integer specifying the year of the election.
#' @param month An integer specifying the month of the election.
#' @param base_url A character string specifying the base URL where the data files are stored. Defaults to "https://github.com/mikadsr/Pollspain-data/raw/main/".
#' @param encoding A character string specifying the encoding of the data files. Defaults to "Latin1".
#' @param starts A numeric vector specifying the start positions of the variables in the raw data file. Defaults to c(1, 3, 7, 9, 10, 12, 14, 17, 19, 23, 24, 30).
#' @param ends A numeric vector specifying the end positions of the variables in the raw data file. Defaults to c(2, 6, 8, 9, 11, 13, 16, 18, 22, 23, 29, 36).
#'
#' @return A tibble containing the processed candidacies data, including election codes, candidacy codes, and other relevant details. If the file is not found, the function returns NULL.
#'
#' @details The function constructs the URL for the relevant data file using the specified parameters and attempts to read the file. It processes the file to extract relevant variables, including election codes, candidacy codes, and other identifying information. The data is then joined with a dataset of election dates to add date information. The resulting tibble contains the processed data.
#'
#' @examples
#' \dontrun{
#' # Example usage
#' data <- import_raw_candidacies_poll_file(type_elec = "congress", year = 2023, month = 7)
#' }
#'
#' @export
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


#' Import Raw Candidate Files from MIR
#'
#' This function imports raw candidate files from the Ministry of Interior of Spain (MIR) for a given election type, year, and month. The data is read from a .DAT file hosted at a specified URL and processed into a clean format.
#'
#' @param type_elec A character string specifying the type of election (e.g., "congress", "senate"). This parameter is required.
#' @param year An integer specifying the year of the election. This parameter is required.
#' @param month An integer specifying the month of the election. This parameter is required.
#' @param base_url A character string specifying the base URL where the data files are stored. Defaults to "https://github.com/mikadsr/Pollspain-data/raw/main/".
#' @param encoding A character string specifying the file encoding. Defaults to "Latin1".
#' @param starts_before_2004 A numeric vector specifying the start positions of the fields in the .DAT file for elections before 2004. Defaults are provided.
#' @param ends_before_2004 A numeric vector specifying the end positions of the fields in the .DAT file for elections before 2004. Defaults are provided.
#' @param starts_after_2004 A numeric vector specifying the start positions of the fields in the .DAT file for elections in 2004 or later. Defaults are provided.
#' @param ends_after_2004 A numeric vector specifying the end positions of the fields in the .DAT file for elections in 2004 or later. Defaults are provided.
#' 
#' @return A tibble containing the processed candidate data, or NULL if the data could not be found or loaded.
#' 
#' @details This function reads raw candidate data from a .DAT file hosted at the given URL. It processes the data according to the election year, as the format of the .DAT files differs before and after 2004. The function handles both the downloading of the data and the necessary data cleaning steps, including combining candidate names and handling different file formats.
#' 
#' @examples
#' candidates_2008 <- import_raw_candidates_file(type_elec = "congress", year = 2008, month = 3)
#'
#' @export
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
  
  # Build the URL to the directory
  election_dir <- glue("{base_url}{cod_elec_info$dir}/{cod_elec_info$cod_elec}{year}{char_month}/")
  
  # Build the URL to the .DAT file
  dat_file_name <- glue("04{cod_elec_info$cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
  url <- glue("{election_dir}{dat_file_name}")
  
  # Print the URL for debugging purposes
  print(glue("Constructed URL: {url}"))
  
  # Download and read raw data as a single string
  raw_data <- tryCatch(
    {
      read_lines(url, locale = locale(encoding = encoding))
    },
    error = function(e) {
      message(glue("No data found for election {type_elec} in {char_month}-{year}"))
      return(NULL)
    }
  )
  
  if (is.null(raw_data)) {
    return(NULL)
  }
  
  # Collapse the data into a single string for `read_fwf` processing
  raw_data <- paste(raw_data, collapse = "\n")
  
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
  
  # Read the fixed-width file using `read_fwf`
  data <- read_fwf(
    I(raw_data),  # Use the raw data string as input
    fwf_positions(starts, ends, col_names),
    col_types = cols(.default = "c")
  )
  
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
        candidate_full_name = paste(candidate_name, candidate_surname1, candidate_surname2)
      ) %>% 
      select(-c(candidate_name, candidate_surname1, candidate_surname2, candidate_elected, candidate_id_card))
  } else if (year == 2000) {
    cleaned_data <- data %>%
      mutate(
        candidate_sex = case_when(
          str_detect(str_trim(candidate_full_name), " M000000000000000000") ~ "male",
          str_detect(str_trim(candidate_full_name), " F000000000000000000") ~ "female",
          TRUE ~ NA_character_
        ),
        candidate_full_name = str_remove(str_trim(candidate_full_name), "    .*")
      )
  } else {
    cleaned_data <- data %>%
      mutate(
        remaining_data_split = str_split_fixed(candidate_full_name, "0", 2),
        candidate_full_name = remaining_data_split[, 1],
        candidate_sex = NA_character_
      ) %>%
      select(-remaining_data_split)
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
  
  # Relocate date_elec
  candidates <- candidates %>%
    relocate(date_elec, .after = type_elec)
  
  # Output
  return(candidates)
}


#' @title Import Raw Municipal Data Files from MIR
#' 
#' @description
#' This function imports raw data files for municipal election data from the Ministry of Interior (MIR) for a specified election type, year, and month. 
#' It processes the raw .DAT files and extracts relevant municipal-level information, including the number of polling stations, population, and various census counts.
#' 
#' @param type_elec A character string representing the type of election (e.g., "congress", "senate").
#' @param year An integer representing the year of the election.
#' @param month An integer representing the month of the election.
#' @param base_url A character string representing the base URL where the raw data files are located. Defaults to "https://github.com/mikadsr/Pollspain-data/raw/main/".
#' @param encoding A character string representing the encoding format for reading the raw data files. Defaults to "Latin1".
#' @param starts A numeric vector representing the starting positions of the data fields in the raw .DAT file.
#' @param ends A numeric vector representing the ending positions of the data fields in the raw .DAT file.
#' 
#' @return A tibble containing processed municipal-level election data, including columns for electoral codes, population, number of polling stations, and various census counts.
#' 
#' @examples
#' # Import municipal data for a congress election in March 2023
#' mun_data <- import_raw_mun_MIR_files(type_elec = "congress", year = 2023, month = 3)
#' 
#' @export
import_raw_mun_MIR_files <- function(type_elec, year, month,
                                     base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                     encoding = "Latin1",
                                     starts = c(1, 3, 7, 10, 12, 14, 17, 19, 120, 123,
                                                129, 137, 142, 150, 158),
                                     ends = c(2, 6, 8, 11, 13, 16, 18, 118, 122, 125,
                                              136, 141, 149, 157, 165)) {
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
  dat_file_name <- glue("05{cod_elec_info$cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
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
  mun_file <- raw_file |>
    mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
           type_elec = type_elec,
           year = as.numeric(str_sub(value, starts[2], end = ends[2])),
           month = as.numeric(str_sub(value, start = starts[3], end = ends[3])),
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
              by = c("cod_elec", "type_elec", "year", "month")) 
  
  # Remove existing date_elec if present before renaming
  if ("date_elec" %in% names(mun_file)) {
    mun_file <- mun_file |>
      select(-date_elec)
  }
  
  # Rename and relocate
  mun_file <- mun_file |>
    rename(date_elec = date) |>
    relocate(date_elec, .after = type_elec)
  
  # output
  return(mun_file)
}

#' Import Raw Poll Station Data Files from MIR
#'
#' @description
#' This function imports raw poll station data files from the Spanish Ministry of the Interior (MIR) for a specific election. 
#' It processes the data based on the structure and format provided by the MIR, converting it into a structured `tibble` format.
#'
#' @param type_elec A character string representing the type of election (e.g., "congress", "senate").
#' @param year An integer representing the year of the election.
#' @param month An integer representing the month of the election.
#' @param base_url A character string representing the base URL where the raw data files are located. Default is a GitHub URL.
#' @param encoding A character string representing the file encoding. Default is "Latin1".
#' @param starts A vector of integers representing the start positions of each variable in the fixed-width file.
#' @param ends A vector of integers representing the end positions of each variable in the fixed-width file.
#'
#' @return A `tibble` containing the structured poll station data.
#'
#' @details
#' The function constructs the URL for the raw data file based on the provided parameters, downloads the file, and processes it.
#' It handles special cases such as C.E.R.A (census of Spanish citizens absent from Spain) and removes these entries if necessary.
#'
#' @examples
#' \dontrun{
#' poll_stations <- import_poll_stations_MIR_files(type_elec = "congress", year = 2023, month = 7)
#' }
#'
#' @export
import_poll_stations_MIR_files <- function(type_elec, year, month,
                                           base_url = "https://github.com/mikadsr/Pollspain-data/raw/main/",
                                           encoding = "Latin1",
                                           starts = c(1, 3, 7, 9, 10, 12, 14, 17, 19, 23, 24, 31, 38, 45, 52, 59, 66, 73, 80),
                                           ends = c(2, 6, 8, 9, 11, 13, 16, 18, 22, 23, 30, 37, 44, 51, 58, 65, 72, 79, 86)) {
  
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
  
  # Build the URL to the directory
  election_dir <- glue("{base_url}{cod_elec_info$dir}/{cod_elec_info$cod_elec}{year}{char_month}/")
  
  # Build the URL to the .DAT file
  dat_file_name <- glue("09{cod_elec_info$cod_elec}{str_sub(year, start = 3, end = 4)}{char_month}.DAT")
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
  poll_stations <- raw_file |>
    mutate(cod_elec = str_sub(value, start = starts[1], end = ends[1]),
           type_elec = type_elec,
           year = as.numeric(str_sub(value, starts[2], end = ends[2])),
           month = as.numeric(str_sub(value, start = starts[3], end = ends[3])),
           turn = as.numeric(str_sub(value, start = starts[4], end = ends[4])),
           cod_MIR_ccaa = str_trim(str_sub(value, start = starts[5], end = ends[5])),
           cod_INE_prov = str_trim(str_sub(value, start = starts[6], end = ends[6])),
           cod_INE_mun = str_trim(str_sub(value, start = starts[7], end = ends[7])),
           id_MIR_mun = glue("{cod_MIR_ccaa}-{cod_INE_prov}-{cod_INE_mun}"),
           cod_mun_district = str_trim(str_sub(value, start = starts[8], end = ends[8])),
           cod_sec = str_trim(str_sub(value, start = starts[9], end = ends[9])),
           cod_poll_station = str_trim(str_sub(value, start = starts[10], end = ends[10])),
           census_INE = as.numeric(str_sub(value, start = starts[11], end = ends[11])),
           census_counting = as.numeric(str_sub(value, start = starts[12], end = ends[12])),
           census_cere = as.numeric(str_sub(value, start = starts[13], end = ends[13])),
           voters_cere = as.numeric(str_sub(value, start = starts[14], end = ends[14])),
           ballots_1 = as.numeric(str_sub(value, start = starts[15], end = ends[15])),
           ballots_2 = as.numeric(str_sub(value, start = starts[16], end = ends[16])),
           blank_ballots = as.numeric(str_sub(value, start = starts[17], end = ends[17])),
           invalid_ballots = as.numeric(str_sub(value, start = starts[18], end = ends[18])),
           party_ballots = as.numeric(str_sub(value, start = starts[19], end = ends[19]))) |>
    select(-value) |>
    select(where(~ !all(is.na(.))))
  
  # Join with dates of elections
  poll_stations <- poll_stations |>
    left_join(dates_elections_spain |> select(-topic),
              by = c("cod_elec", "type_elec", "year", "month")) |>
    select(-year, -month, -day)
  
  # Remove existing date_elec if present before renaming
  if ("date_elec" %in% names(poll_stations)) {
    poll_stations <- poll_stations |>
      select(-date_elec)
  }
  
  # Rename and relocate
  poll_stations <- poll_stations |>
    rename(date_elec = date) |>
    relocate(date_elec, .after = type_elec) |>
    relocate(id_MIR_mun, .after = date_elec) |>
    relocate(turn, .after = cod_poll_station)
  
  # C.E.R.A (census of Spanish citizens absent from Spain)
  # are assigned cod_INE_mun = "999", cod_mun_district = "09"
  # cod_sec = "0000", cod_poll_station = "U"
  # For each ccaa is summarized with cod_INE_prov = "99"
  # and they should be removed
  poll_stations <- poll_stations |>
    filter(!(cod_INE_prov == "99" & cod_INE_mun == "999"))
  
  # Output
  return(poll_stations)
}
