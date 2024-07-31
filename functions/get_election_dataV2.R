#' @title Get municipal census data
#'
#' @description Get municipal census data for one or more elections at the municipal
#' level. This function is a wrapper of \code{import_raw_mun_MIR_files()}
#' function for a set of elections.
#'
#' @inheritParams type_to_code_election
#' @param type_elec Vector or single value of election types.
#' @param year Vector or single value of years of elections to be considered.
#' @param month Vector or single value of months of elections to be considered.
#' Please ensure (see \code{dates_elections_spain}) that elections of the
#' provided type are available for the given year and month.
#'
#' @return A tibble (as many rows as municipalities for each election) with the
#' following elements:
#' \item{cod_elec}{code of type of elections: \code{"01"} (referendum),
#' \code{"02"} (congress), \code{"03"} (senate), \code{"04"} (local elections),
#' \code{"06"} (cabildo - Canarian council - elections), \code{"07"}
#' (European Parliament elections)}
#' \item{type_elec}{type of election.}
#' \item{date_elec}{date of election.}
#' \item{id_INE_mun}{municipality's id built from ccaa-prov-mun codes provided
#' by INE.}
#' \item{id_MIR_mun}{municipality's id built from ccaa-prov-mun codes provided
#' by Spanish Ministry of Interior (MIR).}
#' \item{cod_INE_ccaa, cod_MIR_ccaa, ccaa}{codes and name for regions (ccaa)
#' to which municipalities belong.}
#' \item{cod_INE_prov, prov}{codes and name for provinces to which municipalities belong.}
#' \item{cod_INE_mun, cd_INE_mun, mun}{code, digit control, and name for
#' municipalities.}
#' \item{cod_mun_jud_district, cod_mun_prov_council}{codes of judicial
#' district and province council.}
#' \item{n_poll_stations}{number of poll stations at each municipality.}
#' \item{pop_res_mun}{census of people who are living (CER + CERA).}
#' \item{census_INE_mun}{people from \code{pop_res_mun} who are
#' allowed to vote.}
#' \item{census_counting_mun}{people from \code{census_INE_mun} after claims.}
#' \item{census_CERE_mun}{census of foreigners, just for EU elections.}
#'
#' @authors
#' Mikaela DeSmedt, Javier Álvarez-Liébana.
#' @source Some definitions of variables were extracted from
#' \url{https://www.ige.gal}
#' @keywords get_elections_data
#' @name get_mun_census_data
#' @import crayon
#' @examples
#'
#' ## Get municipal census data for multiple elections
#' data("dates_elections_spain")
#' \dontrun{
#' # Example usage to combine data from different elections into one table
#' type_elec_vec <- c("congress", "congress", "congress")
#' year_vec <- c(2019, 2019, 2016)
#' month_vec <- c(11, 4, 6)
#'
#' combined_mun_census_data <- get_mun_census_data(type_elec_vec, year_vec, month_vec)
#'
#' # Example for a single election
#' mun_census_data <- get_mun_census_data("congress", 2019, 4)
#' }
#'
#' @export
#' 
get_mun_census_data <- function(type_elec, year, month) {
  # Ensure input parameters are vectors
  type_elec_vec <- as.vector(type_elec)
  year_vec <- as.vector(year)
  month_vec <- as.vector(month)
  
  # Initialize an empty list to store the data
  combined_data_list <- list()
  
  # Loop through the vectors and fetch the data
  for (i in seq_along(type_elec_vec)) {
    type_elec <- type_elec_vec[i]
    year <- year_vec[i]
    month <- month_vec[i]
    
    # Ensure the month is two digits
    char_month <- sprintf("%02d", month)
    
    # Construct the URL for the specific election directory
    base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
    election_info <- type_to_code_election(type_elec)
    election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year}{char_month}")
    
    # Construct the URL for the raw data file
    file_url <- glue("{election_dir}/raw_mun_data_{type_elec}_{year}_{char_month}.csv")
    
    # Print the file URL for debugging purposes
    message("Fetching data from ","raw_mun_data_",(type_elec),"_",(year),"_",(char_month),".csv")
    
    # Fetch the file
    response <- GET(file_url)
    if (status_code(response) != 200) {
      message("Failed to fetch the data file for ", type_elec, " in ", year, "-", month)
      next
    }
    
    # Read the data
    mun_data <- read_csv(content(response, "text"), show_col_types = FALSE)
    
    # Join MIR and INE information
    mun_data <- mun_data %>%
      left_join(cod_INE_mun, by = c("id_MIR_mun", "cod_MIR_ccaa", "cod_INE_prov", "cod_INE_mun"), suffix = c(".x", "")) %>%
      select(-mun.x) %>%
      relocate(cod_INE_ccaa, .before = cod_MIR_ccaa) %>%
      relocate(id_INE_mun, .before = id_MIR_mun) %>%
      relocate(cd_INE_mun, mun, .after = cod_INE_mun) %>%
      relocate(ccaa, .after = cod_MIR_ccaa) %>%
      relocate(prov, .after = cod_INE_prov)
    
    # Add the data to the list
    combined_data_list[[i]] <- mun_data
  }
  
  # Combine all data frames into one
  combined_mun_census_data <- bind_rows(combined_data_list)
  
  return(combined_mun_census_data)
}

#' @title Get Poll Station Data
#'
#' @description Fetch and process poll station data for given election types, years, and months.
#' This function supports both single values and vector inputs for fetching and combining data for multiple elections at once.
#'
#' @inheritParams type_to_code_election
#' @param year,month Vectors of years and months of elections to be considered.
#' Ensure (see \code{dates_elections_spain}) that elections of the provided type are available for the given year and month.
#' @param prec_round Number of decimal places to round percentage values to (default is 3).
#'
#' @return A tibble with poll station data including the following elements:
#' \item{id_elec}{ID of the election, constructed from the election code and date.}
#' \item{type_elec}{Type of election.}
#' \item{date_elec}{Date of election.}
#' \item{id_INE_poll_station}{ID of the poll station, constructed from the municipality and poll station codes.}
#' \item{ccaa,prov,mun}{Names and codes for regions, provinces, and municipalities.}
#' \item{census_counting}{Census count.}
#' \item{ballots_1,turnout_1}{Number of ballots and turnout percentage in the first round.}
#' \item{ballots_2,turnout_2}{Number of ballots and turnout percentage in the second round (if applicable).}
#' \item{blank_ballots,invalid_ballots,party_ballots,valid_ballots,total_ballots}{Various ballot counts.}
#' \item{turnout,porc_valid,porc_invalid,porc_parties,porc_blank}{Various turnout and percentage metrics.}
#' \item{pop_res_mun}{Population residing in the municipality.}
#'
#' @examples
#'
#' ## Correct examples
#' # Fetching data for single elections
#' poll_station_data1 <- get_poll_station_data("congress", 2019, 4)
#' poll_station_data2 <- get_poll_station_data("senate", 2019, 4)
#'
#' # Fetching data for multiple elections at once
#' combined_poll_station_data <- get_poll_station_data(
#'   c("congress", "congress", "congress"), 
#'   c(2019, 2019, 2016), 
#'   c(11, 4, 6)
#' )
#'
#' ## Incorrect examples
#' # Invalid election type
#' poll_station_data5 <- get_poll_station_data("national", 2019, 4)
#'
#' # Length mismatch between year and month vectors
#' poll_station_data6 <- get_poll_station_data("congress", 2016, c(4, 11))
#'
#' # Invalid date format
#' poll_station_data7 <- get_poll_station_data("congress", "2016-06-26")
#'
#' # Non-existent election data
#' poll_station_data8 <- get_poll_station_data("congress", 1990, 4)
#'
#' @authors
#' Mikaela DeSmedt, Javier Álvarez-Liébana
#'
#' @export
#'

get_poll_station_data <- function(type_elec, year, month, prec_round = 3) {
  # Ensure the inputs are vectors of the same length
  if (!is.vector(type_elec) | !is.vector(year) | !is.vector(month)) {
    stop("All inputs must be vectors.")
  }
  if (length(type_elec) != length(year) | length(year) != length(month)) {
    stop("All input vectors must be of the same length.")
  }
  
  # Initialize an empty list to store the data
  combined_data_list <- list()
  
  # Loop through the vectors and fetch the data
  for (i in seq_along(type_elec)) {
    # Ensure the month is two digits
    char_month <- sprintf("%02d", month[i])
    
    # Check if the provided election type is valid
    election_info <- type_to_code_election(type_elec[i])
    
    # Base URL for data
    base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
    
    # Construct the URL for the specific election directory
    election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year[i]}{char_month}")
    message("Fetching from URL: ", election_dir)
    
    # Construct the URL for the specific file
    data_url <- glue("{election_dir}/raw_poll_stations_{type_elec[i]}_{year[i]}_{char_month}.csv")
    message("Fetching data from ", data_url)
    
    # Fetch the data
    response <- GET(data_url)
    if (status_code(response) != 200) {
      message(glue("No data found for election {type_elec[i]} in {char_month}-{year[i]}"))
      next
    }
    
    # Read the data
    poll_station_data <- read_csv(content(response, "text"), show_col_types = FALSE)
    
    # Process the data
    poll_station_data <- poll_station_data %>%
      mutate(valid_ballots = blank_ballots + party_ballots,
             total_ballots = valid_ballots + invalid_ballots) %>%
      filter(cod_INE_mun != "999") %>%
      left_join(get_mun_census_data(type_elec[i], year[i], month[i]),
                by = c("cod_elec", "type_elec", "date_elec", "id_MIR_mun"),
                suffix = c("", ".y")) %>%
      select(-contains(".y")) %>%
      relocate(id_INE_mun, .before = id_MIR_mun) %>%
      relocate(cod_INE_ccaa, .before = cod_MIR_ccaa) %>%
      relocate(ccaa, .after = cod_MIR_ccaa) %>%
      relocate(prov, .after = cod_INE_prov) %>%
      relocate(cd_INE_mun, mun, .after = cod_INE_mun) %>%
      select(-c(census_counting_mun, census_CERE_mun, census_INE_mun)) %>%
      bind_rows(poll_station_data %>% filter(cod_INE_mun == "999")) %>%
      left_join(cod_INE_mun %>%
                  distinct(cod_MIR_ccaa, cod_INE_prov, .keep_all = TRUE) %>%
                  select(contains("ccaa") | contains("prov")),
                by = c("cod_MIR_ccaa", "cod_INE_prov"),
                suffix = c("", ".y")) %>%
      mutate(cod_INE_ccaa = ifelse(is.na(cod_INE_ccaa), cod_INE_ccaa.y, cod_INE_ccaa),
             ccaa = ifelse(is.na(ccaa), ccaa.y, ccaa),
             prov = ifelse(is.na(prov), prov.y, prov),
             mun = ifelse(cod_INE_mun == "999", "CERA", mun),
             id_INE_mun = glue("{cod_INE_ccaa}-{cod_INE_prov}-{cod_INE_mun}"),
             pop_res_mun = ifelse(cod_INE_mun == "999", census_INE, pop_res_mun)) %>%
      select(-contains(".y"), -cod_MIR_ccaa) %>%
      drop_na(id_INE_mun) %>%
      mutate(id_INE_poll_station = glue("{id_INE_mun}-{cod_mun_district}-{cod_sec}-{cod_poll_station}"),
             turnout_1 = round(100 * ballots_1 / census_counting, prec_round),
             turnout_2 = round(100 * ballots_2 / census_counting, prec_round),
             turnout = round(100 * total_ballots / census_counting, prec_round),
             turnout_abs = 100 - turnout,
             porc_valid = round(100 * valid_ballots / total_ballots, prec_round),
             porc_invalid = round(100 * invalid_ballots / total_ballots, prec_round),
             porc_parties = round(100 * party_ballots / valid_ballots, prec_round),
             porc_blank = round(100 * blank_ballots / valid_ballots, prec_round)) %>%
      relocate(turnout:porc_blank, .after = total_ballots) %>%
      relocate(id_INE_poll_station, .after = date_elec) %>%
      relocate(turnout_1, .after = ballots_1) %>%
      relocate(turnout_2, .after = ballots_2) %>%
      mutate(id_elec = glue("{election_info$cod_elec}-{date_elec}")) %>%
      select(id_elec, type_elec, date_elec, id_INE_poll_station, ccaa, prov, mun,
             census_counting, ballots_1, turnout_1, ballots_2, turnout_2,
             blank_ballots, invalid_ballots, party_ballots, valid_ballots,
             total_ballots, turnout, porc_valid, porc_invalid,
             porc_parties, porc_blank, pop_res_mun)
    
    # Add the processed data to the list
    combined_data_list[[i]] <- poll_station_data
  }
  
  # Combine all data frames into one
  combined_poll_station_data <- bind_rows(combined_data_list)
  
  return(combined_poll_station_data)
}

#' @title Get candidates data
#'
#' @description Fetch and process candidates data for Spanish elections.
#'
#' @inheritParams get_mun_census_data
#'
#' @return A data frame containing the candidates data.
#'
#' @author ...
#' @source Data extracted from \href{https://github.com/mikadsr/Pollspain-data}{Pollspain Data Repository}
#' @keywords get_elections_data
#' @name get_candidates_data
#'
#' @examples
#'
#' ## Get candidates data
#'
#' # Right examples
#' \dontrun{
#' # Get candidates data for congress elections in March 1996
#' candidates_data <- get_candidates_data("congress", 1996, 3)
#' }
#'
#' # Wrong examples
#' \dontrun{
#' # Trying to get candidates data for non-congress elections
#' candidates_data <- get_candidates_data("senate", 1996, 3)
#' }
#'
#' @export
get_candidates_data <- function(type_elec, year, month) {
  # At this time, just congress election
  if (type_elec != "congress") {
    stop("Development in process: at this time, just congress elections are allowed")
  }
  
  # Check: if elections required are allowed
  elections_allowed <- dates_elections_spain |>
    filter(year >= 1986) |>
    inner_join(tibble(cod_elec = type_to_code_election(type_elec)$cod_elec,
                      type_elec, year, month),
               by = c("cod_elec", "type_elec", "year", "month"))
  join_result <- elections_allowed |> nrow()
  if (join_result == 0) {
    stop("No elections on provided dates are available")
  }
  
  # Construct the URL for the data file
  election_info <- type_to_code_election(type_elec)
  base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
  file_url <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year}{sprintf('%02d', month)}/raw_candidates_{type_elec}_{year}_{sprintf('%02d', month)}.csv")
  
  # Print the URL for debugging purposes
  message("Fetching data from URL: ", file_url)
  
  # Fetch the data
  candidates_data <- tryCatch({
    read_csv(file_url, show_col_types = FALSE)
  }, error = function(e) {
    stop("Failed to fetch or read candidates data from the specified URL.")
  })
  
  # Filter and process candidates data
  candidates_data <- candidates_data |>
    filter(type_elec %in% type_elec & 
             year(date_elec) %in% year & 
             month(date_elec) %in% month) |>
    mutate(cod_mun_district = ifelse(cod_mun_district == "9", NA, cod_mun_district),
           cod_INE_mun = ifelse(cod_INE_mun == "999", NA, cod_INE_mun))
  
  # Output
  return(candidates_data)
}



##' @title Get candidacies data
##'
##' @description Fetch and process candidacies data for Spanish elections. This function supports fetching data for multiple elections simultaneously by accepting vector inputs for the election parameters.
##'
##' @param type_elec Character vector of election types. Valid types are: 'referendum', 'congress', 'senate', 'local', 'regional', 'cabildo', 'EU'.
##' @param year Integer vector of election years.
##' @param month Integer vector of election months.
##' @param include_candidates Logical flag indicating whether to include detailed candidates data. Default is FALSE.
##'
##' @return A tibble containing the candidacies data.
##'
##' @examples
##'
##' ## Get candidacies data
##'
##' # Correct examples
##' # Single election
##' candidates_data <- get_candidacies_data("congress", 1986, 6)
##'
##' # Multiple elections
##' combined_candidates_data <- get_candidacies_data(c("congress", "congress", "senate"),
##'                                                  c(2019, 2016, 2016),
##'                                                  c(11, 6, 6))
##'
##' # With candidates details
##' candidates_data_with_details <- get_candidacies_data("congress", 2019, 4, include_candidates = TRUE)
##'
##' # Incorrect examples
##' candidates_data_invalid <- get_candidacies_data("national", 2019, 4) # Invalid election type
##' candidates_data_mismatch <- get_candidacies_data("congress", 2016, c(4, 11)) # Length mismatch between year and month
##' candidates_data_invalid_date <- get_candidacies_data("congress", "2016-06-26") # Invalid date format
##' candidates_data_non_existent <- get_candidacies_data("congress", 1990, 4) # Non-existent election data
##'
##' @authors 
##' Mikaela DeSmedt, Javier Álvarez-Liébana. 
##' @source Data extracted from \href{https://github.com/mikadsr/Pollspain-data}{Pollspain Data Repository}
##' @keywords get_elections_data
##' @export
get_candidacies_data <- function(type_elec, year, month, include_candidates = FALSE) {
  
  # Ensure type_elec is a character vector
  type_elec <- as.character(type_elec)
  
  # Initialize an empty list to store the data
  combined_data_list <- list()
  
  # Loop through each set of parameters
  for (i in seq_along(type_elec)) {
    # Validate election type
    if (!type_elec[i] %in% c("referendum", "congress", "senate", "local", "regional", "cabildo", "EU")) {
      stop("Invalid election type. Allowed types are: 'referendum', 'congress', 'senate', 'local', 'regional', 'cabildo', 'EU'")
    }
    
    # Get election info from the utils.R script
    election_info <- type_to_code_election(type_elec[i])
    
    # Construct the base URL and directory URL
    base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
    election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year[i]}{sprintf('%02d', month[i])}")
    
    # Print the final URL for debugging
    message("Fetching from URL: ", election_dir)
    
    # Fetch the candidacies data
    data_url <- glue("{election_dir}/raw_candidacies_{type_elec[i]}_{year[i]}_{sprintf('%02d', month[i])}.csv")
    message("Fetching data from ", basename(data_url))
    
    # Collect raw data
    candidacies_files <- tryCatch({
      read_csv(data_url, show_col_types = FALSE)
    }, error = function(e) {
      message(glue("No data found for election {type_elec[i]} in {sprintf('%02d', month[i])}-{year[i]}"))
      return(NULL)
    })
    
    if (is.null(candidacies_files)) next
    
    # Include candidates if requested
    if (include_candidates) {
      candidates_files <- get_candidates_data(type_elec[i], year[i], month[i])
      
      # Join candidacies and candidates data, keeping only the x variables in case of duplication
      candidacies_data <- candidacies_files %>%
        left_join(candidates_files, by = "id_candidacies", suffix = c("", ".y")) %>%
        select(-ends_with(".y")) %>%
        rename(candidate_name = name, candidate_surname = surname,
               candidate_order = order, candidate_holder = holder,
               candidate_sex = sex, candidate_elected = elected)
      
    } else {
      candidacies_data <- candidacies_files
    }
    
    # Add the processed data to the list
    combined_data_list[[i]] <- candidacies_data
  }
  
  # Combine all data frames into one
  combined_candidacies_data <- bind_rows(combined_data_list)
  
  return(combined_candidacies_data)
}



#' @title Get CERA data (at poll station level)
#'
#' @description This function aggregates CERA (Census of Absent Residents) data from election data, based on a specified hierarchical level.
#'
#' @inheritParams get_mun_census_data
#' @param election_data A data frame containing the election data to be processed.
#' @param id_col The name of the column containing the poll station ID. Defaults to \code{"id_INE_poll_station"}.
#' @param level The hierarchical level for data aggregation. Can be one of \code{"all"}, \code{"ccaa"}, \code{"prov"}, \code{"mun"}, \code{"mun_district"}, \code{"sec"}, or \code{"poll_station"}. Defaults to \code{"all"}.
#' @param cod_CERA The code representing CERA. Defaults to \code{"999"}.
#' @param prec_round The precision for rounding percentages. Defaults to \code{3}.
#'
#' @return A data frame with the aggregated CERA data, including columns for type of election, election date, census data, total ballots, and turnout percentages.
#'
#' @authors Mikaela DeSmedt(documentation), Javier Álvarez-Liébana.
#' 
#' @source Data extracted and processed from various election sources.
#' 
#' @keywords get_elections_data
#' 
#' @name get_CERA_data
#'
#' @examples
#'
#' ## Get CERA data
#'
#' # Example with proper arguments
#' \dontrun{
#' election_data <- data.frame(
#'   id_INE_poll_station = c("999-123-456-789", "999-987-654-321"),
#'   type_elec = c("congress", "congress"),
#'   date_elec = as.Date(c("2023-06-28", "2023-06-28")),
#'   census_counting = c(1000, 1500),
#'   total_ballots = c(800, 1200)
#' )
#' get_CERA_data(election_data, id_col = "id_INE_poll_station", level = "mun")
#' }
#'
#' # Multiple elections
#' combined_CERA_data <- get_CERA_data(election_data,
#'                                     id_col = "id_INE_poll_station",
#'                                     level = "prov")
#'
#' # Wrong examples
#' \dontrun{
#' get_CERA_data(election_data, id_col = "non_existent_column", level = "invalid_level")
#' }
#'
#' @export
get_CERA_data <- function(election_data, id_col = "id_INE_poll_station",
                          level = "all", cod_CERA = "999", prec_round = 3) {
  
  # Aggregation of data
  hierarchy_levels <- c("ccaa", "prov", "mun", "mun_district", "sec", "poll_station")
  
  # Extract CERA data
  cera_data <- election_data %>%
    filter(grepl(cod_CERA, .data[[id_col]])) %>%
    mutate(cod_INE_ccaa = substr(id_INE_poll_station, 1, 2),
           cod_INE_prov = substr(id_INE_poll_station, 4, 5),
           cod_INE_mun = substr(id_INE_poll_station, 7, 9),
           cod_mun_district = substr(id_INE_poll_station, 11, 12),
           cod_sec = substr(id_INE_poll_station, 14, 15),
           cod_poll_station = substr(id_INE_poll_station, 17, 17))
  
  # Grouping
  if (level == "all") {
    group_vars <- "id_elec"
  } else {
    levels <- hierarchy_levels[1:which(hierarchy_levels == level)]
    group_vars <- c("id_elec", paste0("cod_INE_", levels))
  }
  
  # Summarize CERA data
  data_cera <- cera_data %>%
    group_by(across(all_of(group_vars))) %>%
    summarize(type_elec = unique(type_elec),
              date_elec = unique(date_elec),
              census_cera = sum(census_counting, na.rm = TRUE),
              total_ballots_cera = sum(total_ballots, na.rm = TRUE),
              turnout_cera = round(100 * total_ballots_cera / census_cera, prec_round),
              .groups = 'drop')
  
  return(data_cera)
}

#' @title aggregate_election_data 
#'
#' @description This function aggregates election data at various hierarchical levels, including CERA (Census of Absent Residents) data.
#'
#' @inheritParams get_mun_census_data
#' @param election_data A data frame containing the election data to be processed.
#' @param level The hierarchical level for data aggregation. Can be one of \code{"all"}, \code{"ccaa"}, \code{"prov"}, \code{"mun"}, \code{"mun_district"}, \code{"sec"}, or \code{"poll_station"}. Defaults to \code{"all"}.
#' @param id_col The name of the column containing the poll station ID. Defaults to \code{"id_INE_poll_station"}.
#' @param cod_CERA The code representing CERA. Defaults to \code{"999"}.
#' @param prec_round The precision for rounding percentages. Defaults to \code{3}.
#'
#' @return A data frame with the aggregated election data, including columns for type of election, election date, census data, total ballots, turnout percentages, and resident population.
#'
#' @authors Mikaela DeSmedt(documentation),Javier Álvarez-Liébana.
#' 
#' @source Data extracted and processed from various election sources.
#' 
#' @keywords get_elections_data
#' 
#' @name aggregate_election_data
#'
#' @examples
#'
#' ## Aggregate election data
#'
#' # Example with proper arguments
#' \dontrun{
#' election_data <- data.frame( # Add appropriate data frame here
#'   id_INE_poll_station = c("999-123-456-789"),
#'   type_elec = "congress",
#'   date_elec = as.Date("2023-06-28"),
#'   census_counting = 1000,
#'   total_ballots = 800,
#'   # Add other required columns
#' )
#' aggregate_election_data(election_data, level = "mun", id_col = "id_INE_poll_station")
#' }
#'
#' # Wrong examples
#' \dontrun{
#' aggregate_election_data(election_data, id_col = "non_existent_column", level = "invalid_level")
#' }
#'
#' @export
aggregate_election_data <-
  function(election_data, level = "all", id_col = "id_INE_poll_station",
           cod_CERA = "999", prec_round = 3) {
    
    # Remove duplicates
    election_data <-
      election_data |>
      distinct(id_elec, .data[[id_col]], .keep_all = TRUE)
    
    # Extract cod by level
    if (level != "all") {
      
      hierarchy_levels <- c("ccaa", "prov", "mun", "mun_district",
                            "sec", "poll_station")
      
      levels <- hierarchy_levels[1:which(hierarchy_levels == level)]
      
      for (i in 1:length(levels)) {
        
        election_data <-
          election_data |>
          mutate("cod_INE_{levels[i]}" :=
                   extract_code(.data[[id_col]], level = levels[i]),
                 .after = .data[[id_col]])
        
      }
    }
    
    # Aggregation of data
    if (level == "all") {
      
      levels <- "all"
      group_vars <- "id_elec"
      
    } else {
      
      if (length(levels) <= 3) { # at mun level
        
        group_vars <- c("id_elec", glue("cod_INE_{levels}"), levels)
        
      } else {
        
        group_vars <- c("id_elec", glue("cod_INE_{levels}"), levels[1:3])
      }
      
    }
    
    # n_poll_stations CERA data
    if (length(levels) <= 2) { # at province level or greater
      
      n_poll_stations_CERA <-
        election_data |>
        filter(mun == "CERA") |>
        reframe(n_poll_stations = n(), .by = group_vars)
      
      agg_data <-
        election_data |>
        left_join(n_poll_stations_CERA, by = group_vars) |>
        # Replace NA
        mutate(n_poll_stations = replace_na(n_poll_stations, 0)) |>
        # Summary
        reframe(type_elec = unique(type_elec),
                date_elec = unique(date_elec),
                n_poll_stations =
                  n_distinct(.data[[id_col]]) - unique(n_poll_stations),
                across(c("census_counting", "ballots_1",
                         "ballots_2", "blank_ballots":"total_ballots"), sum),
                turnout =
                  round(100 * total_ballots / census_counting, prec_round),
                porc_valid =
                  round(100 * valid_ballots / total_ballots, prec_round),
                porc_invalid =
                  round(100 * invalid_ballots / total_ballots, prec_round),
                porc_parties =
                  round(100 * party_ballots / valid_ballots, prec_round),
                porc_blank =
                  round(100 * blank_ballots / valid_ballots, prec_round),
                .by = group_vars)
      
    } else {
      
      agg_data <-
        election_data |>
        # Summary
        reframe(type_elec = unique(type_elec),
                date_elec = unique(date_elec),
                n_poll_stations = n_distinct(.data[[id_col]]),
                across(c("census_counting", "ballots_1",
                         "ballots_2", "blank_ballots":"total_ballots"), sum),
                turnout =
                  round(100 * total_ballots / census_counting, prec_round),
                porc_valid =
                  round(100 * valid_ballots / total_ballots, prec_round),
                porc_invalid =
                  round(100 * invalid_ballots / total_ballots, prec_round),
                porc_parties =
                  round(100 * party_ballots / valid_ballots, prec_round),
                porc_blank =
                  round(100 * blank_ballots / valid_ballots, prec_round),
                .by = group_vars) |>
        mutate(n_poll_stations = if_else(mun == "CERA", 0, n_poll_stations))
      
    }
    
    agg_data <-
      agg_data |>
      left_join(get_CERA_data(election_data, id_col = id_col,
                              level = level, cod_CERA = cod_CERA,
                              prec_round = prec_round),
                by = group_vars, suffix = c("", ".y")) |>
      select(-contains(".y")) |>
      # turnout_1 and turnout_2 over counting census without cera.
      mutate(turnout_1 =
               round(100 * ballots_1 / (census_counting - census_cera),
                     prec_round),
             turnout_2 =
               round(100 * ballots_2 / (census_counting - census_cera),
                     prec_round), .before = turnout) |>
      # Relocate columns
      relocate(type_elec, date_elec, .after = id_elec)
    
    # Resident population (without CERA)
    pop_res <-
      election_data |>
      mutate(id_INE_mun =
               extract_code(.data[[id_col]], level = "mun",
                            full_cod = TRUE)) |>
      filter(!str_detect(id_INE_mun, "-999")) |>
      distinct(id_elec, id_INE_mun, .keep_all = TRUE) |>
      summarise(pop_res = sum(pop_res_mun),
                .by = group_vars)
    
    # Join with pop data
    agg_data <-
      agg_data |>
      # Join pop res data
      left_join(pop_res, by = group_vars) |>
      # Relocate
      relocate(pop_res, .after = date_elec)
    
    # Output
    return(agg_data)
  }
