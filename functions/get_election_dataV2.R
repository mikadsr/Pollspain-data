#' @title Get municipal census data
#'
#' @description Get municipal census data for a particular election at municipal
#' level. This function is a wrapper of \code{import_raw_mun_MIR_files()}
#' function for a set of elections
#'
#' @inheritParams type_to_code_election
#' @param year,month vector of years and months of elections to be considered.
#' Please be sure (see \code{dates_elections_spain}) that elections of the
#' provided type are available for the given year and month.
#'
#' @return A tibble (as many rows as municipalities for each election) with the
#' following elements
#' \item{cod_elec}{code of type of elections: \code{"01"} (referendum),
#' \code{"02"} (congress), \code{"03"} (senate), \code{"04"} (local elections),
#' \code{"06"} (cabildo - Canarian council - elections), \code{"07"}
#' (European Parlament elections)}
#' \item{type_elec}{type of election.}
#' \item{date_elec}{date of election.}
#' \item{id_INE_mun}{municipality's id build from ccaa-prov-mun codes provided
#' by INE.}
#' \item{id_MIR_mun}{municipality's id build from ccaa-prov-mun codes provided
#' by Spanish Ministry of Interior (MIR).}
#' \item{cod_INE_ccaa, cod_MIR_ccaa, ccaa}{codes and name for regions (ccaa)
#' to which it belongs municipalities.}
#' \item{cod_INE_prov, prov}{codes and name for provinces to which it belongs
#' municipalities.}
#' \item{cod_INE_mun, cd_INE_mun, mun}{code, digit control and name for
#' municipalities.}
#' \item{cod_mun_jud_district, cod_mun_prov_council}{codes of judicial
#' district and province council.}
#' \item{n_poll_stations}{number of poll stations at each mun}
#' \item{pop_res_mun}{census of people who are living (CER + CERA).}
#' \item{census_INE_mun}{people from \code{pop_res_mun} who are
#' allowed to vote.}
#' \item{census_counting_mun}{people from \code{census_INE_mun} after claims.}
#' \item{census_CERE_mun}{ensus of foreigners, just for EU elections.}
#'
#' @authors Mikaela DeSmedt, Javier Álvarez-Liébana.
#' @source Some definitions of variables were extracted from
#' \url{https://www.ige.gal}
#' @keywords get_elections_data
#' @name get_mun_census_data
#' @import crayon
#' @examples
#'
#' ## Get mun census data
#' data("dates_elections_spain")
#' \dontrun{
#' # Right examples
#' mun_census_data <- get_mun_census_data("congress", 2019, 4)
#' mun_census_data <- get_mun_census_data("senate", 2019, 11)
#' mun_census_data <- get_mun_census_data(rep("congress", 3),
#'                                        c(2019, 2019, 2016),
#'                                        c(11, 4, 6))
#' mun_census_data <- get_mun_census_data(c("congress", "senate"),
#'                                        c(2019, 2019), c(11, 4))
#' # Wrong examples
#' mun_census_data <- get_mun_census_data("national", 2019, 4)
#' mun_census_data <- get_mun_census_data("congress", 2016, c(4, 11))
#' mun_census_data <- get_mun_census_data("congress", "2016-06-26")
#' }
#'
#' @export
#' 
get_mun_census_data <- function(type_elec, year, month) {
  # Check if the election type is valid and get the corresponding code
  election_info <- type_to_code_election(type_elec)
  
  # Construct the URL for the specific election directory
  base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
  election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year}{sprintf('%02d', month)}")
  
  # Print the constructed URL
  message("Fetching from URL: ", election_dir)
  
  # Construct the URL for the raw data file
  file_url <- glue("{election_dir}/raw_mun_data_{type_elec}_{year}_{month}.csv")
  
  # Print the file URL
  message("Fetching data from URL: ", file_url)
  
  # Fetch the file
  response <- GET(file_url)
  if (status_code(response) != 200) {
    stop("Failed to fetch the data file")
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
  
  return(mun_data)
}



#' @title Get poll station data
#'
#' @description Fetches and processes poll station data for a given election type, year, and month.
#'
#' @param type_elec The type of election (e.g., "congress").
#' @param year The year of the election.
#' @param month The month of the election.
#' @param prec_round Rounding accuracy. Defaults to \code{prec_round = 3}.
#'
#' @return A data frame containing poll station data with additional computed columns.
#'
#' @authors Mikaela DeSmedt, Javier Álvarez-Liébana. 
#' @source Data fetched from the specified GitHub repository.
#' @keywords get_elections_data
#' @name get_poll_station_data
#'
#' @examples
#'
#' ## Get poll station data
#'
#' # Right examples
#' \dontrun{
#' poll_station_data <- get_poll_station_data("congress", 1986, 6)
#' head(poll_station_data)
#' }
#'
#' # Wrong examples
#' \dontrun{
#' # poll_station_data <- get_poll_station_data("invalid_election", 1986, 6)
#' }
#'
#' @export
get_poll_station_data <- function(type_elec, year, month, prec_round = 3) {
  # Check if the provided election type is valid
  election_info <- type_to_code_election(type_elec)
  
  # Base URL for data
  base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
  
  # Construct the URL for the specific election directory
  election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year}{sprintf('%02d', month)}")
  message("Fetching from URL: ", election_dir)
  
  # Construct the URL for the specific file
  data_url <- glue("{election_dir}/raw_poll_station_data_{type_elec}_{year}_{month}.csv")
  message("Fetching data from URL: ", data_url)
  
  # Fetch the data
  poll_station_data <- read_csv(data_url, show_col_types = FALSE)
  
  # Debug: Check if the data was read correctly
  if (nrow(poll_station_data) == 0) {
    stop("No data read from the CSV file")
  }
  
  # Debug: Print the column names to verify the expected columns are present
  print(colnames(poll_station_data))
  
  # Process the data
  poll_station_data <- poll_station_data %>%
    mutate(valid_ballots = blank_ballots + party_ballots,
           total_ballots = valid_ballots + invalid_ballots) %>%
    filter(cod_INE_mun != "999") %>%
    left_join(get_mun_census_data(type_elec, year, month),
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
  
  return(poll_station_data)
}


##' @title Get candidates data
##'
##' @description Fetch and process candidates data for Spanish elections.
##'
##' @inheritParams get_mun_census_data
##'
##' @return A data frame containing the candidates data.
##'
##' @authors Mikaela DeSmedt, Javier Álvarez-Liébana. 
##' @source Data extracted from \href{https://github.com/mikadsr/Pollspain-data}{Pollspain Data Repository}
##' @keywords get_elections_data
##' @name get_candidates_data
##'
##' @examples
##'
##' ## Get candidates data
##'
##' # Right examples
##' candidates_data <- get_candidates_data("congress", 1986, 6)
##'
##' @export
# Function to get candidates data
get_candidates_data <- function(type_elec, year, month) {
  # Validate election type
  if (!type_elec %in% c("referendum", "congress", "senate", "local", "regional", "cabildo", "EU")) {
    stop("Invalid election type. Allowed types are: 'referendum', 'congress', 'senate', 'local', 'regional', 'cabildo', 'EU'")
  }
  
  # Get election info
  election_info <- type_to_code_election(type_elec)
  
  # Check if the requested elections are available
  elections_allowed <- dates_elections_spain %>%
    filter(year >= 1986) %>%
    inner_join(tibble(cod_elec = election_info$cod_elec, type_elec, year, month),
               by = c("cod_elec", "type_elec", "year", "month"))
  
  if (nrow(elections_allowed) == 0) {
    stop(glue("No {type_elec} elections are available in {month}-{year}"))
  }
  
  # Construct the URL for the specific election directory
  base_url <- "https://raw.githubusercontent.com/mikadsr/Pollspain-data/main"
  election_dir <- glue("{base_url}/{election_info$dir}/{election_info$cod_elec}{year}{sprintf('%02d', month)}")
  data_url <- glue("{election_dir}/raw_candidates_{type_elec}_{year}_{month}.csv")
  message("Fetching from URL: ", election_dir)
  message("Fetching data from URL: ", data_url)
  
  # Collect candidates data
  candidates_data <- tryCatch({
    read_csv(data_url, show_col_types = FALSE)
  }, error = function(e) {
    stop("Failed to fetch or read candidates data.")
  })
  
  # Clean and process candidates data
  candidates_data <- candidates_data %>%
    filter(type_elec %in% type_elec & year(date_elec) %in% year & month(date_elec) %in% month) %>%
    mutate(cod_mun_district = ifelse(cod_mun_district == "9", NA, cod_mun_district),
           cod_INE_mun = ifelse(cod_INE_mun == "999", NA, cod_INE_mun))
  
  return(candidates_data)
}
