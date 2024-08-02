library(dplyr)
library(rvest)
library(glue)
library(lubridate)
library(janitor)
library(stringr)
library(readr)

# GET LINKS -----
##Generate the historical_survey_links_wikipedia tibble ----
historical_survey_links_wikipedia <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(
    year = year,
    month = month.name[month],
    type_elec = "congress",
    type_survey = "national",
    links = case_when(
      duplicated(year) | duplicated(year, fromLast = TRUE) ~ glue("https://en.wikipedia.org/wiki/Opinion_polling_for_the_{month}_{year}_Spanish_general_election"),
      TRUE ~ glue("https://en.wikipedia.org/wiki/Opinion_polling_for_the_{year}_Spanish_general_election")
    )
  ) %>%
  select(date_elec = date_elec, year, type_elec, type_survey, links)

##Create a list to store the HTML content----
survey_html_list <- list()

# Loop through each link and scrape the HTML content
for (i in seq_along(historical_survey_links_wikipedia$links)) {
  url <- historical_survey_links_wikipedia$links[i]
  year <- historical_survey_links_wikipedia$year[i]
  month <- sprintf("%02d", month(historical_survey_links_wikipedia$date_elec[i]))
  name <- glue("POLL_02{year}{month}")
  
  message("Fetching HTML from: ", url)
  
  try({
    page <- read_html(url)
    survey_html_list[[name]] <- as.character(page)
  }, silent = TRUE)
  
  if (is.null(survey_html_list[[name]])) {
    survey_html_list[[name]] <- NA
  }
}

# Save the list to an RDS file
saveRDS(survey_html_list, file = "survey_html_list.rds")



#GET TABLES----
##Initialize the list to store the cleaned tables ----
raw_voting_intention_tables <- list()

# Loop through each entry in survey_html_list
for (name in names(survey_html_list)) {
  # Extract the HTML content
  html_content <- survey_html_list[[name]]
  
  # Parse the HTML content
  page <- read_html(html_content)
  
  # Extract all tables from the webpage
  tables <- page %>% html_table(fill = TRUE)
  
  # Check if there are any tables
  if (length(tables) > 0) {
    # Extract all headers, including both img and a tags, in order of appearance
    header_elements <- page %>%
      html_nodes(xpath = "//table[1]//th")
    
    # Extract party names from img tags
    header_names_img <- header_elements %>%
      html_nodes("img") %>%
      html_attr("alt")
    
    # Extract party names from a tags
    header_names_a <- header_elements %>%
      html_nodes("a") %>%
      html_attr("title")
    
    # Extract titles from a tags wrapping images
    header_names_img_in_a <- header_elements %>%
      html_nodes("a") %>%
      html_attr("title")
    
    # Combine the extracted names in order of appearance
    header_names <- ifelse(!is.na(header_names_img_in_a), header_names_img_in_a, 
                           ifelse(!is.na(header_names_img), header_names_img, 
                                  ifelse(!is.na(header_names_a), header_names_a, "unknown")))
    
    # Add additional headers at the beginning
    additional_headers <- c("polling_firm", "field_work_date", "sample_size", "turnout")
    header_names <- c(additional_headers, header_names)
    
    # Get the first table to manipulate it
    first_table <- tables[[1]]
    
    # Remove the last column
    first_table <- first_table[, -ncol(first_table)]
    
    # Remove the first four rows
    first_table <- first_table[-c(1:4), ]
    
    # Ensure the number of columns matches the number of headers
    if (ncol(first_table) >= length(header_names)) {
      colnames(first_table)[1:length(header_names)] <- header_names
    } else {
      colnames(first_table) <- c(header_names[1:ncol(first_table)])
    }
    
    # Store the cleaned table in the list
    raw_voting_intention_tables[[name]] <- first_table
  }
}


# CLEAN TABLES ----
clean_table <- function(df, year, cod_elec, month, date_elec) {
  # Identify rows containing the word "elections"
  df <- df %>%
    mutate(has_elections = apply(., 1, function(row) any(grepl("elections", row, ignore.case = TRUE))))
  
  # Find the first occurrence of "elections"
  first_election_row <- which(df$has_elections)[1]
  
  # Slice the data frame up to the first occurrence of "elections"
  if (!is.na(first_election_row)) {
    df <- df %>% slice(1:(first_election_row - 1))
  }
  
  # Continue with the cleaning process
  df_cleaned <- df %>%
    janitor::clean_names() %>%
    mutate(polling_firm = str_remove_all(polling_firm, "\\[.*?\\]")) %>%
    mutate(
      fieldwork_start = str_extract(field_work_date, "^\\d+\\s+\\w{3}"),
      fieldwork_end = str_extract(field_work_date, "\\d+\\s+\\w{3}"),
      fieldwork_start = dmy(paste(fieldwork_start, year)),
      fieldwork_end = dmy(paste(fieldwork_end, year))
    ) %>%
    mutate(
      fieldwork_start = if_else(is.na(fieldwork_start) & !is.na(fieldwork_end), 
                                fieldwork_end - days(2), 
                                fieldwork_start),
      fieldwork_end = if_else(is.na(fieldwork_end), fieldwork_start, fieldwork_end)
    ) %>%
    mutate(
      fieldwork_start = as.Date(fieldwork_start),
      fieldwork_end = as.Date(fieldwork_end)
    ) %>%
    mutate(fieldwork_duration = as.numeric(fieldwork_end - fieldwork_start)) %>%
    mutate(turnout = case_when(
      str_detect(turnout, "–") ~ {
        values <- str_split(turnout, "–", simplify = TRUE)
        mean(as.numeric(values), na.rm = TRUE)
      },
      TRUE ~ as.numeric(str_replace_all(turnout, "[^0-9.]", ""))
    )) %>%
    mutate(sample_size = as.numeric(str_replace_all(sample_size, "[^0-9]", ""))) %>%
    mutate(across(6:ncol(.), ~ {
      numeric_value <- as.numeric(str_replace_all(., "[^0-9.]", ""))
      numeric_value <- if_else(str_detect(., "\\?"), NA_real_, numeric_value)
      truncated_value <- floor(numeric_value * 10) / 10
      return(truncated_value)
    })) %>%
    # Extract media part from polling_firm
    mutate(media = if_else(str_detect(polling_firm, "/"), 
                           str_extract(polling_firm, "(?<=/).*"), 
                           NA_character_)) %>%
    mutate(polling_firm = str_remove(polling_firm, "/.*")) %>%  # Remove the media part from polling_firm
    mutate(date_elec = as.Date(date_elec)) %>%
    mutate(is_exit_poll = fieldwork_duration <= 1 & fieldwork_end == date_elec) %>%
    relocate(fieldwork_start, fieldwork_end, fieldwork_duration, .after = polling_firm) %>%
    relocate(date_elec, .after = fieldwork_duration) %>%
    relocate(is_exit_poll, .after = date_elec) %>%
    relocate(media, .after = polling_firm) %>%
    select(-has_elections, -field_work_date, -cod_elec)  # Remove the helper column after its use
  
  return(df_cleaned)
}





# Extract and clean tables
cleaned_voting_intention_tables <- lapply(names(raw_voting_intention_tables), function(name) {
  # Extract the cod_elec and date information from the table name
  year <- as.numeric(str_sub(name, 8, 11))
  month <- as.numeric(str_sub(name, 12, 13))
  
  raw_table <- raw_voting_intention_tables[[name]]
  
  # Create a unique key for filtering
  unique_key <- paste(year, month, sep = "-")
  
  # Get the corresponding date_elec from dates_elections_spain
  date_elec <- dates_elections_spain %>%
    filter(cod_elec == "02") |> 
    mutate(unique_key = paste(year, month, sep = "-")) %>%
    filter(unique_key == !!unique_key) %>%
    pull(date_elec)
  
  # Ensure date_elec is of length 1
  if (length(date_elec) != 1) {
    stop("date_elec must be of length 1, but got: ", length(date_elec))
  }
  
  cleaned_table <- clean_table(raw_table, year, cod_elec, month, date_elec)
  
  return(cleaned_table)
})


names(cleaned_voting_intention_tables) <- names(raw_voting_intention_tables)



# Ensure the cleaned tables list is not empty
#if (length(cleaned_voting_intention_tables) > 0) {
#  # Define the output directory
#  output_directory <- "C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/survey data"
#  
#  # Check if the directory exists
#  if (!dir.exists(output_directory)) {
#    stop("Output directory does not exist: ", output_directory)
#  }
#  
#  # Save cleaned tables to CSV files
#  for (name in names(cleaned_voting_intention_tables)) {
#    file_path <- file.path(output_directory, paste0(name, ".csv"))
#    tryCatch({
#      write_csv(cleaned_voting_intention_tables[[name]], file_path)
#      message("Saved file: ", file_path)
#    }, error = function(e) {
#      message("Failed to save file: ", file_path, " - ", e$message)
#    })
#  }
#} else {
#  message("No tables to save.")
#}



# Ensure the cleaned tables list is not empty
if (length(cleaned_voting_intention_tables) > 0) {
  # Define the output directory
  output_directory <- "C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/survey data"
  
  # Check if the directory exists
  if (!dir.exists(output_directory)) {
    stop("Output directory does not exist: ", output_directory)
  }
  
  # Save cleaned tables to RDA files
  for (name in names(cleaned_voting_intention_tables)) {
    file_path <- file.path(output_directory, paste0(name, ".rda"))
    tryCatch({
      # Save the data frame to an RDA file with the object name being the same as the table name
      assign(name, cleaned_voting_intention_tables[[name]])
      save(list = name, file = file_path)
      message("Saved file: ", file_path)
    }, error = function(e) {
      message("Failed to save file: ", file_path, " - ", e$message)
    })
  }
} else {
  message("No tables to save.")
}
