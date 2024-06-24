# Install and load required packages
install.packages("rvest")
install.packages("dplyr")

library(rvest)
library(dplyr)
library(tidyverse)

# Define the URL and read the webpage
url <- "https://es.wikipedia.org/wiki/Anexo:Elecciones_en_Espa%C3%B1a"
webpage <- read_html(url)

# Extract all tables from the webpage
tables <- webpage %>% html_table(fill = TRUE)

# Inspect the first few tables to see which one contains the election data

# Select tables
referendum_elections <- tables[[10]]
eu_elections <- tables[[12]]
general_elections <- tables[[13]]
ccaa_elections <- tables[[14]]
municipal_elections <- tables[[15]]

# Cleaning

referendum_elections <- referendum_elections |> 
  mutate(full_date = paste(Año,Día))

referendum_elections$date <- sapply(referendum_elections$full_date,convert_spanish_date)


#---------------- 

# Function to convert "1976 15 de diciembre" to "YYYY/MM/DD"
convert_spanish_date <- function(date_string) {
  # Convert the Spanish month to a number
  months <- c("enero" = 1, "febrero" = 2, "marzo" = 3, "abril" = 4,
              "mayo" = 5, "junio" = 6, "julio" = 7, "agosto" = 8,
              "septiembre" = 9, "octubre" = 10, "noviembre" = 11, "diciembre" = 12)
  
  # Extract year, day, and month from the string
  parts <- strsplit(date_string, " ")[[1]]
  year <- as.numeric(parts[1])
  day <- as.numeric(parts[2])
  month <- months[parts[4]]
  
  # Create a date object
  date_obj <- make_date(month = month, day = day, year = year)
  
  # Format the date as "YYYY/MM/DD"
  formatted_date <- format(date_obj, "%Y/%m/%d")
  return(formatted_date)
}
