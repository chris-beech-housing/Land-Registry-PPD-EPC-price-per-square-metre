# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)

options(scipen = 100)

## Read all EPC certificates.csv files and re-write with only the desired columns
# https://get-energy-performance-data.communities.gov.uk/

# Function to process each certificates-YYYY.csv file
process_certificates_file <- function(file_path) {
  tryCatch(
    {
      # Extract the suffix (e.g., "-2012") from the filename
      file_name <- basename(file_path)
      suffix <- str_extract(file_name, "-[^.]+")
      folder_path <- dirname(file_path)
      new_file_path <- file.path(folder_path, paste0("certificates_ucl", suffix, ".csv"))

      read_csv(file_path, show_col_types = FALSE) |>
        select(
          address1,
          address2,
          address3,
          address,
          postcode,
          property_type,
          inspection_date,
          lodgement_date,
          total_floor_area,
          number_habitable_rooms
        ) |>
        write_csv(new_file_path)
    },
    error = function(e) {
      message("Error processing file: ", file_path, "\n", e)
    }
  )
}

# Main function to find and process certificates-YYYY.csv files
process_certificates_folder <- function(folder_path) {
  list.files(
    path = folder_path,
    pattern = "^certificates-.*\\.csv$",
    recursive = FALSE,
    full.names = TRUE
  ) |>
    walk(process_certificates_file)
}

# Run the function
process_certificates_folder(
  "Data/domestic-csv"
)

# Function to read each certificates.csv file
read_certificates <- function(file_path) {
  tryCatch(
    read_csv(file_path, show_col_types = FALSE),
    error = function(e) {
      message("Error reading file: ", file_path, "\n", e)
      NULL
    }
  )
}

# Combine all certificates_ucl-YYYY.csv files
combine_certificates_from_subfolders <- function(folder_path, output_file) {
  list.files(
    path = folder_path,
    pattern = "^certificates_ucl-.*\\.csv$",
    recursive = FALSE,
    full.names = TRUE
  ) |>
    map(read_certificates) |>
    compact() |>
    bind_rows() |>
    distinct() |>
    arrange(lodgement_date, total_floor_area) |>
    mutate(
      add1 = toupper(address1),
      add2 = toupper(address2),
      add3 = toupper(address3),
      add = toupper(address),
      id = row_number()
    ) |>
    select(
      add1,
      add2,
      add3,
      add,
      postcode = postcode,
      propertytype = property_type,
      inspectiondate = inspection_date,
      lodgementdate = lodgement_date,
      tfarea = total_floor_area,
      numberrooms = number_habitable_rooms,
      id
    ) |>
    write_csv(output_file)
}

# Run the function
combine_certificates_from_subfolders(
  folder_path = "Data/domestic-csv",
  output_file = "Data/combined_certificates_ucl.csv"
)
