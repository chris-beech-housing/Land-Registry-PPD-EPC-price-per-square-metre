# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)

options(scipen = 100)

## Read all EPC certificates.csv files and re-write with only the desired columns
# https://epc.opendatacommunities.org/

# Function to process each certificates.csv file
process_certificates_file <- function(file_path) {
  tryCatch(
    {
      # Define the new file path as certificates_ucl.csv in the same folder
      folder_path <- dirname(file_path)
      new_file_path <- file.path(folder_path, "certificates_ucl.csv")

      read_csv(file_path, show_col_types = FALSE) |>
        select(
          ADDRESS1,
          ADDRESS2,
          ADDRESS3,
          ADDRESS,
          POSTCODE,
          PROPERTY_TYPE,
          INSPECTION_DATE,
          LODGEMENT_DATE,
          TOTAL_FLOOR_AREA,
          NUMBER_HABITABLE_ROOMS
        ) |>
        write_csv(new_file_path)

      # Remove recommendations.csv in the same folder
      folder_path <- dirname(file_path)
      recommendations_path <- file.path(folder_path, "recommendations.csv")
      if (file.exists(recommendations_path)) {
        file.remove(recommendations_path)
      }
    },
    error = function(e) {
      message("Error processing file: ", file_path, "\n", e)
    }
  )
}

# Main function to recursively find and process certificates.csv files
process_certificates_folder <- function(folder_path) {
  list.files(
    path = folder_path,
    pattern = "\\certificates.csv$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
    walk(process_certificates_file)
}

# Run the function
process_certificates_folder(
  "Data/all-domestic-certificates"
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

# Combine all certificates_ucl.csv files from subfolders
combine_certificates_from_subfolders <- function(folder_path, output_file) {
  list.files(
    path = folder_path,
    pattern = "\\certificates_ucl.csv$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
    map(read_certificates) |>
    compact() |>
    bind_rows() |>
    distinct() |>
    arrange(LODGEMENT_DATE, TOTAL_FLOOR_AREA) |>
    mutate(
      add1 = toupper(ADDRESS1),
      add2 = toupper(ADDRESS2),
      add3 = toupper(ADDRESS3),
      add = toupper(ADDRESS),
      id = row_number()
    ) |>
    select(
      add1,
      add2,
      add3,
      add,
      postcode = POSTCODE,
      propertytype = PROPERTY_TYPE,
      inspectiondate = INSPECTION_DATE,
      lodgementdate = LODGEMENT_DATE,
      tfarea = TOTAL_FLOOR_AREA,
      numberrooms = NUMBER_HABITABLE_ROOMS,
      id
    ) |>
    write_csv(output_file)
}

# Run the function
combine_certificates_from_subfolders(
  folder_path = "Data/all-domestic-certificates",
  output_file = "Data/combined_certificates_ucl.csv"
)
