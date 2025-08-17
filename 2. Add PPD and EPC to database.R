# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)
library(stringi)
library(DBI)
library(duckdb)

options(scipen = 100)

## Import data

# England and Wales price paid data: https://www.gov.uk/guidance/about-the-price-paid-data

column_names <- c(
  "Transaction unique identifier",
  "Price",
  "Date of Transfer",
  "Postcode",
  "Property Type",
  "Old/New",
  "Duration",
  "PAON",
  "SAON",
  "Street",
  "Locality",
  "Town/City",
  "District",
  "County",
  "PPD Category Type",
  "Record Status"
)

column_types <- cols(
  "Transaction unique identifier" = col_character(),
  "Price" = col_integer(),
  "Date of Transfer" = col_datetime(),
  "Postcode" = col_character(),
  "Property Type" = col_factor(),
  "Old/New" = col_factor(),
  "Duration" = col_factor(),
  "PAON" = col_character(),
  "SAON" = col_character(),
  "Street" = col_character(),
  "Locality" = col_character(),
  "Town/City" = col_character(),
  "District" = col_character(),
  "County" = col_character(),
  "PPD Category Type" = col_factor(),
  "Record Status" = col_factor()
)

ppd <-
  read_csv(
    "Data/pp-complete.csv",
    col_names = column_names,
    col_types = column_types
  ) |>
  select(-`Transaction unique identifier`) |> # there are duplicate rows with identical columns except for the second part of this id
  distinct() |> # remove the duplicates
  filter(`Property Type` != "O") |>
  arrange(Postcode) |>
  mutate(transactionid = row_number(), yearchi = year(`Date of Transfer`)) |>
  mutate(across(where(is.character), ~ replace_na(.x, ""))) |>
  rename(
    price = Price,
    dateoftransfer = `Date of Transfer`,
    postcode = Postcode,
    propertytype = `Property Type`,
    oldnew = `Old/New`,
    duration = Duration,
    paon = PAON,
    saon = SAON,
    street = Street,
    locality = Locality,
    towncity = `Town/City`,
    district = District,
    county = County,
    categorytype = `PPD Category Type`,
    recordstatus = `Record Status`
  ) |>
  select(transactionid, everything(), yearchi) |>
  mutate(saon = stri_replace_all_regex(saon, "[\\[\\]\\(\\)\\{\\}]", " ")) # replace brackets by space

epc <-
  read_csv("Data/combined_certificates_ucl.csv") |>
  mutate(across(where(is.character), ~ replace_na(.x, ""))) |>
  mutate(
    add1 = stri_trim_both(add1),
    add2 = stri_trim_both(add2),
    add3 = stri_trim_both(add3),
    add = stri_trim_both(add)
  ) |>
  distinct() |>
  mutate(across(where(is.character), ~ stri_replace_all_regex(.x, "[\\[\\]\\(\\)\\{\\}]", " "))) |> # replace brackets by space; e.g. for a handful of Flat 1(2 xyz) type addresses
  arrange(postcode)

# Keep only epc and ppd records with postcodes in common
postcodes <- intersect(unique(epc$postcode), unique(ppd$postcode))
epc <- epc |> filter(postcode %in% postcodes) |> distinct()
ppd <- ppd |> filter(postcode %in% postcodes) |> distinct()

# Write data to database
con <- dbConnect(duckdb::duckdb(), dbdir = "Data/datajournal.duckdb")

dbWriteTable(con, "epc", epc, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "transactions", ppd, overwrite = TRUE, row.names = FALSE)

dbDisconnect(con)
