# ------------------------------------------------
# PPD_EPC_linkage.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. This code only allows to used for academic purpose.
# ------------------------------------------------
# Author: Bin Chi, Adam Dennett
# UCL Centre for Advanced Spatial Analysis
# bin.chi.16@ucl.ac.uk; a.dennett@ucl.ac.uk
# Date: 20/12/2019

# Re-factored for 18x faster speed, by Chris Beech; summer 2025, published 2025-08-17
# https://github.com/chris-beech-housing/Land-Registry-PPD-EPC-price-per-square-metre

# -------------------------------------------------

# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)
library(stringi)
library(DBI)
library(duckdb)

options(scipen = 100)

# anti_join_original_variables returns the unmatched records with the 12 original variables thereby saving memory
anti_join_original_variables <- function(x, y) {
  anti_join(x, y, by = "transactionid") |>
    select(
      transactionid,
      postcode,
      propertytype,
      paon,
      saon,
      street,
      locality
    )
}

# numextract extracts the first number in the string
numextract <- function(string) {
  stri_extract_first_regex(string, "\\-*\\d+\\.*\\d*")
}

# Thanks to ChatGPT, we can define beg2char() and char2end() using stringi:
beg2char <- function(text, char, include = FALSE, fixed = TRUE) {
  # Find position of first match
  pos <- if (fixed) {
    stri_locate_first_fixed(text, char)[, "end"]
  } else {
    stri_locate_first_regex(text, char)[, "end"]
  }

  # Adjust position if not including the match
  if (!include) pos <- pos - nchar(char)

  # Handle NA positions gracefully
  pos[is.na(pos)] <- stri_length(text[is.na(pos)])

  # Extract substring
  stri_sub(text, from = 1, to = pos)
}

char2end <- function(text, char, include = FALSE, fixed = TRUE) {
  # Find position of first match
  pos <- if (fixed) {
    stri_locate_first_fixed(text, char)[, "start"]
  } else {
    stri_locate_first_regex(text, char)[, "start"]
  }

  # Adjust position if not including the match
  if (!include) pos <- pos + nchar(char)

  # Handle NA positions gracefully
  pos[is.na(pos)] <- stri_length(text[is.na(pos)]) + 1

  # Extract substring
  stri_sub(text, from = pos)
}

# ------------------------------------------------

# Creates a connection to the datajournal database
con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "Data/datajournal.duckdb"
)

# Read the Land Registry PPD from duckDB
tran <-
  dbGetQuery(
    con,
    "select transactionid, postcode, propertytype, paon, saon, street, locality from transactions"
  )

# Read the Domestic EPCs from duckDB, we only need a subset of variables here
epc <- dbGetQuery(con, "select add1, add2, add3, add, postcode, propertytype, id from epc")

# ------------------stage 1------------------------------
###############matching rule 1 saonpaonstreet= ADDRE################
tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = " "))
tran$saonpaonstreet <- stri_replace_all_fixed(tran$saonpaonstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba1 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 2 saonpaonstreet1= ADDRE################

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreet1 <- stri_replace_all_fixed(tran$saonpaonstreet1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba2 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 3  saonpaonstreet2= ADDRE ################

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet2 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = ", "))
tran$saonpaonstreet2 <- stri_replace_all_fixed(tran$saonpaonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba3 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

tran <- anti_join_original_variables(tran, taba1)
tran <- anti_join_original_variables(tran, taba2)
tran <- anti_join_original_variables(tran, taba3)

###############matching rule 4 saonpaonstreetn=ADDC################
tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = " "))
tran$saonpaonstreetn <- stri_replace_all_fixed(tran$saonpaonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba4 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 5 saonpaonstreetn1=ADDC ################
tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn1 <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = ", "))
tran$saonpaonstreetn1 <- stri_replace_all_fixed(tran$saonpaonstreetn1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba5 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 6 saonpaonstreetn2=ADDC################
tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn2 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = ", "))
tran$saonpaonstreetn2 <- stri_replace_all_fixed(tran$saonpaonstreetn2, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba6 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

tran <- anti_join_original_variables(tran, taba4)
tran <- anti_join_original_variables(tran, taba5)
tran <- anti_join_original_variables(tran, taba6)

###############matching rule 7 saonpaonlo =ADDRE################
tran$saonpaon1 <- stri_paste(tran$saon, tran$paon, sep = " ")
tran$saonpaonlo <- stri_paste(tran$saonpaon1, tran$locality, sep = ", ")
tran$saonpaonlo <- stri_replace_all_fixed(tran$saonpaonlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba7 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 8 saonpaonlon= ADDC  ################
tran <- anti_join_original_variables(tran, taba7)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$localityn <- stri_replace_all_regex(tran$locality, "['.]", "")
tran$saonpaonn1 <- stri_paste(tran$saonn, tran$paonn, sep = " ")
tran$saonpaonlon <- stri_paste(tran$saonpaonn1, tran$localityn, sep = ", ")
tran$saonpaonlon <- stri_replace_all_fixed(tran$saonpaonlon, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba8 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

################matching rule 9 saonpaonlon = ADDCC####################
tran <- anti_join_original_variables(tran, taba8)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$localityn <- stri_replace_all_regex(tran$locality, "['.]", "")
tran$saonpaonn1 <- stri_paste(tran$saonn, tran$paonn, sep = " ")
tran$saonpaonlon <- stri_paste(tran$saonpaonn1, tran$localityn, sep = ", ")
tran$saonpaonlon <- stri_replace_all_fixed(tran$saonpaonlon, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba9 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 10 saonpaonstreet = ADD12################
tran <- anti_join_original_variables(tran, taba9)

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = " "))
tran$saonpaonstreet <- stri_replace_all_fixed(tran$saonpaonstreet, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba10 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 11 saonpaonstreet1 = ADD12################
tran <- anti_join_original_variables(tran, taba10)

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreet1 <- stri_replace_all_fixed(tran$saonpaonstreet1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba11 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 12 saonpaonstreet2= ADD12 ################
tran <- anti_join_original_variables(tran, taba11)

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet2 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = ", "))
tran$saonpaonstreet2 <- stri_replace_all_fixed(tran$saonpaonstreet2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba12 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 13 saonpaonstreetn =ADD12C################
tran <- anti_join_original_variables(tran, taba12)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = ","))
tran$saonpaonstreetn <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = " "))
tran$saonpaonstreetn <- stri_replace_all_fixed(tran$saonpaonstreetn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba13 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 14 saonpaonstreetn1=ADD12C ################
tran <- anti_join_original_variables(tran, taba13)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn1 <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = ", "))
tran$saonpaonstreetn1 <- stri_replace_all_fixed(tran$saonpaonstreetn1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba14 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 15 saonpaonstreetn2=ADD12C  ################
tran <- anti_join_original_variables(tran, taba14)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn2 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = ", "))
tran$saonpaonstreetn2 <- stri_replace_all_fixed(tran$saonpaonstreetn2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba15 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 16 saonpaonstreet3= ADD12################
tran <- anti_join_original_variables(tran, taba15)

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet3 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = " "))
tran$saonpaonstreet3 <- stri_replace_all_fixed(tran$saonpaonstreet3, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba16 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 17 saonpaonstreetn3 = ADD12C1################
tran <- anti_join_original_variables(tran, taba16)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn3 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = " "))
tran$saonpaonstreetn3 <- stri_replace_all_fixed(tran$saonpaonstreetn3, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba17 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 18 saonpaonstreetlo =ADDRE################
tran <- anti_join_original_variables(tran, taba17)

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreetlo <- stri_paste(tran$saonpaonstreet1, tran$locality, sep = ", ")
tran$saonpaonstreetlo <- stri_replace_all_fixed(tran$saonpaonstreetlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba18 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 19 saonpaonstreetnlo= ADDC ################
tran <- anti_join_original_variables(tran, taba18)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn1 <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = ", "))
tran$localityn <- stri_replace_all_regex(tran$locality, "['.]", "")
tran$saonpaonstreetnlo <- stri_paste(tran$saonpaonstreetn1, tran$localityn, sep = ", ")
tran$saonpaonstreetnlo <- stri_replace_all_fixed(tran$saonpaonstreetnlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba19 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetnlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 20 saonpaonstreetlo =ADD12################
tran <- anti_join_original_variables(tran, taba19)

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreetlo <- stri_paste(tran$saonpaonstreet1, tran$locality, sep = ", ")
tran$saonpaonstreetlo <- stri_replace_all_fixed(tran$saonpaonstreetlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba20 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 21 saonpaonstreet3 = ADDRE ################
tran <- anti_join_original_variables(tran, taba20)

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet3 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = " "))
tran$saonpaonstreet3 <- stri_replace_all_fixed(tran$saonpaonstreet3, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba21 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 22 saonpaonstreetn3= ADDC################
tran <- anti_join_original_variables(tran, taba21)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn3 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = " "))
tran$saonpaonstreetn3 <- stri_replace_all_fixed(tran$saonpaonstreetn3, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba22 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 23 saonpaonlo=ADD12################
tran <- anti_join_original_variables(tran, taba22)

tran$saonpaon1 <- stri_paste(tran$saon, tran$paon, sep = " ")
tran$saonpaonlo <- stri_paste(tran$saonpaon1, tran$locality, sep = ", ")
tran$saonpaonlo <- stri_replace_all_fixed(tran$saonpaonlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba23 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 24 saonpaonlon = ADD12C ################
tran <- anti_join_original_variables(tran, taba23)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$localityn <- stri_replace_all_regex(tran$locality, "['.]", "")
tran$saonpaonn1 <- stri_paste(tran$saonn, tran$paonn, sep = " ")
tran$saonpaonlon <- stri_paste(tran$saonpaonn1, tran$localityn, sep = ", ")
tran$saonpaonlon <- stri_replace_all_fixed(tran$saonpaonlon, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba24 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 25 saonpaon1 = ADDRE################
tran <- anti_join_original_variables(tran, taba24)

tran$saonpaon1 <- stri_paste(tran$saon, tran$paon, sep = " ")
tran$saonpaon1 <- stri_replace_all_fixed(tran$saonpaon1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba25 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 26 saonpaonstreet31 = ADDREC################
tran <- anti_join_original_variables(tran, taba25)

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet3 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = " "))
tran$saonpaonstreet31 <- stri_replace_all_regex(tran$saonpaonstreet3, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba26 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet31" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 27 saonpaonstreetn31 = ADDC3################
tran <- anti_join_original_variables(tran, taba26)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")

tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn3 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = " "))
tran$saonpaonstreetn31 <- stri_replace_all_regex(tran$saonpaonstreetn3, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba27 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn31" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

tran <- anti_join_original_variables(tran, taba27)

###############combine the results of matching rules 1 to 27 ################

# Create a list
taba_list <- mget(ls(pattern = "^taba"))

#casa1 is the linked result through stage1
casa1 <- reduce(taba_list, union)

# save casa1 in duckDB and name as casabin1
dbWriteTable(con, "casabin1", casa1, overwrite = TRUE, row.names = FALSE)

# Remove all the matching results
rm(list = ls(pattern = "^taba"))

# Save the unnmatched tran in duckDB as tranbin1
dbWriteTable(con, "tranbin1", tran, overwrite = TRUE, row.names = FALSE)

# -------------------stage 2------------------------------
###############prepare for stage 2###################

# tran2 is the transaction records which saon is null
tran2 <- tran[tran$saon == "", ]

# tran2 is the transaction records which saon is not null
tran3 <- tran[tran$saon != "", ]

# Save tran2 and tran3 in duckDB
dbWriteTable(con, "tranbin2", tran2, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "tranbin3", tran3, overwrite = TRUE, row.names = FALSE)

# Tidy up
rm(tran, tran3)

epc <- epc[epc$postcode %in% unique(tran2$postcode), ]

###############matching rule 28 paonstreetlo = ADDRE#################
tran2$paonstreet <- stri_paste(tran2$paon, tran2$street, sep = ", ")
tran2$paonstreetlo <- stri_paste(tran2$paonstreet, tran2$locality, sep = ", ")
tran2$paonstreetlo <- stri_replace_all_fixed(tran2$paonstreetlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba28 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 29 paonstreetnlo  = ADDC#################
tran2 <- anti_join_original_variables(tran2, taba28)

tran2$saonn <- stri_replace_all_fixed(tran2$saon, "/", "")
tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn <- stri_replace_all_fixed(tran2$street, "'", "")
tran2$localityn <- stri_replace_all_regex(tran2$locality, "['.]", "")
tran2$paonstreetn <- stri_paste(tran2$paonn, tran2$streetn, sep = ", ")
tran2$paonstreetnlo <- stri_paste(tran2$paonstreetn, tran2$localityn, sep = ", ")
tran2$paonstreetnlo <- stri_replace_all_fixed(tran2$paonstreetnlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba29 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetnlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 30 paonstreetlo =ADD12#################
tran2 <- anti_join_original_variables(tran2, taba29)

tran2$paonstreet <- stri_paste(tran2$paon, tran2$street, sep = ", ")
tran2$paonstreetlo <- stri_paste(tran2$paonstreet, tran2$locality, sep = ", ")
tran2$paonstreetlo <- stri_replace_all_fixed(tran2$paonstreetlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba30 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 31 paonstreetnlo = ADD12C#################
tran2 <- anti_join_original_variables(tran2, taba30)

tran2$saonn <- stri_replace_all_fixed(tran2$saon, "/", "")
tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn <- stri_replace_all_fixed(tran2$street, "'", "")
tran2$localityn <- stri_replace_all_regex(tran2$locality, "['.]", "")
tran2$paonstreetn <- stri_paste(tran2$paonn, tran2$streetn, sep = ", ")
tran2$paonstreetnlo <- stri_paste(tran2$paonstreetn, tran2$localityn, sep = ", ")
tran2$paonstreetnlo <- stri_replace_all_fixed(tran2$paonstreetnlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba31 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetnlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 32 paonstreetlo1 = ADDRE#################
tran2 <- anti_join_original_variables(tran2, taba31)

tran2$paonstreet1 <- stri_paste(tran2$paon, tran2$street, sep = " ")
tran2$paonstreetlo1 <- stri_paste(tran2$paonstreet1, tran2$locality, sep = ", ")
tran2$paonstreetlo1 <- stri_replace_all_fixed(tran2$paonstreetlo1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba32 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 33 paonstreetnlo1 = ADDC#################
tran2 <- anti_join_original_variables(tran2, taba32)

tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn <- stri_replace_all_fixed(tran2$street, "'", "")
tran2$localityn <- stri_replace_all_regex(tran2$locality, "['.]", "")
tran2$paonstreetn1 <- stri_paste(tran2$paonn, tran2$streetn, sep = " ")
tran2$paonstreetnlo1 <- stri_paste(tran2$paonstreetn1, tran2$localityn, sep = ", ")
tran2$paonstreetnlo1 <- stri_replace_all_fixed(tran2$paonstreetnlo1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba33 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetnlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 34 paonstreetlo1 = ADD12#################
tran2 <- anti_join_original_variables(tran2, taba33)

tran2$paonstreet1 <- stri_paste(tran2$paon, tran2$street, sep = " ")
tran2$paonstreetlo1 <- stri_paste(tran2$paonstreet1, tran2$locality, sep = ", ")
tran2$paonstreetlo1 <- stri_replace_all_fixed(tran2$paonstreetlo1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba34 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 35 paonstreetnlo1 = ADD12C#################
tran2 <- anti_join_original_variables(tran2, taba34)

tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn <- stri_replace_all_fixed(tran2$street, "'", "")
tran2$localityn <- stri_replace_all_regex(tran2$locality, "['.]", "")
tran2$paonstreetn1 <- stri_paste(tran2$paonn, tran2$streetn, sep = " ")
tran2$paonstreetnlo1 <- stri_paste(tran2$paonstreetn1, tran2$localityn, sep = ", ")
tran2$paonstreetnlo1 <- stri_replace_all_fixed(tran2$paonstreetnlo1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba35 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetnlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 36 paonstreetlo2== ADD12C2#################
tran2 <- anti_join_original_variables(tran2, taba35)

tran2$paonstreet1 <- stri_paste(tran2$paon, tran2$street, sep = " ")
tran2$paonstreetlo2 <- stri_paste(tran2$paonstreet1, tran2$locality, sep = " ")
tran2$paonstreetlo2 <- stri_replace_all_regex(tran2$paonstreetlo2, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba36 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 37 paonstreetlo2= ADDREC################
tran2 <- anti_join_original_variables(tran2, taba36)

tran2$paonstreet1 <- stri_paste(tran2$paon, tran2$street, sep = " ")
tran2$paonstreetlo2 <- stri_paste(tran2$paonstreet1, tran2$locality, sep = " ")
tran2$paonstreetlo2 <- stri_replace_all_regex(tran2$paonstreetlo2, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba37 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 38 paonstreetn=ADD12C3#################
tran2 <- anti_join_original_variables(tran2, taba37)

tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn <- stri_replace_all_fixed(tran2$street, "'", "")
tran2$paonstreetn <- stri_paste(tran2$paonn, tran2$streetn, sep = ", ")
tran2$paonstreetn <- stri_replace_all_fixed(tran2$paonstreetn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[-'./ ]", "")

taba38 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 39 for the street is null, paonn3 =ADD1CC#################
tran2 <- anti_join_original_variables(tran2, taba38)

tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")

tran21 <- tran2[tran2$street == "", ]
tran21$paonn3 <- stri_replace_all_regex(tran21$paonn, "[- ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[-'./ ]", "")

taba39 <-
  inner_join(
    tran21,
    epc,
    by = c("postcode" = "postcode", "paonn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran21)

###############matching rule 40  paon66=ADD1CC#################
tran2 <- anti_join_original_variables(tran2, taba39)

tran22 <- tran2[stri_detect_fixed(tran2$paon, ","), ]

tran22$paon61 <- beg2char(tran22$paon, ",")
tran22$paon62 <- char2end(tran22$paon, ",")
tran22$paon66 <- stri_paste(tran22$paon62, tran22$paon61, sep = ", ")
tran22$paon66 <- stri_replace_all_fixed(tran22$paon66, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[-'./ ]", "")

taba40 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paon66" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran22)

tran2 <- anti_join_original_variables(tran2, taba40)

######combine the results of matching rules 28 to 40########

# Create a list
taba_list <- mget(ls(pattern = "^taba"))

#casa2 is the linked result through stage 2
casa2 <- reduce(taba_list, union)

# save casa2 in duckDB and name as casabin2
dbWriteTable(con, "casabin2", casa2, overwrite = TRUE, row.names = FALSE)

# Remove all the matching results
rm(list = ls(pattern = "^taba"))

# # Save the unnmatched tran2 in duckDB as tran21bin
dbWriteTable(con, "tran21bin", tran2, overwrite = TRUE, row.names = FALSE)

# ------------------stage 3------------------------------
###############prepare for stage 3 #############

# Select the transactions for which the property type is not Flats/Maisonettes
tran22 <- tran2[tran2$propertytype != "F", ]

# Select the transaction for which the property type is Flats/Maisonettes
tran24 <- tran2[tran2$propertytype == "F", ]

# Save tran22 and tran24 in duckDB
dbWriteTable(con, "tranbin22", tran22, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "tranbin24", tran24, overwrite = TRUE, row.names = FALSE)

# Tidy up
rm(tran2, tran24)

epc <- epc[epc$postcode %in% unique(tran22$postcode), ]

## Stage 3a transactions with null saon for Detached, Semi-Detached and Terraced

###############matching rule 41 paon65streetlo=ADDRE#########
tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = ", ")
tran221$paon65streetlo <- stri_paste(tran221$paon65street, tran221$locality, sep = ", ")
tran221$paon65streetlo <- stri_replace_all_fixed(tran221$paon65streetlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba41 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 42 paon65streetlo=ADD12#################
tran22 <- anti_join_original_variables(tran22, taba41)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = ", ")
tran221$paon65streetlo <- stri_paste(tran221$paon65street, tran221$locality, sep = ", ")
tran221$paon65streetlo <- stri_replace_all_fixed(tran221$paon65streetlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba42 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 43 paon65streetnlo=ADDCC#################
tran22 <- anti_join_original_variables(tran22, taba42)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$saonn <- stri_replace_all_fixed(tran221$saon, "/", "")
tran221$paonn <- stri_replace_all_regex(tran221$paon, "['.]", "")
tran221$streetn <- stri_replace_all_fixed(tran221$street, "'", "")
tran221$localityn <- stri_replace_all_regex(tran221$locality, "['.]", "")

tran221$paon65n <- word(tran221$paonn, -1)
tran221$paon65street <- stri_paste(tran221$paon65n, tran221$streetn, sep = ", ")
tran221$paon65streetnlo <- stri_paste(tran221$paon65street, tran221$localityn, sep = ", ")
tran221$paon65streetnlo <- stri_replace_all_fixed(tran221$paon65streetnlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba43 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetnlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 44 paon65streetlo1=ADDREC#################
tran22 <- anti_join_original_variables(tran22, taba43)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = " ")
tran221$paon65streetlo1 <- stri_paste(tran221$paon65street, tran221$locality, sep = " ")
tran221$paon65streetlo1 <- stri_replace_all_regex(tran221$paon65streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba44 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 45 paon61streetlo=ADDC#################
tran22 <- anti_join_original_variables(tran22, taba44)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = ", ")
tran221$paon61streetlo <- stri_paste(tran221$paon61street1, tran221$locality, sep = ", ")
tran221$paon61streetlo <- stri_replace_all_fixed(tran221$paon61streetlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba45 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 46 paon61streetlo1=ADDREC#################
tran22 <- anti_join_original_variables(tran22, taba45)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba46 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 47 paon61streetlo1=ADDC3#################
tran22 <- anti_join_original_variables(tran22, taba46)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba47 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 48 paon61streetlo1=ADD12C1#################
tran22 <- anti_join_original_variables(tran22, taba47)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba48 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 49 paon61lo=ADD12C#################
tran22 <- anti_join_original_variables(tran22, taba48)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61lo <- stri_paste(tran221$paon61, tran221$locality, sep = ", ")
tran221$paon61lo <- stri_replace_all_fixed(tran221$paon61lo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba49 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61lo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 50 paon61street= ADD12C1#################
tran22 <- anti_join_original_variables(tran22, taba49)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61street <- stri_replace_all_regex(tran221$paon61street, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba50 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 51 paon61street= ADD13C1#################
tran22 <- anti_join_original_variables(tran22, taba50)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61street <- stri_replace_all_regex(tran221$paon61street, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba51 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 52 paon65street = ADDC3#################
tran22 <- anti_join_original_variables(tran22, taba51)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = " ")
tran221$paon65street <- stri_replace_all_regex(tran221$paon65street, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba52 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 53 paon65street= ADD1C2#################
tran22 <- anti_join_original_variables(tran22, taba52)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = " ")
tran221$paon65street <- stri_replace_all_regex(tran221$paon65street, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./, ]", "")

taba53 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 54 paon66streetlo=ADDCCC#################
tran22 <- anti_join_original_variables(tran22, taba53)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221$paon66 <- stri_paste(tran221$paon62, tran221$paon61, sep = " ")
tran221$paon66street <- stri_paste(tran221$paon66, tran221$street, sep = " ")
tran221$paon66streetlo <- stri_paste(tran221$paon66street, tran221$locality, sep = " ")
tran221$paon66streetlo <- stri_replace_all_regex(tran221$paon66streetlo, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[-'./, ]", "") # have added removal of spaces to align to tran; matches 14 versus 0 previously

taba54 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon66streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 55 paon66streetlo =ADD12C3#################
tran22 <- anti_join_original_variables(tran22, taba54)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221$paon66 <- stri_paste(tran221$paon62, tran221$paon61, sep = " ")
tran221$paon66street <- stri_paste(tran221$paon66, tran221$street, sep = " ")
tran221$paon66streetlo <- stri_paste(tran221$paon66street, tran221$locality, sep = " ")
tran221$paon66streetlo <- stri_replace_all_regex(tran221$paon66streetlo, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[-'./, ]", "")

taba55 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon66streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 56  paon65streetlo1 =ADD23C1#################
tran22 <- anti_join_original_variables(tran22, taba55)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon65 <- word(tran221$paon, -1)
tran221$paon65street <- stri_paste(tran221$paon65, tran221$street, sep = " ")
tran221$paon65streetlo <- stri_paste(tran221$paon65street, tran221$locality, sep = " ")
tran221$paon65streetlo <- stri_replace_all_regex(tran221$paon65streetlo, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add2, epc$add3, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba56 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 57 paon61new=ADD1#################
tran22 <- anti_join_original_variables(tran22, taba56)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]
tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61new <- stri_paste("THE", tran221$paon61, sep = " ")

taba57 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61new" = "add1"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 58 paonstreetlo3= ADD12new#################
tran22 <- anti_join_original_variables(tran22, taba57)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreetlo3 <- stri_paste(tran22$paonstreet, tran22$locality, sep = ", ")
tran22$paonstreetlo3 <- stri_replace_all_regex(tran22$paonstreetlo3, "[, ]", "")

epc$add2new <- stri_replace_all_fixed(epc$add2, "-", "")
epc$addressfinal <- stri_paste(epc$add1, epc$add2new, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba58 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 59  paonstreetlo3= ADD13C1#################
tran22 <- anti_join_original_variables(tran22, taba58)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreetlo3 <- stri_paste(tran22$paonstreet, tran22$locality, sep = ", ")
tran22$paonstreetlo3 <- stri_replace_all_regex(tran22$paonstreetlo3, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba59 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 60 paonstreetlo3 = ADD13C2#################
tran22 <- anti_join_original_variables(tran22, taba59)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreetlo3 <- stri_paste(tran22$paonstreet, tran22$locality, sep = ", ")
tran22$paonstreetlo3 <- stri_replace_all_regex(tran22$paonstreetlo3, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba60 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetlo3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 61  paonstreet=ADD1C3#################
tran22 <- anti_join_original_variables(tran22, taba60)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[, ]", "")

taba61 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 62 PAON=ADD1#################
tran22 <- anti_join_original_variables(tran22, taba61)

taba62 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paon" = "add1"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 63  paonstreetlo3 =ADD662#################
tran22 <- anti_join_original_variables(tran22, taba62)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreetlo3 <- stri_paste(tran22$paonstreet, tran22$locality, sep = ", ")
tran22$paonstreetlo3 <- stri_replace_all_regex(tran22$paonstreetlo3, "[, ]", "")

epc1 <- epc[stri_detect_fixed(epc$add1, ","), ]
epc1$add161 <- beg2char(epc1$add1, ",")
epc1$add162 <- char2end(epc1$add1, ",")

epc2 <- epc1[stri_detect_fixed(epc1$add162, "-"), ]
epc2$add162 <- stri_replace_all_fixed(epc2$add162, "-", "")
epc2$add66 <- stri_paste(epc2$add161, epc2$add162, sep = ", ")
epc2$addressfinal <- stri_paste(epc2$add66, epc2$add2, sep = ", ")
epc2$addressfinal <- stri_replace_all_regex(epc2$addressfinal, "[, ]", "")

taba63 <-
  inner_join(
    tran22,
    epc2,
    by = c("postcode" = "postcode", "paonstreetlo3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(epc1)
rm(epc2)

###############matching rule 64 paonstreet= ADD67#################
tran22 <- anti_join_original_variables(tran22, taba63)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc1 <- epc[stri_detect_fixed(epc$add1, ","), ]

epc1$add161 <- beg2char(epc1$add1, ",")
epc1$add165 <- char2end(epc1$add1, ",")

epc2 <- epc1[stri_detect_fixed(epc1$add165, "."), ]

epc2$add165 <- stri_replace_all_fixed(epc2$add165, ".", "")
epc2$add67 <- stri_paste(epc2$add161, epc2$add165, sep = ", ")
epc2$addressfinal <- stri_replace_all_regex(epc2$add67, "[, ]", "")

taba64 <-
  inner_join(
    tran22,
    epc2,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(epc1)
rm(epc2)

###############matching rule 65  paonstreet= ADDSP12#################
tran22 <- anti_join_original_variables(tran22, taba64)

tran231 <- tran22[tran22$street != "", ]
tran231$paonstreet <- stri_paste(tran231$paon, tran231$street, sep = ", ")
tran231$paonstreet <- stri_replace_all_regex(tran231$paonstreet, "[, ]", "")

epc$add1sp <- beg2char(epc$add1, " ")
epc$addsp12 <- stri_paste(epc$add1sp, epc$add2, sep = ", ")

epc1 <- epc[!stri_detect_regex(epc$add2, "^\\d"), ]
epc1 <- epc1[!stri_detect_regex(epc1$add1, "\\s\\D(\\s|[,]|>)"), ]
epc1$addressfinal <- stri_replace_all_regex(epc1$addsp12, "[, ]", "")

taba65 <-
  inner_join(
    tran231,
    epc1,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)

rm(tran231)
rm(epc1)

###############matching rule 66 paonstreetn1=ADD1C4#################
tran22 <- anti_join_original_variables(tran22, taba65)

tran22$streetn1 <- stri_replace_all_regex(tran22$street, "[-'.]", "")
tran22$paonstreetn1 <- stri_paste(tran22$paon, tran22$streetn1, sep = ", ")
tran22$paonstreetn1 <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[', ]", "")

taba66 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 67 paonstreet=ADDU#################
tran22 <- anti_join_original_variables(tran22, taba66)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, "UNIT ", "")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba67 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)

###############matching rule 68 paonstreet1=ADD68   #################
tran22 <- anti_join_original_variables(tran22, taba67)

tran22$paonstreet1 <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet1 <- stri_replace_all_fixed(tran22$paonstreet1, " ", "")

epc$add162 <- char2end(epc$add1, ",")
epc$add63 <- stri_replace_all_fixed(epc$add162, "-", "")
epc$add63 <- stri_replace_all_fixed(epc$add63, ".", "")
epc$add161 <- beg2char(epc$add1, ",")

epc$addressfinal <- stri_paste(epc$add161, epc$add63, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[' ]", "")

taba68 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 69  paonstreet1=ADD69#################
tran22 <- anti_join_original_variables(tran22, taba68)

tran22$paonstreet1 <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet1 <- stri_replace_all_fixed(tran22$paonstreet1, " ", "")

epc$add1nn <- stri_replace_all_fixed(epc$add1, "NO ", "")
epc$add1nn <- stri_replace_all_fixed(epc$add1nn, ",", "")
epc$addressfinal <- stri_paste(epc$add1nn, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba69 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############prepare for address string in EPCs, written different in Land Registry PPD#################
tran22 <- anti_join_original_variables(tran22, taba69)

rules <- read.csv("Data/rulechi.csv")
rules$add1 <- stri_trim_both(rules$add1)
rules$street <- stri_trim_both(rules$street)
rules$postcode <- stri_trim_both(rules$postcode)

tran25s <- tran22[tran22$postcode %in% unique(rules$postcode), ]

epc1 <- epc[epc$postcode %in% unique(rules$postcode), ]

### create back the right address string

i <- dim(rules)[1]

pc <- 0
for (pc in 1:i) {
  print(pc)
  print(i)

  pp <- rules[pc, "postcode"]
  data1 <- rules[rules$postcode == pp, ]
  epc1[epc1$postcode == pp, "add1"] <- gsub(
    data1[1, 2],
    data1[1, 3],
    epc1[epc1$postcode == pp, "add1"]
  )
  pc <- pc + 1
}

###############matching rule 70 paonstreet1 =ADD1C5################
tran25s$paonstreet1 <- stri_paste(tran25s$paon, tran25s$street, sep = ", ")
tran25s$paonstreet1 <- stri_replace_all_fixed(tran25s$paonstreet1, " ", "")

epc1$addressfinal <- stri_replace_all_regex(epc1$add1, "[. ]", "")

taba70 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 71 paonstreet2=ADD1C5#################
tran22 <- anti_join_original_variables(tran22, taba70)
tran25s <- anti_join_original_variables(tran25s, taba70)

tran25s$paonstreet2 <- stri_paste(tran25s$paon, tran25s$street, sep = " ")
tran25s$paonstreet2 <- stri_replace_all_fixed(tran25s$paonstreet2, " ", "")

epc1$addressfinal <- stri_replace_all_regex(epc1$add1, "[. ]", "")

taba71 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 72 paonn2=ADD1C6#################
tran22 <- anti_join_original_variables(tran22, taba71)
tran25s <- anti_join_original_variables(tran25s, taba71)

tran25s$paonn2 <- stri_replace_all_regex(tran25s$paon, "[, ]", "")

epc1$addressfinal <- stri_replace_all_fixed(epc1$add1, "UNIT ", "")
epc1$addressfinal <- stri_replace_all_regex(epc1$addressfinal, "[, ]", "")

taba72 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)

###############matching rule 73 paonstreet3=ADDCCC #################
tran22 <- anti_join_original_variables(tran22, taba72)

rm(tran25s)
rm(epc1)
rm(rules)

tran22$paonstreet3 <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet3 <- stri_replace_all_regex(tran22$paonstreet3, "[, ]", "")

epcq <- epc[!stri_detect_regex(epc$add, "\\d+-\\d+"), ]
epcq$addressfinal <- stri_replace_all_regex(epcq$add, "[-'./, ]", "")

taba11111 <-
  inner_join(
    tran22,
    epcq,
    by = c("postcode" = "postcode", "paonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(saon == "") |>
  select(transactionid, id)

tran22 <- anti_join_original_variables(tran22, taba11111)

rm(epcq)

###############combine the results of matching rules 41 to 73################

# Create a list
taba_list <- mget(ls(pattern = "^taba"))

# casa3 is the linked result from matching rules 41 to 73
casa3 <- reduce(taba_list, union)

# save casa3 in duckDB and named as casabin3
dbWriteTable(con, "casabin3", casa3, overwrite = TRUE, row.names = FALSE)

# Remove all the matching results
rm(list = ls(pattern = "^taba"))

# Save tran22 in duckDB
dbWriteTable(con, "tran22leftbin", tran22, overwrite = TRUE, row.names = FALSE)

# Tidy up
rm(tran22)
rm(i, pc, pp)

## Stage 3b transactions with null saon for Flats/Maisonettes

tran22 <- dbGetQuery(con, "select * from tranbin24")

epc <-
  dbGetQuery(
    con,
    "select add1, add2, add3, add, postcode, propertytype, id 
    from epc 
    where postcode in (select distinct postcode from tranbin24)"
  )

###############matching rule 74  paon62streetlo= ADDRE###############
tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221 <- tran221[!stri_detect_regex(tran221$paon61, "FLAT\\s"), ]
tran221 <- tran221[!stri_detect_fixed(tran221$paon61, "FLOOR"), ]

tran221$paon62street <- stri_paste(tran221$paon62, tran221$street, sep = ", ")
tran221$paon62streetlo <- stri_paste(tran221$paon62street, tran221$locality, sep = ", ")
tran221$paon62streetlo <- stri_replace_all_fixed(tran221$paon62streetlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba41 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon62streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 75  paon62streetlo=ADD12#################
tran22 <- anti_join_original_variables(tran22, taba41)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221 <- tran221[!stri_detect_regex(tran221$paon61, "FLAT\\s"), ]
tran221 <- tran221[!stri_detect_fixed(tran221$paon61, "FLOOR"), ]
tran221 <- tran221[!stri_detect_regex(tran221$paon61, "\\d"), ]

tran221$paon62street <- stri_paste(tran221$paon62, tran221$street, sep = ", ")
tran221$paon62streetlo <- stri_paste(tran221$paon62street, tran221$locality, sep = ", ")
tran221$paon62streetlo <- stri_replace_all_fixed(tran221$paon62streetlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba42 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon62streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 76  paon65streetnlo=ADDCC#################
tran22 <- anti_join_original_variables(tran22, taba42)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$saonn <- stri_replace_all_fixed(tran221$saon, "/", "")
tran221$paonn <- stri_replace_all_regex(tran221$paon, "['.]", "")
tran221$streetn <- stri_replace_all_fixed(tran221$street, "'", "")
tran221$localityn <- stri_replace_all_regex(tran221$locality, "['.]", "")

tran221$paon65n <- word(tran221$paonn, -1)
tran221$paon61 <- beg2char(tran221$paonn, ",")

tran221 <- tran221[!stri_detect_regex(tran221$paon61, "FLAT\\s"), ]
tran221 <- tran221[!stri_detect_fixed(tran221$paon61, "FLOOR"), ]
tran221 <- tran221[!stri_detect_regex(tran221$paon61, "\\d"), ]

tran221$paon65street <- stri_paste(tran221$paon65n, tran221$streetn, sep = ", ")
tran221$paon65streetnlo <- stri_paste(tran221$paon65street, tran221$localityn, sep = ", ")
tran221$paon65streetnlo <- stri_replace_all_fixed(tran221$paon65streetnlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba43 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon65streetnlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 77 paon62streetlo1=ADDREC#################
tran22 <- anti_join_original_variables(tran22, taba43)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221 <- tran221[!stri_detect_regex(tran221$paon61, "FLAT\\s"), ]
tran221 <- tran221[!stri_detect_fixed(tran221$paon61, "FLOOR"), ]
tran221 <- tran221[!stri_detect_regex(tran221$paon61, "\\d"), ]

tran221$paon62street <- stri_paste(tran221$paon62, tran221$street, sep = " ")
tran221$paon62streetlo1 <- stri_paste(tran221$paon62street, tran221$locality, sep = " ")
tran221$paon62streetlo1 <- stri_replace_all_regex(tran221$paon62streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba44 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon62streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 78 paon61streetlo =ADDC #################
tran22 <- anti_join_original_variables(tran22, taba44)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = ", ")
tran221$paon61streetlo <- stri_paste(tran221$paon61street1, tran221$locality, sep = ", ")
tran221$paon61streetlo <- stri_replace_all_fixed(tran221$paon61streetlo, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba45 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 79 paon61streetlo1=ADDREC#################
tran22 <- anti_join_original_variables(tran22, taba45)
tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba46 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 80 paon61streetlo1=ADDC3#################
tran22 <- anti_join_original_variables(tran22, taba46)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba47 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 81 paon61streetlo1=ADD12C1#################
tran22 <- anti_join_original_variables(tran22, taba47)
tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street1 <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61streetlo1 <- stri_paste(tran221$paon61street1, tran221$locality, sep = " ")
tran221$paon61streetlo1 <- stri_replace_all_regex(tran221$paon61streetlo1, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba48 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61streetlo1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 82 paon61street= ADD13C1#################
tran22 <- anti_join_original_variables(tran22, taba48)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon61street <- stri_paste(tran221$paon61, tran221$street, sep = " ")
tran221$paon61street <- stri_replace_all_regex(tran221$paon61street, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba51 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 83 paon66streetlo= ADDCCC#################
tran22 <- anti_join_original_variables(tran22, taba51)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221$paon66 <- stri_paste(tran221$paon62, tran221$paon61, sep = " ")
tran221$paon66street <- stri_paste(tran221$paon66, tran221$street, sep = " ")
tran221$paon66streetlo <- stri_paste(tran221$paon66street, tran221$locality, sep = " ")
tran221$paon66streetlo <- stri_replace_all_regex(tran221$paon66streetlo, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./, ]", "")

taba52 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon66streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 84 paon66streetlo =ADD12C3#################
tran22 <- anti_join_original_variables(tran22, taba52)

tran221 <- tran22[stri_detect_fixed(tran22$paon, ","), ]

tran221$paon61 <- beg2char(tran221$paon, ",")
tran221$paon62 <- char2end(tran221$paon, ",")

tran221$paon66 <- stri_paste(tran221$paon62, tran221$paon61, sep = " ")
tran221$paon66street <- stri_paste(tran221$paon66, tran221$street, sep = " ")
tran221$paon66streetlo <- stri_paste(tran221$paon66street, tran221$locality, sep = " ")
tran221$paon66streetlo <- stri_replace_all_regex(tran221$paon66streetlo, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[-'./ ]", "")

taba53 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paon66streetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 85 paonfstreet=ADDRE#################
tran22 <- anti_join_original_variables(tran22, taba53)

tran22$paonflat <- stri_paste("FLAT", tran22$paon, sep = " ")
tran22$paonfstreet <- stri_paste(tran22$paonflat, tran22$street, sep = ", ")
tran22$paonfstreet <- stri_replace_all_fixed(tran22$paonfstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba54 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonfstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 86 paonfstreet= ADD12#################
tran22 <- anti_join_original_variables(tran22, taba54)

tran22$paonflat <- stri_paste("FLAT ", tran22$paon, sep = "")
tran22$paonfstreet <- stri_paste(tran22$paonflat, tran22$street, sep = ", ")
tran22$paonfstreet <- stri_replace_all_fixed(tran22$paonfstreet, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba55 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonfstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 87 paonapstreet =ADDRE#################
tran22 <- anti_join_original_variables(tran22, taba55)

tran22$paonap <- stri_paste("APARTMENT", tran22$paon, sep = " ")
tran22$paonapstreet <- stri_paste(tran22$paonap, tran22$street, sep = ", ")
tran22$paonapstreet <- stri_replace_all_fixed(tran22$paonapstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba56 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonapstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 88 paonfstreet1= ADDRE#################
tran22 <- anti_join_original_variables(tran22, taba56)

tran22$paonflat <- stri_paste("FLAT", tran22$paon, sep = " ")
tran22$paonfstreet1 <- stri_paste(tran22$paonflat, tran22$street, sep = " ")
tran22$paonfstreet1 <- stri_replace_all_fixed(tran22$paonfstreet1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba57 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonfstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 89 paonstreet= ADD1C7#################
tran22 <- anti_join_original_variables(tran22, taba57)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[, ]", "")

taba58 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 90 paonstreetn1= ADD1C7#################
tran22 <- anti_join_original_variables(tran22, taba58)

tran22$streetn1 <- stri_replace_all_regex(tran22$street, "[-'.]", "")

tran22$paonstreetn1 <- stri_paste(tran22$paon, tran22$streetn1, sep = ", ")
tran22$paonstreetn1 <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[, ]", "")

taba59 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 91 paonstreetn1= ADD1C8 #################
tran22 <- anti_join_original_variables(tran22, taba59)

tran22$streetn1 <- stri_replace_all_regex(tran22$street, "[-'.]", "")
tran22$paonstreet <- stri_paste(tran22$paon, tran22$streetn1, sep = ", ")
tran22$paonstreetn1 <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[., ]", "")

taba60 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############for the address string written different in EPCs #################
tran22 <- anti_join_original_variables(tran22, taba60)

rules <- read.csv("Data/rulechi.csv")
rules$add1 <- stri_trim_both(rules$add1)
rules$street <- stri_trim_both(rules$street)

tran25s <- tran22[tran22$postcode %in% unique(rules$postcode), ]

epc1 <- epc[epc$postcode %in% unique(rules$postcode), ]
### create back the right address string
i <- dim(rules)[1]

pc <- 0
for (pc in 1:i) {
  print(pc)
  print(i)

  pp <- rules[pc, "postcode"]
  data1 <- rules[rules$postcode == pp, ]
  epc1[epc1$postcode == pp, "add1"] <- gsub(
    data1[1, 2],
    data1[1, 3],
    epc1[epc1$postcode == pp, "add1"]
  )
  pc <- pc + 1
}
###############matching rule 92 paonstreet1=ADD1C5################
tran25s$paonstreet1 <- stri_paste(tran25s$paon, tran25s$street, sep = ", ")
tran25s$paonstreet1 <- stri_replace_all_fixed(tran25s$paonstreet1, " ", "")

epc1$addressfinal <- stri_replace_all_regex(epc1$add1, "[. ]", "")

taba61 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 93 paonstreet2=ADD1C5#################
tran22 <- anti_join_original_variables(tran22, taba61)
tran25s <- anti_join_original_variables(tran25s, taba61)

tran25s$paonstreet2 <- stri_paste(tran25s$paon, tran25s$street, sep = " ")
tran25s$paonstreet2 <- stri_replace_all_fixed(tran25s$paonstreet2, " ", "")

epc1$addressfinal <- stri_replace_all_regex(epc1$add1, "[. ]", "")

taba62 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 94 PAON=ADD1df1 SE5 7QS#################
tran22 <- anti_join_original_variables(tran22, taba62)
tran25s <- anti_join_original_variables(tran25s, taba62)

p <- "SE5 7QS"

cc1 <- epc1[
  epc1$postcode == p,
  c("postcode", "add", "add1", "add2", "add3", "propertytype", "id")
]
cc <- tran25s[
  tran25s$postcode == p,
  c(
    "transactionid",
    "postcode",
    "propertytype",
    "paon",
    "saon",
    "street",
    "locality"
  )
]

cc1$add11 <- stri_replace_all_fixed(cc1$add1, "FLAT ", "")

cc1$add12 <- word(cc1$add11, 1)
cc1$add12 <- stri_replace_all_fixed(cc1$add12, ",", "")
cc$add12 <- cc$paon

taba63 <-
  inner_join(
    cc,
    cc1,
    by = "add12",
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 95 paonn2=ADD1du#################
tran22 <- anti_join_original_variables(tran22, taba63)
tran25s <- anti_join_original_variables(tran25s, taba63)

tran25s$paonn2 <- stri_replace_all_regex(tran25s$paon, "[, ]", "")

epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G01", "G1")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G02", "G2")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G03", "G3")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G04", "G4")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G05", "G5")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G06", "G6")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G07", "G7")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G08", "G8")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "G09", "G9")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H01", "H1")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H02", "H2")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H03", "H3")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H04", "H4")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H05", "H5")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H06", "H6")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H07", "H7")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H08", "H8")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "H09", "H9")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I01", "I1")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I02", "I2")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I03", "I3")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I04", "I4")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I05", "I5")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I06", "I6")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I07", "I7")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I08", "I8")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "I09", "I9")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J01", "J1")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J02", "J2")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J03", "J3")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J04", "J4")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J05", "J5")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J06", "J6")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J07", "J7")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J08", "J8")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "J09", "J9")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k01", "k1")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k02", "k2")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k03", "k3")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k04", "k4")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k05", "k5")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k06", "k6")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k07", "k7")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k08", "k8")
epc1$add1 <- stri_replace_all_fixed(epc1$add1, "k09", "k9")

epc1$addressfinal <- stri_replace_all_fixed(epc1$add1, "UNIT ", "")
epc1$addressfinal <- stri_replace_all_regex(epc1$addressfinal, "[, ]", "")

taba64 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paonn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 96 paon61c=ADD1C9#################
tran22 <- anti_join_original_variables(tran22, taba64)
tran25s <- anti_join_original_variables(tran25s, taba64)

tran25s$paon61 <- beg2char(tran25s$paon, ",")
tran25s$paon61 <- stri_replace_all_fixed(tran25s$paon61, " ", "")

epc1$addressfinal <- stri_replace_all_fixed(epc1$add1, " ", "")

taba65 <-
  inner_join(
    tran25s,
    epc1,
    by = c("postcode" = "postcode", "paon61" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 97 paonfstreet1 =ADD1C9#################
tran22 <- anti_join_original_variables(tran22, taba65)
tran25s <- anti_join_original_variables(tran25s, taba65)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]
tran221$paonflat <- stri_paste("FLAT", tran221$paon, sep = " ")

tran221$paonfstreet1 <- stri_paste(tran221$paonflat, tran221$street, sep = " ")
tran221$paonfstreet1 <- stri_replace_all_fixed(tran221$paonfstreet1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add1, " ", "")

taba66 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paonfstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 98 paonfstreetn5=ADD1C #################
tran22 <- anti_join_original_variables(tran22, taba66)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]
tran221$paonflat <- stri_paste("FLAT", tran221$paon, sep = " ")
tran221$streetn5 <- stri_replace_all_regex(tran221$street, "['./]", "")
tran221$paonfstreetn5 <- stri_paste(tran221$paonflat, tran221$streetn5, sep = " ")
tran221$paonfstreetn5 <- stri_replace_all_fixed(tran221$paonfstreetn5, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./ ]", "")

taba67 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paonfstreetn5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 99  paonstreet3=ADD1632#################
tran22 <- anti_join_original_variables(tran22, taba67)

tran22$paonstreet3 <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet3 <- stri_replace_all_regex(tran22$paonstreet3, "[, ]", "")

epc$add163 <- beg2char(epc$add1, " ")
epc$addressfinal <- stri_paste(epc$add163, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba68 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 100 paonapstreet1=ADD12C2#################
tran22 <- anti_join_original_variables(tran22, taba68)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]

tran221$paonap <- stri_paste("APARTMENT", tran221$paon, sep = " ")
tran221$paonapstreet1 <- stri_paste(tran221$paonap, tran221$street, sep = " ")
tran221$paonapstreet1 <- stri_replace_all_fixed(tran221$paonapstreet1, " ", "")

epc1 <- epc[!stri_detect_regex(epc$add2, "^\\d+"), ]

epc1$addressfinal <- stri_paste(epc1$add1, epc1$add2, sep = " ")
epc1$addressfinal <- stri_replace_all_fixed(epc1$addressfinal, " ", "") # ADD12C2 - missing the removal of a comma here?

taba69 <-
  inner_join(
    tran221,
    epc1,
    by = c("postcode" = "postcode", "paonapstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)
rm(epc1)

###############matching rule 101 paonapstreetn5=ADD12C1#################
tran22 <- anti_join_original_variables(tran22, taba69)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]

tran221$paonap <- stri_paste("APARTMENT", tran221$paon, sep = " ")
tran221$streetn5 <- stri_replace_all_regex(tran221$street, "['./]", "")
tran221$paonapstreet5 <- stri_paste(tran221$paonap, tran221$streetn5, sep = " ")
tran221$paonapstreet5 <- stri_replace_all_fixed(tran221$paonapstreet5, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba70 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paonapstreet5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 102 paonn2 =ADDC3#################
tran22 <- anti_join_original_variables(tran22, taba70)

tran22$paonn2 <- stri_replace_all_regex(tran22$paon, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba71 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 103 paonstreet3 =flADD#################
tran22 <- anti_join_original_variables(tran22, taba71)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_paste("FLAT ", epc$add, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba72 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 104 paonn2=ADD2611#################
tran22 <- anti_join_original_variables(tran22, taba72)

tran22$paonn2 <- stri_replace_all_regex(tran22$paon, "[, ]", "")

epc1 <- epc[stri_detect_fixed(epc$add2, ","), ]

epc1$add261 <- beg2char(epc1$add2, ",")
epc1$add2166 <- stri_paste(epc1$add261, epc1$add1, sep = ", ")
epc1$addressfinal <- stri_replace_all_regex(epc1$add2166, "[, ]", "")

taba73 <-
  inner_join(
    tran22,
    epc1,
    by = c("postcode" = "postcode", "paonn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(epc1)

###############matching rule 105 paonstreet3=flADD13#################
tran22 <- anti_join_original_variables(tran22, taba73)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet3 <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$add1f <- stri_paste("FLAT", epc$add1, sep = " ")

epc$add31 <- stri_replace_all_regex(epc$add3, "['./]", "")
epc$addressfinal <- stri_paste(epc$add1f, epc$add31, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba74 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 106 paonstreet3=ADD13C2#################
tran22 <- anti_join_original_variables(tran22, taba74)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet3 <- stri_replace_all_regex(tran22$paonstreet, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba75 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 107 paonstreet4=ADDC3#################
tran22 <- anti_join_original_variables(tran22, taba75)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet4 <- stri_replace_all_fixed(tran22$paonstreet, "FLAT", "APARTMENT")
tran22$paonstreet4 <- stri_replace_all_regex(tran22$paonstreet4, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba76 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 108 paonfstreetn5=ADD1C2#################
tran22 <- anti_join_original_variables(tran22, taba76)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]

tran221$paonfl <- stri_paste("FLAT", tran221$paon, sep = " ")
tran221$streetn5 <- stri_replace_all_regex(tran221$street, "['./]", "")
tran221$paonfstreetn5 <- stri_paste(tran221$paonfl, tran221$streetn5, sep = " ")
tran221$paonfstreetn5 <- stri_replace_all_fixed(tran221$paonfstreetn5, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./, ]", "")

taba77 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "paonfstreetn5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 109 paonapstreet2=ADD12C2#################
tran22 <- anti_join_original_variables(tran22, taba77)

tran22$paonap <- stri_paste("APARTMENT", tran22$paon, sep = " ")

tran22$paonapstreet1 <- stri_paste(tran22$paonap, tran22$street, sep = " ")
tran22$paonapstreet2 <- stri_replace_all_regex(tran22$paonapstreet1, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba78 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonapstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 110 paonn2=ADD1C2#################
tran22 <- anti_join_original_variables(tran22, taba78)

tran22$paonn2 <- stri_replace_all_regex(tran22$paon, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./, ]", "")

taba79 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 111 paonf1streetn5=ADD12C#################
tran22 <- anti_join_original_variables(tran22, taba79)

tran22$paonfl1 <- stri_paste("FLAT,", tran22$paon, sep = " ")
tran22$streetn5 <- stri_replace_all_regex(tran22$street, "['./]", "")
tran22$paonf1streetn5 <- stri_paste(tran22$paonfl1, tran22$streetn5, sep = ", ")
tran22$paonf1streetn5 <- stri_replace_all_fixed(tran22$paonf1streetn5, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba80 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonf1streetn5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 112 paonfstreetn6=ADD12C#################
tran22 <- anti_join_original_variables(tran22, taba80)


tran22$paonfl <- stri_paste("FLAT", tran22$paon, sep = " ")
tran22$streetn5 <- stri_replace_all_regex(tran22$street, "['./]", "")
tran22$paonfstreetn6 <- stri_paste(tran22$paonfl, tran22$streetn5, sep = ", ")
tran22$paonfstreetn6 <- stri_replace_all_fixed(tran22$paonfstreetn6, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba81 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonfstreetn6" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 113 flpaon3streetn5=ADDC10#################
tran22 <- anti_join_original_variables(tran22, taba81)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+"), ]
tran221 <- tran221[!stri_detect_fixed(tran221$paon, "-"), ]

tran221$paonfl <- stri_paste("FLAT", tran221$paon, sep = " ")
tran221$streetn5 <- stri_replace_all_regex(tran221$street, "['./]", "")
tran221$flpaon3streetn5 <- stri_paste(tran221$paonfl, tran221$streetn5, sep = " ")
tran221$flpaon3streetn5 <- stri_replace_all_regex(tran221$flpaon3streetn5, "[- ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-,'./ ]", "")

taba82 <-
  inner_join(
    tran221,
    epc,
    by = c("postcode" = "postcode", "flpaon3streetn5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!stri_detect_regex(add, "\\d+,\\s\\d+")) |>
  select(transactionid, id)

rm(tran221)

###############matching rule 114 paonstreet1=ADD1C#################
tran22 <- anti_join_original_variables(tran22, taba82)

tran22$paonstreet <- stri_paste(tran22$paon, tran22$street, sep = ", ")
tran22$paonstreet1 <- stri_replace_all_fixed(tran22$paonstreet, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./ ]", "")

taba83 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 115 paonstreet2=ADD5#################
tran22 <- anti_join_original_variables(tran22, taba83)

tran221 <- tran22[stri_detect_regex(tran22$paon, "^\\d+[A-Z]"), ]

tran221$paonstreet2 <- stri_paste(tran221$paon, tran221$street, sep = " ")
tran221$paonstreet2 <- stri_replace_all_fixed(tran221$paonstreet2, " ", "")

epc1 <- epc[epc$postcode %in% unique(tran221$postcode), ]
epc2 <- epc1[stri_detect_regex(epc1$add1, "FLAT\\s[A-Z]$"), ]

epc2$add1ff <- stri_replace_all_fixed(epc2$add1, "FLAT ", "")
epc2$add263 <- beg2char(epc2$add2, " ")
epc2$add263 <- stri_replace_all_fixed(epc2$add263, ",", "")
epc2$add2611 <- stri_paste(epc2$add263, epc2$add1ff, sep = "")

epc2$add264 <- char2end(epc2$add2, " ")

epc2$addressfinal <- stri_paste(epc2$add2611, epc2$add264, sep = "")
epc2$addressfinal <- stri_replace_all_fixed(epc2$addressfinal, " ", "")

taba84 <-
  inner_join(
    tran221,
    epc2,
    by = c("postcode" = "postcode", "paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(epc1)
rm(epc2)
rm(tran221)

###############matching rule 116 paonstreet2=apADD1#################
tran22 <- anti_join_original_variables(tran22, taba84)

tran22$paonstreet2 <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet2 <- stri_replace_all_fixed(tran22$paonstreet2, " ", "")

epc$addressfinal <- stri_paste("APARTMENT", epc$add1, sep = " ")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-,'./ ]", "")

taba85 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 117 paonstreet2=ADD1C2#################
tran22 <- anti_join_original_variables(tran22, taba85)

tran22$paonstreet2 <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet2 <- stri_replace_all_fixed(tran22$paonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./, ]", "")

taba86 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 118 paonapstreet2=ADD13C2#################
tran22 <- anti_join_original_variables(tran22, taba86)

tran22$paonap <- stri_paste("APARTMENT", tran22$paon, sep = " ")

tran22$paonapstreet2 <- stri_paste(tran22$paonap, tran22$street, sep = " ")
tran22$paonapstreet2 <- stri_replace_all_regex(tran22$paonapstreet2, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba87 <-
  inner_join(
    tran22,
    epc,
    by = c("postcode" = "postcode", "paonapstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 119 paonstreet3=ADDr66#################
tran22 <- anti_join_original_variables(tran22, taba87)

tran22$paonstreet1 <- stri_paste(tran22$paon, tran22$street, sep = " ")
tran22$paonstreet3 <- stri_replace_all_regex(tran22$paonstreet1, "[, ]", "")

epc1 <- epc[stri_detect_fixed(epc$add, ","), ]

epc1$add61 <- beg2char(epc1$add, ",")
epc1$add62 <- char2end(epc1$add, ",")

epc1$add62 <- stri_replace_all_regex(epc1$add62, "[-'./]", "")
epc1$adda66 <- stri_paste(epc1$add61, epc1$add62, sep = ", ")
epc1$addressfinal <- stri_replace_all_regex(epc1$adda66, "[, ]", "")

taba88 <-
  inner_join(
    tran22,
    epc1,
    by = c("postcode" = "postcode", "paonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

tran22 <- anti_join_original_variables(tran22, taba88)

###############combine the results of matching rules 74 to 119################

# Create a list
taba_list <- mget(ls(pattern = "^taba"))

#casa4 is the linked result from matching rules 74 to 119
casa4 <- reduce(taba_list, union)

# save casa4 in duckDB and name as casabin4
dbWriteTable(con, "casabin4", casa4, overwrite = TRUE, row.names = FALSE)

# Remove all the matching results
rm(list = ls(pattern = "^taba"))

# Save the unmatched tran in duckDB as tran24binleft
dbWriteTable(con, "tran24binleft", tran22, overwrite = TRUE, row.names = FALSE)

rm(epc1)
rm(tran22)
rm(tran25s)
rm(rules, cc, cc1, data1, i, p, pc, pp)

# ------------------stage 4------------------------------
###############prepare for stage 4 #############

tran <- dbGetQuery(con, "select * from tranbin3")

epc <-
  dbGetQuery(
    con,
    "select add1, add2, add3, add, postcode, propertytype, id 
    from epc 
    where postcode in (select distinct postcode from tranbin3)"
  )

####### correct the unmatched address components in EPCs###
epc[epc$postcode == "AL7 3EQ", "add"] <- gsub(
  "SALVISBURG COURT",
  "SALVISBERG COURT",
  epc[epc$postcode == "AL7 3EQ", "add"]
)

epc[epc$postcode == "AL1 3GL", "add1"] <- gsub(
  "WHITELY COURT",
  "WHITLEY COURT",
  epc[epc$postcode == "AL1 3GL", "add1"]
)

epc[epc$postcode == "B1 1LR", "add"] <- gsub(
  "CONCORDE HOUSE",
  "CONCORD HOUSE",
  epc[epc$postcode == "B1 1LR", "add"]
)

epc[epc$postcode == "B38 8BH", "add"] <- gsub(
  "ST. NICOLAS GARDENS",
  "ST NICHOLAS GARDENS",
  epc[epc$postcode == "B38 8BH", "add"]
)

epc[epc$postcode == "B5 6AB", "add"] <- gsub(
  "BROSMGROVE STREET",
  "BROMSGROVE STREET",
  epc[epc$postcode == "B5 6AB", "add"]
)

epc[epc$postcode == "B16 8FT", "add"] <- gsub(
  "SHERBOURNE STREET",
  "SHERBORNE STREET",
  epc[epc$postcode == "B16 8FT", "add"]
)

epc[epc$postcode == "BB5 1NA", "add"] <- gsub(
  "ST. JAMES COURT",
  "ST JAMES COURT WEST",
  epc[epc$postcode == "BB5 1NA", "add"]
)

epc[epc$postcode == "B5 7SE", "add"] <- gsub(
  "THE BOULVARD",
  "THE BOULEVARD",
  epc[epc$postcode == "B5 7SE", "add"]
)

epc[epc$postcode == "B73 6AD", "add"] <- gsub(
  "KING EDWARD SQUARE",
  "KING EDWARDS SQUARE",
  epc[epc$postcode == "B73 6AD", "add"]
)

epc[epc$postcode == "BH12 1HS", "add"] <- gsub(
  "OVERBURY MANOR",
  "OVERBERRY MANOR",
  epc[epc$postcode == "BH12 1HS", "add"]
)

epc[epc$postcode == "BB3 0LB", "add"] <- gsub(
  "CARR HALL COTTAGE",
  "CARR HALL COTTAGES",
  epc[epc$postcode == "BB3 0LB", "add"]
)

epc[epc$postcode == "BS2 0EH", "add"] <- gsub(
  "THE OLD DRILL HALL",
  "THE DRILL HALL",
  epc[epc$postcode == "BS2 0EH", "add"]
)

epc[epc$postcode == "BS5 7HU", "add"] <- gsub(
  "FROST & REED HOUSE",
  "FROST AND REED HOUSE",
  epc[epc$postcode == "BS5 7HU", "add"]
)

epc[epc$postcode == "BA1 6AX", "add"] <- gsub(
  "GROVENOR PLACE",
  "GROSVENOR PLACE",
  epc[epc$postcode == "BA1 6AX", "add"]
)

epc[epc$postcode == "CM12 0BQ", "add"] <- gsub(
  "LANGTHORNS",
  "LANGTHORNES",
  epc[epc$postcode == "CM12 0BQ", "add"]
)

epc[epc$postcode == "CB8 8AB", "add"] <- gsub(
  "THE GALLOPS",
  "NORTH LODGE",
  epc[epc$postcode == "CB8 8AB", "add"]
)

epc[epc$postcode == "CO14 8JZ", "add"] <- gsub(
  "NAZE APARTMENTS",
  "NAZE REACH",
  epc[epc$postcode == "CO14 8JZ", "add"]
)

epc[epc$postcode == "CV1 5PE", "add"] <- gsub(
  "CAISTOR HALL",
  "CAISTER HALL",
  epc[epc$postcode == "CV1 5PE", "add"]
)

epc[epc$postcode == "CW12 1QQ", "add"] <- gsub(
  "EDWARDS MILL",
  "EDWARD MILL",
  epc[epc$postcode == "CW12 1QQ", "add"]
)

epc[epc$postcode == "CO11 1AL", "add1"] <- gsub(
  "NO 1 THE MALTINGS",
  "1 THE MALTINGS",
  epc[epc$postcode == "CO11 1AL", "add1"]
)

epc[epc$postcode == "CO11 1AL", "add"] <- gsub(
  "NO 1 THE MALTINGS",
  "1 THE MALTINGS",
  epc[epc$postcode == "CO11 1AL", "add"]
)

epc[epc$postcode == "CO2 8TN", "add"] <- gsub(
  "CAROLYNE COURT",
  "CAROLYN COURT",
  epc[epc$postcode == "CO2 8TN", "add"]
)

epc[epc$postcode == "CT11 9NH", "add"] <- gsub(
  "CANONBURY ROAD",
  "CANNONBURY ROAD",
  epc[epc$postcode == "CT11 9NH", "add"]
)

epc[epc$postcode == "CA11 0XB", "add"] <- gsub(
  "WHITBARROW VILLAGE",
  "WHITBARROW",
  epc[epc$postcode == "CA11 0XB", "add"]
)
epc[epc$postcode == "CA11 0XB", "add"] <- gsub(
  "KIRKSTONE COTTAGES",
  "KIRKSTONE COTTAGE",
  epc[epc$postcode == "CA11 0XB", "add"]
)
##more complex

epc[epc$postcode == "DA11 0BX", "add"] <- gsub(
  "MELBOURNE QUAYS",
  "MELBOURNE QUAY",
  epc[epc$postcode == "DA11 0BX", "add"]
)

epc[epc$postcode == "DN34 4ST", "add"] <- gsub(
  "WESTLANDS,",
  "WESTLANDS HOUSE,",
  epc[epc$postcode == "DN34 4ST", "add"]
)

epc[epc$postcode == "FY4 4FN", "add"] <- gsub(
  "HAWESIDE LANE",
  "HAWES SIDE LANE",
  epc[epc$postcode == "FY4 4FN", "add"]
)

epc[epc$postcode == "IG1 2FJ", "add"] <- gsub(
  "ICON BUILDINGS",
  "ICON BUILDING",
  epc[epc$postcode == "IG1 2FJ", "add"]
)

epc[epc$postcode == "GL54 1EJ", "add"] <- gsub(
  "NEWLANDS",
  "NEWLANDS HOUSE",
  epc[epc$postcode == "GL54 1EJ", "add"]
)
epc[epc$postcode == "GL54 1EJ", "add2"] <- gsub(
  "NEWLANDS",
  "NEWLANDS HOUSE",
  epc[epc$postcode == "GL54 1EJ", "add2"]
)
#half and half need keep, ap to flat
epc[epc$postcode == "HD7 5UN", "add1"] <- gsub(
  "TITANIC MILLS",
  "TITANIC MILL",
  epc[epc$postcode == "HD7 5UN", "add1"]
)

epc[epc$postcode == "HR8 1BY", "add"] <- gsub(
  "LEADON BANK CENTRE",
  "LEADON BANK",
  epc[epc$postcode == "HR8 1BY", "add"]
)

epc[epc$postcode == "HU15 2QW", "add2"] <- gsub(
  "CLAYTON FOLD",
  "CLAYTONS FOLD",
  epc[epc$postcode == "HU15 2QW", "add2"]
)

epc[epc$postcode == "HA9 7HJ", "add"] <- gsub(
  "CRESWELL HOUSE",
  "CRESSWELL HOUSE",
  epc[epc$postcode == "HA9 7HJ", "add"]
)

epc[epc$postcode == "LE5 2LZ", "add"] <- gsub(
  "RAILWAY COTTAGES",
  "THE RAILWAY COTTAGES",
  epc[epc$postcode == "LE5 2LZ", "add"]
)

epc[epc$postcode == "LE5 2LZ", "add"] <- gsub(
  "THE THE RAILWAY COTTAGES",
  "THE RAILWAY COTTAGES",
  epc[epc$postcode == "LE5 2LZ", "add"]
)

epc[epc$postcode == "LS9 8BT", "add"] <- gsub(
  "ST. PETERS SQUARE",
  "ST PETER'S STREET",
  epc[epc$postcode == "LS9 8BT", "add"]
)
# half and half, 2019 write right
epc[epc$postcode == "L18 8BP", "add"] <- gsub(
  "BRUCKLAY HOUSE",
  "BRUCKLEY HOUSE",
  epc[epc$postcode == "L18 8BP", "add"]
)
#half right
epc[epc$postcode == "L8 7NZ", "add"] <- gsub(
  "FAULKNER SQUARE",
  "FALKNER SQUARE",
  epc[epc$postcode == "L8 7NZ", "add"]
)
#one wrong
epc[epc$postcode == "LA23 3GQ", "add"] <- gsub(
  "MARTINS COURT",
  "ST MARTINS COURT",
  epc[epc$postcode == "LA23 3GQ", "add"]
)

epc[epc$postcode == "LE2 3JB", "add"] <- gsub(
  "THE STONEY GATE GRANGE",
  "STONEYGATE GRANGE",
  epc[epc$postcode == "LE2 3JB", "add"]
)

epc[epc$postcode == "LE5 1BH", "add"] <- gsub(
  "KESTRAL LANE",
  "KESTREL LANE",
  epc[epc$postcode == "LE5 1BH", "add"]
)

epc[epc$postcode == "M3 7BY", "add"] <- gsub(
  "BLOCK 7",
  "BLOCK 7 SPECTRUM",
  epc[epc$postcode == "M3 7BY", "add"]
)

epc[epc$postcode == "M3 7BY", "add"] <- gsub(
  "BLOCK 7 SPECTRUM SPECTRUM",
  "BLOCK 7 SPECTRUM",
  epc[epc$postcode == "M3 7BY", "add"]
)

epc[epc$postcode == "M9 7HQ", "add"] <- gsub(
  "FRESHFIELD",
  "FRESHFIELDS",
  epc[epc$postcode == "M9 7HQ", "add"]
)

epc[epc$postcode == "N1 7SH", "add"] <- gsub(
  "ROYLE BUILDING",
  "ROYLE HOUSE",
  epc[epc$postcode == "N1 7SH", "add"]
)

epc[epc$postcode == "M3 7DZ", "add"] <- gsub(
  "BLOCK 9,",
  "BLOCK 9 SPECTRUM,",
  epc[epc$postcode == "M3 7DZ", "add"]
)

epc[epc$postcode == "M3 7EE", "add"] <- gsub(
  "BLOCK 11,",
  "BLOCK 11 SPECTRUM,",
  epc[epc$postcode == "M3 7EE", "add"]
)

epc[epc$postcode == "N20 0RX", "add"] <- gsub(
  "PARKWOOD FLATS,",
  "PARKWOOD,",
  epc[epc$postcode == "N20 0RX", "add"]
)

epc[epc$postcode == "N1 0GL", "add"] <- gsub(
  "KINGS QUARTER, 170",
  "KINGS QUARTER APARTMENTS, 170",
  epc[epc$postcode == "N1 0GL", "add"]
)

epc[epc$postcode == "NW9 5UF", "add"] <- gsub(
  "GABRIEL CLOSE",
  "GABRIEL COURT",
  epc[epc$postcode == "NW9 5UF", "add"]
)

epc[epc$postcode == "HD3 4ZW", "add1"] <- gsub(
  "QUARRY BANK MILL",
  "QUARRY BANK",
  epc[epc$postcode == "HD3 4ZW", "add1"]
)

epc[epc$postcode == "OX16 5DR", "add"] <- gsub(
  "HOLLIES COURT,",
  "THE HOLLIES COURT,",
  epc[epc$postcode == "OX16 5DR", "add"]
)

epc[epc$postcode == "OX2 6AQ", "add"] <- gsub(
  "FOUNDRY HOUSE",
  "FOUNDRY HOUSE EAGLE WORKS",
  epc[epc$postcode == "OX2 6AQ", "add"]
)

epc[epc$postcode == "PO12 4BT", "add"] <- gsub(
  "IRWIN HEIGTS",
  "IRWIN HEIGHTS",
  epc[epc$postcode == "PO12 4BT", "add"]
)

epc[epc$postcode == "PE1 4YS", "add"] <- gsub(
  "CATHEDRAL GREEN COURT",
  "CATHEDRAL GREEN",
  epc[epc$postcode == "PE1 4YS", "add"]
)

epc[epc$postcode == "PO1 3SZ", "add"] <- gsub(
  "THE CRESCENT",
  "THE CRESCENT BUILDING",
  epc[epc$postcode == "PO1 3SZ", "add"]
)

epc[epc$postcode == "RG10 9GA", "add"] <- gsub(
  "OLD SILK MILL, ",
  "THE OLD SILK MILL,",
  epc[epc$postcode == "RG10 9GA", "add"]
)

epc[epc$postcode == "SE1 2YE", "add"] <- gsub(
  "BUTLERS WHARF,",
  "BUTLERS WHARF BUILDING,",
  epc[epc$postcode == "SE1 2YE", "add"]
)

epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KINIGHTSBRIDGE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIDGE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIDGE APPARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIGDE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIGE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTBRIDGE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIDGE APRTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "KNIGHTSBRIDGE APPARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIDGE APARTMENTS, 119",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "THE KNIGHTSBRIDGE, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  "KNIGHTSBRIDGE APARTMENTS, 199",
  ", 199",
  epc[epc$postcode == "SW7 1RH", "add"]
)
epc[epc$postcode == "SW7 1RH", "add"] <- gsub(
  ",",
  " ",
  epc[epc$postcode == "SW7 1RH", "add"]
)

epc[epc$postcode == "SE1 4PF", "add"] <- gsub(
  "MADISON APARTMENTS, 5-27",
  "THE MADISON, 5 - 27",
  epc[epc$postcode == "SE1 4PF", "add"]
)

epc[epc$postcode == "SW1V 3JL", "add"] <- gsub(
  "THE PANORAMIC, 152",
  ", 152",
  epc[epc$postcode == "SW1V 3JL", "add"]
)


epc[epc$postcode == "SW11 3YL", "add"] <- gsub(
  "IMONTEVETRO, 100",
  "MONTEVETRO BUILDING, 100",
  epc[epc$postcode == "SW11 3YL", "add"]
)

epc[epc$postcode == "S10 5DQ", "add"] <- gsub(
  "KING COURT,",
  "KINGS COURT,",
  epc[epc$postcode == "S10 5DQ", "add"]
)

epc[epc$postcode == "SW4 7EF", "add"] <- gsub(
  "GREENWAY APARTMENTS",
  "GREENAWAY APARTMENTS",
  epc[epc$postcode == "SW4 7EF", "add"]
)

epc[epc$postcode == "SL6 8DJ", "add"] <- gsub(
  "KESTRAL COURT",
  "KESTREL COURT",
  epc[epc$postcode == "SL6 8DJ", "add"]
)

epc[epc$postcode == "SW1X 0DH", "add"] <- gsub(
  "LENOX GARDENS",
  "LENNOX GARDENS",
  epc[epc$postcode == "SW1X 0DH", "add"]
)

epc[epc$postcode == "SK9 2AS", "add"] <- gsub(
  "OAK LAWNS,",
  "OAK LAWN,",
  epc[epc$postcode == "SK9 2AS", "add"]
)

epc[epc$postcode == "SE16 7BT", "add"] <- gsub(
  "FAIRMOUNT HOUSE,",
  "FAIRMONT HOUSE,",
  epc[epc$postcode == "SE16 7BT", "add"]
)

epc[epc$postcode == "SE8 4HN", "add"] <- gsub(
  "MILLBANK LANE,",
  "MILL LANE",
  epc[epc$postcode == "SE8 4HN", "add"]
)

epc[epc$postcode == "SE8 4HP", "add"] <- gsub(
  "MILLBANK LANE,",
  "MILL LANE",
  epc[epc$postcode == "SE8 4HP", "add"]
)

epc[epc$postcode == "SG4 9SB", "add"] <- gsub(
  "BALIOL CHAMBERS",
  "BALLIOL CHAMBERS",
  epc[epc$postcode == "SG4 9SB", "add"]
)

epc[epc$postcode == "SO41 0WR", "add"] <- gsub(
  "HOMEGRANGE,",
  "HOMEGRANGE HOUSE,",
  epc[epc$postcode == "SO41 0WR", "add"]
)

epc[epc$postcode == "SE16 3AD", "add"] <- gsub(
  "CROWN PLACE APARTMENTS,",
  "CROWN PLACE,",
  epc[epc$postcode == "SE16 3AD", "add"]
)

epc[epc$postcode == "KT5 8DG", "add"] <- gsub(
  "GUILDFORD AVENUE",
  "GUILFORD AVENUE",
  epc[epc$postcode == "KT5 8DG", "add"]
)

epc[epc$postcode == "TN22 1JT", "add"] <- gsub(
  "SHAFTSBURY COURT",
  "SHAFTESBURY COURT",
  epc[epc$postcode == "TN22 1JT", "add"]
)

epc[epc$postcode == "TW10 7PE", "add"] <- gsub(
  "SECRETT HOUSE",
  "SECRETTS HOUSE",
  epc[epc$postcode == "TW10 7PE", "add"]
)

epc[epc$postcode == "TN34 1JN", "add"] <- gsub(
  "QUEENS APARTMENTS,",
  "THE QUEENS APARTMENTS,",
  epc[epc$postcode == "TN34 1JN", "add"]
)

epc[epc$postcode == "TR13 9JR", "add"] <- gsub(
  "THE MONTEREY, ",
  "MONTEREY,",
  epc[epc$postcode == "TR13 9JR", "add"]
)

epc[epc$postcode == "TN38 0LU", "add"] <- gsub(
  "BOSCOBLE ROAD",
  "BOSCOBEL ROAD",
  epc[epc$postcode == "TN38 0LU", "add"]
)

epc[epc$postcode == "WR10 1EH", "add"] <- gsub(
  "REDLAND HOUSE",
  "REDLANDS HOUSE",
  epc[epc$postcode == "WR10 1EH", "add"]
)

epc[epc$postcode == "EN1 2PL", "add"] <- gsub(
  "ASPLEY HOUSE",
  "APSLEY HOUSE",
  epc[epc$postcode == "EN1 2PL", "add"]
)

epc[epc$postcode == "E3 3TU", "add"] <- gsub(
  "GREENFELL COURT",
  "GRENFELL COURT",
  epc[epc$postcode == "E3 3TU", "add"]
)

epc[epc$postcode == "HR9 6AW", "add"] <- gsub(
  "OLD STABLES",
  "THE OLD STABLES",
  epc[epc$postcode == "HR9 6AW", "add"]
)

epc[epc$postcode == "HA2 0ES", "add"] <- gsub(
  "EAST CROFT,",
  "EAST CROFT HOUSE,",
  epc[epc$postcode == "HA2 0ES", "add"]
)

epc[epc$postcode == "IP4 1FP", "add"] <- gsub(
  "THE FOUNDRY",
  "FOUNDRY",
  epc[epc$postcode == "IP4 1FP", "add"]
)

###############matching rule 120 saonpaonstreet2=ADDRE###########################
tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet2 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = ", "))
tran$saonpaonstreet2 <- stri_replace_all_fixed(tran$saonpaonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba89 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 121 saonpaonstreet2=ADD12###########################
tran <- anti_join_original_variables(tran, taba89)

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet2 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = ", "))
tran$saonpaonstreet2 <- stri_replace_all_fixed(tran$saonpaonstreet2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba90 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 122 saonpaonstreetn=ADDC ###########################
tran <- anti_join_original_variables(tran, taba90)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = " "))
tran$saonpaonstreetn <- stri_replace_all_fixed(tran$saonpaonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba91 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 123 saonpaon65street=ADD12C ###########################
tran <- anti_join_original_variables(tran, taba91)

tran1 <- tran[stri_detect_fixed(tran$paon, ","), ]

tran1$paon65 <- word(tran1$paon, -1)
tran1$saonpaon65 <- stri_paste(tran1$saon, tran1$paon65, sep = ", ")
tran1$saonpaon65street <- stri_paste(tran1$saonpaon65, tran1$street, sep = ", ")
tran1$saonpaon65street <- stri_replace_all_fixed(tran1$saonpaon65street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba92 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon65street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 124 saonpaon62cstreetn2=ADD13C ###########################
tran <- anti_join_original_variables(tran, taba92)

tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$paon62c <- char2end(tran$paon, ",")
tran$saonpaon6 <- stri_paste(tran$saon, tran$paon62c, sep = ", ")
tran$saonpaon62cstreetn2 <- stri_trim_both(stri_paste(tran$saonpaon6, tran$streetn, sep = " "))
tran$saonpaon62cstreetn2 <- stri_replace_all_fixed(tran$saonpaon62cstreetn2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba93 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon62cstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 125 saonpaonstreetn=ADD6###########################
tran <- anti_join_original_variables(tran, taba93)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = " "))
tran$saonpaonstreetn <- stri_replace_all_fixed(tran$saonpaonstreetn, " ", "")

epc1 <- epc[stri_detect_fixed(epc$add3, ","), ]

epc1$addressfinal <- stri_paste(epc1$add1, epc1$add2, sep = ", ")
epc1$add361 <- beg2char(epc1$add3, ",")
epc1$addressfinal <- stri_paste(epc1$addressfinal, epc1$add361, sep = ", ")
epc1$addressfinal <- stri_replace_all_regex(epc1$addressfinal, "['./ ]", "")

taba941 <-
  inner_join(
    tran,
    epc1,
    by = c("postcode" = "postcode", "saonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(epc1)

###############matching rule 126 saonpaonstreetn=ADDCC#################
tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonstreetn <- stri_trim_both(stri_paste(tran$saonpaonn, tran$streetn, sep = " "))
tran$saonpaonstreetn <- stri_replace_all_fixed(tran$saonpaonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba9411 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 127 saonpaon61xstreet=ADD12C2#################
tran$paon61x <- beg2char(tran$paon, ",")

tran$saonpaon61 <- stri_paste(tran$saon, tran$paon61x, sep = " ")
tran$saonpaon61street <- stri_paste(tran$saonpaon61, tran$street, sep = ", ")
tran$saonpaon61street <- stri_replace_all_regex(tran$saonpaon61street, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba942 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 128 saonpaon61xstreet=ADDREC#################
tran$paon61x <- beg2char(tran$paon, ",")

tran$saonpaon61 <- stri_paste(tran$saon, tran$paon61x, sep = " ")
tran$saonpaon61street <- stri_paste(tran$saonpaon61, tran$street, sep = ", ")
tran$saonpaon61street <- stri_replace_all_regex(tran$saonpaon61street, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba943 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 129 saonpaon62cstreetn=ADD7##########
tran$paon62c <- char2end(tran$paon, ",")
tran$saonpaon62 <- stri_paste(tran$saon, tran$paon62c, sep = " ")
tran$saonpaon62streetn <- stri_trim_both(stri_paste(tran$saonpaon62, tran$streetn, sep = " "))
tran$saonpaon62streetn <- stri_replace_all_fixed(tran$saonpaon62streetn, " ", "")

epc$add161 <- beg2char(epc$add1, ",")
epc$add12com <- stri_paste(epc$add161, epc$add2, sep = " ")
epc$add12com <- stri_replace_all_fixed(epc$add12com, " ", "")

taba9431 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon62streetn" = "add12com"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 130 saonpaonstreet1=ADD13C2#################
tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreet1 <- stri_replace_all_regex(tran$saonpaonstreet1, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba944 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 131 saonpaon1=ADD1C9#################
tran11 <- tran[!stri_detect_regex(tran$paon, "^\\d"), ]

tran11$saonpaon1 <- stri_paste(tran11$saon, tran11$paon, sep = " ")
tran11$saonpaon1 <- stri_replace_all_fixed(tran11$saonpaon1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add1, " ", "")

taba945 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonpaon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 132 saonpaonn=ADDC4#################
tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonn <- stri_replace_all_fixed(tran$saonpaonn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba9451 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 133 paonstreetn=ADDC4 #################
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$paonstreetn <- stri_paste(tran$paonn, tran$streetn, sep = ", ")
tran$paonstreetn <- stri_replace_all_fixed(tran$paonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

tab120 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  )

tab121 <-
  tab120 |>
  filter(saon == "FLAT") |>
  select(transactionid, id)

tab122 <- anti_join(tab120, tab121, by = "transactionid") # this is the same as saon != "FLAT" but likely faster
tab122 <- tab122[!stri_detect_fixed(tab122$saon, "FLOOR"), ]
tab122 <- tab122[!stri_detect_fixed(tab122$saon, "UPPER"), ]
tab122 <- tab122[!stri_detect_fixed(tab122$saon, "BASEMENT"), ]
tab122 <- tab122[!stri_detect_fixed(tab122$saon, "LOWER"), ]
tab122 <- tab122[!stri_detect_regex(tab122$saon, "FLAT \\d"), ]
tab122 <- tab122[!stri_detect_regex(tab122$saon, "FLAT \\w"), ]
tab122 <- tab122[!stri_detect_fixed(tab122$saon, "FLAT"), ]
tab122 <- tab122[!stri_detect_regex(tab122$saon, "-*\\d+\\.*\\d*"), ]

tab123 <-
  tab122 |>
  filter(
    propertytype.x != "F",
    propertytype.y != "Flat",
    propertytype.y != "Maisonette"
  ) |>
  select(transactionid, id)

tab124 <-
  tab120 |>
  filter(saon == "APARTMENT") |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

taba9452 <- reduce(list(tab121, tab123, tab124), union)

rm(tab120)
rm(tab121)
rm(tab122)
rm(tab123)
rm(tab124)

tran <- anti_join_original_variables(tran, taba941)
tran <- anti_join_original_variables(tran, taba9411)
tran <- anti_join_original_variables(tran, taba942)
tran <- anti_join_original_variables(tran, taba943)
tran <- anti_join_original_variables(tran, taba9431)
tran <- anti_join_original_variables(tran, taba944)
tran <- anti_join_original_variables(tran, taba945)
tran <- anti_join_original_variables(tran, taba9451)
tran <- anti_join_original_variables(tran, taba9452)

###############matching rule 134 saon2paon61street= ADDCC ################
tran1 <- tran[stri_detect_fixed(tran$paon, ","), ]

tran2 <- tran1[tran1$propertytype == "F", ]

tran2$paon61 <- beg2char(tran2$paon, ",")
tran2$saon2 <- stri_replace_all_fixed(tran2$saon, "APARTMENT ", "")
tran2$saon2paon61 <- stri_paste(tran2$saon2, tran2$paon61, sep = " ")
tran2$saon2paon61street <- stri_paste(tran2$saon2paon61, tran2$street, sep = ", ")
tran2$saon2paon61street <- stri_replace_all_fixed(tran2$saon2paon61street, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba9453 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "saon2paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 135 saonpaonn=ADD12C#################
tran <- anti_join_original_variables(tran, taba9453)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonn <- stri_replace_all_fixed(tran$saonpaonn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba9454 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 136 flsaonpaonstreet0=ADD#################
tran <- anti_join_original_variables(tran, taba9454)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d+"), ]

tran2$flsaon <- stri_paste("FLAT", tran2$saon, sep = " ")
tran2$flsaonpaon <- stri_paste(tran2$flsaon, tran2$paon, sep = ", ")
tran2$flsaonpaonstreet <- stri_paste(tran2$flsaonpaon, tran2$street, sep = ", ")

taba946 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet" = "add"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 137 flsaon1paonstreetn2=ADDCC#################
tran <- anti_join_original_variables(tran, taba946)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d+"), ]

tran2$saonn <- stri_replace_all_fixed(tran2$saon, "/", "")
tran2$paonn <- stri_replace_all_regex(tran2$paon, "['.]", "")
tran2$streetn2 <- stri_replace_all_regex(tran2$street, "[-']", "")

tran2$flsaon1 <- stri_paste("FLAT", tran2$saonn, sep = " ")
tran2$flsaon1paonn <- stri_paste(tran2$flsaon1, tran2$paonn, sep = ", ")
tran2$flsaon1paonstreetn2 <- stri_paste(tran2$flsaon1paonn, tran2$streetn2, sep = ", ")
tran2$flsaon1paonstreetn2 <- stri_replace_all_fixed(tran2$flsaon1paonstreetn2, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-'./ ]", "")

taba947 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "flsaon1paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 138 flsaonpaonstreet1= ADDREC#################
tran <- anti_join_original_variables(tran, taba947)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d+"), ]

tran2$flsaon <- stri_paste("FLAT", tran2$saon, sep = " ")
tran2$flsaonpaon <- stri_paste(tran2$flsaon, tran2$paon, sep = " ")
tran2$flsaonpaonstreet1 <- stri_paste(tran2$flsaonpaon, tran2$street, sep = " ")
tran2$flsaonpaonstreet1 <- stri_replace_all_regex(tran2$flsaonpaonstreet1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba948 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 139 flsaonpaon62street1 = ADDREC#################
tran <- anti_join_original_variables(tran, taba948)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d+"), ]

tran3 <- tran2[stri_detect_fixed(tran2$paon, ","), ]
tran3$paon62 <- char2end(tran3$paon, ",")

tran3$flsaon <- stri_paste("FLAT", tran3$saon, sep = " ")
tran3$flsaonpaon62 <- stri_paste(tran3$flsaon, tran3$paon62, sep = " ")
tran3$flsaonpaon62street1 <- stri_paste(tran3$flsaonpaon62, tran3$street, sep = " ")
tran3$flsaonpaon62street1 <- stri_replace_all_regex(tran3$flsaonpaon62street1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba949 <-
  inner_join(
    tran3,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon62street1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)
rm(tran3)

###############matching rule 140  fldsaonpaonstreet1=ADDREC#################
tran <- anti_join_original_variables(tran, taba949)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_fixed(tran1$saon, "FLAT "), ]

tran2 <- tran2[!stri_detect_regex(tran2$paon, "^\\d"), ]

tran2$fldsaon <- stri_replace_all_fixed(tran2$saon, "FLAT ", "")
tran2$fldsaonpaon <- stri_paste(tran2$fldsaon, tran2$paon, sep = " ")
tran2$fldsaonpaonstreet1 <- stri_paste(tran2$fldsaonpaon, tran2$street, sep = " ")
tran2$fldsaonpaonstreet1 <- stri_replace_all_regex(tran2$fldsaonpaonstreet1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba950 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "fldsaonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 141 saon7paonstreet1=ADDRE#################
tran <- anti_join_original_variables(tran, taba950)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_fixed(tran1$saon, "FLAT "), ]

tran2$saon7 <- stri_replace_all_fixed(tran2$saon, "FLAT ", "APARTMENT ")

tran2$saon7paon <- stri_paste(tran2$saon7, tran2$paon, sep = ", ")
tran2$saon7paonstreet1 <- stri_paste(tran2$saon7paon, tran2$street, sep = " ")
tran2$saon7paonstreet1 <- stri_replace_all_fixed(tran2$saon7paonstreet1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba951 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "saon7paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 142 saon7paonstreet2=ADDREC#################
tran <- anti_join_original_variables(tran, taba951)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_fixed(tran1$saon, "FLAT "), ]

tran2$saon7 <- stri_replace_all_fixed(tran2$saon, "FLAT ", "APARTMENT ")

tran2$saon7paon <- stri_paste(tran2$saon7, tran2$paon, sep = " ")
tran2$saon7paonstreet2 <- stri_paste(tran2$saon7paon, tran2$street, sep = " ")
tran2$saon7paonstreet2 <- stri_replace_all_regex(tran2$saon7paonstreet2, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba952 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "saon7paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 143 apsaonpaonstreet1=ADDREC#################
tran <- anti_join_original_variables(tran, taba952)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[!stri_detect_regex(tran1$saon, "^\\d"), ]

tran2$apsaon <- stri_paste("APARTMENT", tran2$saon, sep = " ")
tran2$apsaonpaon <- stri_paste(tran2$apsaon, tran2$paon, sep = " ")
tran2$apsaonpaonstreet1 <- stri_paste(tran2$apsaonpaon, tran2$street, sep = " ")
tran2$apsaonpaonstreet1 <- stri_replace_all_regex(tran2$apsaonpaonstreet1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba953 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "apsaonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 144 saon7paonstreet2=ADD12C2 #################
tran <- anti_join_original_variables(tran, taba953)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_fixed(tran1$saon, "FLAT "), ]

tran2$saon7 <- stri_replace_all_fixed(tran2$saon, "FLAT ", "APARTMENT ")

tran2$saon7paon <- stri_paste(tran2$saon7, tran2$paon, sep = " ")
tran2$saon7paonstreet2 <- stri_paste(tran2$saon7paon, tran2$street, sep = " ")
tran2$saon7paonstreet2 <- stri_replace_all_regex(tran2$saon7paonstreet2, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba954 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "saon7paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 145 apsaonpaonstreet1=ADD12C2#################
tran <- anti_join_original_variables(tran, taba954)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]

tran2$apsaon <- stri_paste("APARTMENT", tran2$saon, sep = " ")
tran2$apsaonpaon <- stri_paste(tran2$apsaon, tran2$paon, sep = " ")
tran2$apsaonpaonstreet1 <- stri_paste(tran2$apsaonpaon, tran2$street, sep = " ")
tran2$apsaonpaonstreet1 <- stri_replace_all_regex(tran2$apsaonpaonstreet1, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba955 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "apsaonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)

###############matching rule 146 saon7paonstreetn=ADDC4#################
tran <- anti_join_original_variables(tran, taba955)

tran$saon7 <- stri_replace_all_fixed(tran$saon, "FLAT ", "APARTMENT ")
tran$saon71 <- stri_replace_all_fixed(tran$saon7, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saon7paonn <- stri_paste(tran$saon71, tran$paonn, sep = ", ")
tran$saon7paonstreetn <- stri_trim_both(stri_paste(tran$saon7paonn, tran$streetn, sep = " "))
tran$saon7paonstreetn <- stri_replace_all_fixed(tran$saon7paonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba956 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saon7paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 147 saon7paonn=ADD12C4#################
tran <- anti_join_original_variables(tran, taba956)

tran$saon7 <- stri_replace_all_fixed(tran$saon, "FLAT ", "APARTMENT ")

tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saon7paonn <- stri_paste(tran$saon7, tran$paonn, sep = ", ")
tran$saon7paonn <- stri_replace_all_fixed(tran$saon7paonn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[-./ ]", "")

taba957 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saon7paonn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 148 saon4paonstreetn=ADDC4#################
tran <- anti_join_original_variables(tran, taba957)

tran$fldsaon1 <- stri_replace_all_fixed(tran$saon, "FLAT ", "")
tran$saonn4 <- stri_replace_all_fixed(tran$fldsaon1, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saon4paonn <- stri_paste(tran$saonn4, tran$paonn, sep = ", ")
tran$saon4paonstreetn <- stri_trim_both(stri_paste(tran$saon4paonn, tran$streetn, sep = " "))
tran$saon4paonstreetn <- stri_replace_all_fixed(tran$saon4paonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba958 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saon4paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 149 apsaonpaon6streetn=ADDC4#################
tran <- anti_join_original_variables(tran, taba958)

tran1 <- tran[stri_detect_fixed(tran$paon, ","), ]

tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$paon62 <- char2end(tran1$paon, ",")
tran1$apsaon <- stri_paste("APARTMENT", tran1$saon, sep = " ")
tran1$apsaonpaon6 <- stri_paste(tran1$apsaon, tran1$paon62, sep = ", ")
tran1$apsaonpaon6streetn <- stri_trim_both(stri_paste(tran1$apsaonpaon6, tran1$streetn, sep = " "))
tran1$apsaonpaon6streetn <- stri_replace_all_fixed(tran1$apsaonpaon6streetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba961 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "apsaonpaon6streetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 150 flsaonpaonstreetn=ADDC4#################
tran <- anti_join_original_variables(tran, taba961)

tran1 <- tran[tran$propertytype == "F", ]
tran1$flsaon <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$flsaonpaon <- stri_paste(tran1$flsaon, tran1$paonn, sep = ", ")
tran1$flsaonpaonstreetn <- stri_trim_both(stri_paste(tran1$flsaonpaon, tran1$streetn, sep = " "))
tran1$flsaonpaonstreetn <- stri_replace_all_fixed(tran1$flsaonpaonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba962 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 151 saon4paonstreetn3=ADDC5#################
tran <- anti_join_original_variables(tran, taba962)

tran1 <- tran[!stri_detect_regex(tran$paon, "^\\d"), ]

tran1$saon4 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "")
tran1$saonn4 <- stri_replace_all_fixed(tran1$saon4, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon4paonn <- stri_paste(tran1$saonn4, tran1$paonn, sep = " ")
tran1$saon4paonstreetn3 <- stri_trim_both(stri_paste(tran1$saon4paonn, tran1$streetn, sep = " "))
tran1$saon4paonstreetn3 <- stri_replace_all_fixed(tran1$saon4paonstreetn3, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[./ ]", "")

taba963 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon4paonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 152 saon4paonstreetn4=ADD12C#################
tran <- anti_join_original_variables(tran, taba963)

tran$saon4 <- stri_replace_all_fixed(tran$saon, "FLAT ", "")

tran$saonn4 <- stri_replace_all_fixed(tran$saon4, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saon4paonn <- stri_paste(tran$saonn4, tran$paonn, sep = ", ")
tran$saon4paonstreetn4 <- stri_trim_both(stri_paste(tran$saon4paonn, tran$streetn, sep = ", "))
tran$saon4paonstreetn4 <- stri_replace_all_fixed(tran$saon4paonstreetn4, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba964 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saon4paonstreetn4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 153  saon4paonstreetn1=ADD12C#################
tran <- anti_join_original_variables(tran, taba964)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saon4 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "")
tran1$saonn4 <- stri_replace_all_fixed(tran1$saon4, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon4paonn1 <- stri_paste(tran1$saonn4, tran1$paonn, sep = " ")
tran1$saon4paonstreetn1 <- stri_trim_both(stri_paste(tran1$saon4paonn1, tran1$streetn, sep = ", "))
tran1$saon4paonstreetn1 <- stri_replace_all_fixed(tran1$saon4paonstreetn1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba965 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon4paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)

tran <- anti_join_original_variables(tran, taba965)

###############matching rule 154  saon1paonstreetn=ADDC#################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = " ")
tran1$saon1paonstreetn <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = ", "))
tran1$saon1paonstreetn <- stri_replace_all_fixed(tran1$saon1paonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba94 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 155 saon1paonstreetn=ADD12C###########################
tran <- anti_join_original_variables(tran, taba94)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = " ")
tran1$saon1paonstreetn <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = ", "))
tran1$saon1paonstreetn <- stri_replace_all_fixed(tran1$saon1paonstreetn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba95 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 156  saon1paonstreetn1=ADDC ####################
tran <- anti_join_original_variables(tran, taba95)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn1 <- stri_paste(tran1$saonn1, tran1$paonn, sep = ", ")
tran1$saon1paonstreetn1 <- stri_trim_both(stri_paste(tran1$saon1paonn1, tran1$streetn, sep = ", "))
tran1$saon1paonstreetn1 <- stri_replace_all_fixed(tran1$saon1paonstreetn1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba96 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 157  saon1paonstreetn1=ADD12C####################
tran <- anti_join_original_variables(tran, taba96)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn1 <- stri_paste(tran1$saonn1, tran1$paonn, sep = ", ")
tran1$saon1paonstreetn1 <- stri_trim_both(stri_paste(tran1$saon1paonn1, tran1$streetn, sep = ", "))
tran1$saon1paonstreetn1 <- stri_replace_all_fixed(tran1$saon1paonstreetn1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba97 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 158  saon1paonstreetn2=ADDC3 ####################
tran <- anti_join_original_variables(tran, taba97)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = " ")
tran1$saon1paonstreetn2 <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = " "))
tran1$saon1paonstreetn2 <- stri_replace_all_regex(tran1$saon1paonstreetn2, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[,'./ ]", "")

taba98 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 159  saon1paonstreetn2=ADD12C1####################
tran <- anti_join_original_variables(tran, taba98)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saonpaonn1 <- stri_trim_both(stri_paste(tran1$saonn1, tran1$paonn, sep = " "))
tran1$saon1paonstreetn2 <- stri_trim_both(stri_paste(tran1$saonpaonn1, tran1$streetn, sep = " "))
tran1$saon1paonstreetn2 <- stri_replace_all_fixed(tran1$saon1paonstreetn2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba99 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 160 saon2paon61street=ADD12C ####################
tran <- anti_join_original_variables(tran, taba99)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]
tran2 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran2$paon61 <- beg2char(tran2$paon, ",")
tran2$saon2 <- stri_replace_all_fixed(tran2$saon, "APARTMENT ", "")
tran2$saon2paon61 <- stri_paste(tran2$saon2, tran2$paon61, sep = " ")
tran2$saon2paon61street <- stri_paste(tran2$saon2paon61, tran2$street, sep = ", ")
tran2$saon2paon61street <- stri_replace_all_fixed(tran2$saon2paon61street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba100 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "saon2paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)
rm(tran11)

###############matching rule 161  saon2paonstreetn3=ADDC####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn3 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = " "))
tran1$saon2paonstreetn3 <- stri_replace_all_fixed(tran1$saon2paonstreetn3, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba101 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 162  saon2paonstreetn3=ADD12C####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn3 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = " "))

tran1$saon2paonstreetn3 <- stri_replace_all_fixed(tran1$saon2paonstreetn3, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba102 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 163 saon2paonstreetn2=ADDC####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d+"), ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn2 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn2 <- stri_replace_all_fixed(tran1$saon2paonstreetn2, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba103 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 164 saon2paonstreetn2=ADD12C ####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d+"), ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn2 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn2 <- stri_replace_all_fixed(tran1$saon2paonstreetn2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba104 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

###############matching rule 165 saonn2paonn1=ADDC ####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")

tran1$saonn2paonn1 <- stri_paste(tran1$saonn2, tran1$paonn, sep = " ")
tran1$saonn2paonn1 <- stri_replace_all_fixed(tran1$saonn2paonn1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba105 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonn2paonn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)
rm(tran11)

tran <- anti_join_original_variables(tran, taba100)
tran <- anti_join_original_variables(tran, taba101)
tran <- anti_join_original_variables(tran, taba102)
tran <- anti_join_original_variables(tran, taba103)
tran <- anti_join_original_variables(tran, taba104)
tran <- anti_join_original_variables(tran, taba105)

###############matching rule 166  saonpaon62street=ADD12C####################
tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11 <- tran11[stri_detect_fixed(tran11$paon, ","), ]
tran11$paon62 <- char2end(tran11$paon, ",")

tran11$saonpaon6 <- stri_paste(tran11$saon, tran11$paon62, sep = ", ")
tran11$saonpaon62street <- stri_paste(tran11$saonpaon6, tran11$street, sep = " ")
tran11$saonpaon62street <- stri_replace_all_fixed(tran11$saonpaon62street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba107 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonpaon62street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 167  saon1paonstreet6n1=ADD12C####################
tran <- anti_join_original_variables(tran, taba107)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11 <- tran11[stri_detect_fixed(tran11$paon, ","), ]
tran11$paon62 <- char2end(tran11$paon, ",")

tran11$saon1 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "FLAT ")
tran11$saon1 <- stri_replace_all_fixed(tran11$saon1, "/", "")

tran11$saonpaonn6 <- stri_paste(tran11$saon1, tran11$paon62, sep = ", ")
tran11$saon1paonstreet6n1 <- stri_paste(tran11$saonpaonn6, tran11$street, sep = " ")
tran11$saon1paonstreet6n1 <- stri_replace_all_fixed(tran11$saon1paonstreet6n1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba108 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreet6n1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 168 saon2paonstreetn=ADD12C ####################
tran <- anti_join_original_variables(tran, taba108)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saon2 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "")
tran11$saonn2 <- stri_replace_all_fixed(tran11$saon2, "/", "")
tran11$paonn <- stri_replace_all_regex(tran11$paon, "['.]", "")

tran11$saonn2paon1 <- stri_paste(tran11$saonn2, tran11$paonn, sep = ", ")
tran11$saon2paonstreetn <- stri_paste(tran11$saonn2paon1, tran11$street, sep = " ")
tran11$saon2paonstreetn <- stri_replace_all_fixed(tran11$saon2paonstreetn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba109 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)


rm(tran11)

###############matching rule 169 saonn3paonnstreet=ADD13C ####################
tran <- anti_join_original_variables(tran, taba109)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saonn3 <- stri_replace_all_regex(tran11$saon, "[./]", "")
tran11$paonn <- stri_replace_all_regex(tran11$paon, "['.]", "")
tran11$saonn3paonn <- stri_paste(tran11$saonn3, tran11$paonn, sep = ", ")
tran11$saonn3paonnstreet <- stri_paste(tran11$saonn3paonn, tran11$street, sep = " ")
tran11$saonn3paonnstreet <- stri_replace_all_fixed(tran11$saonn3paonnstreet, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba110 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonn3paonnstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 170 saonn2paonn1streetn=ADDC ####################
tran <- anti_join_original_variables(tran, taba110)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saon2 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "")
tran11$saonn2 <- stri_replace_all_fixed(tran11$saon2, "/", "")
tran11$paonn <- stri_replace_all_regex(tran11$paon, "['.]", "")

tran11$saonn2paonn1 <- stri_paste(tran11$saonn2, tran11$paonn, sep = ", ")
tran11$saonn2paonn1streetn <- stri_paste(tran11$saonn2paonn1, tran11$street, sep = " ")
tran11$saonn2paonn1streetn <- stri_replace_all_fixed(tran11$saonn2paonn1streetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba111 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonn2paonn1streetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 171  paon62saonpstreet= ADDREC####################
tran <- anti_join_original_variables(tran, taba111)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]
tran1$paon61 <- beg2char(tran1$paon, ",")
tran1$paon62 <- char2end(tran1$paon, ",")

tran1$paon62saon <- stri_paste(tran1$paon62, tran1$saon, sep = " ")
tran1$paon62saonp <- stri_paste(tran1$paon62saon, tran1$paon61, sep = " ")
tran1$paon62saonpstreet <- stri_trim_both(stri_paste(tran1$paon62saonp, tran1$street, sep = ", "))
tran1$paon62saonpstreet <- stri_replace_all_regex(tran1$paon62saonpstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba112 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "paon62saonpstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 172 saonpaon62streetn1=ADDC ####################
tran <- anti_join_original_variables(tran, taba112)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saonn <- stri_replace_all_fixed(tran11$saon, "/", "")
tran11$paonn <- stri_replace_all_regex(tran11$paon, "['.]", "")
tran11$streetn <- stri_replace_all_fixed(tran11$street, "'", "")

tran11$paon62 <- char2end(tran11$paonn, ",")
tran11$saonpaon62 <- stri_paste(tran11$saon, tran11$paon62, sep = ", ")
tran11$saonpaon62streetn1 <- stri_trim_both(stri_paste(tran11$saonpaon62, tran11$streetn, sep = ", "))
tran11$saonpaon62streetn1 <- stri_replace_all_fixed(tran11$saonpaon62streetn1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba113 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonpaon62streetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 173 saon1paonstreet6n=ADD12C####################
tran <- anti_join_original_variables(tran, taba113)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saon1 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "FLAT ")
tran11$saonn1 <- stri_replace_all_fixed(tran11$saon1, "/", "")
tran11$paonn <- stri_replace_all_regex(tran11$paon, "['.]", "")
tran11$streetn <- stri_replace_all_fixed(tran11$street, "'", "")
tran11$paon62c <- char2end(tran11$paonn, ",")
tran11$saon1paonn6 <- stri_paste(tran11$saonn1, tran11$paon62c, sep = ", ")
tran11$saon1paonstreet6n <- stri_trim_both(stri_paste(tran11$saon1paonn6, tran11$streetn, sep = ", "))
tran11$saon1paonstreet6n <- stri_replace_all_fixed(tran11$saon1paonstreet6n, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba114 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreet6n" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 174  saon2paonstreetn4=ADDC####################
tran <- anti_join_original_variables(tran, taba114)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = ", "))
tran1$saon2paonstreetn4 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn4 <- stri_replace_all_fixed(tran1$saon2paonstreetn4, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba115 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 175 saon2paonstreetn4=ADD12C ####################
tran <- anti_join_original_variables(tran, taba115)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = ", "))
tran1$saon2paonstreetn4 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn4 <- stri_replace_all_fixed(tran1$saon2paonstreetn4, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba116 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 176 saon2paonstreetn4=ADD1num2####################
tran <- anti_join_original_variables(tran, taba116)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon2paonn1 <- stri_paste(tran1$saonn2, tran1$paonn, sep = ", ")
tran1$saon2paonstreetn4 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn4 <- stri_replace_all_fixed(tran1$saon2paonstreetn4, " ", "")

epc$add1num <- numextract(epc$add1)
epc$addressfinal <- stri_paste(epc$add1num, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba118 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(
    !stri_detect_regex(add1, "\\d+(A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T)")
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 177  saon5paonstreetn1=ADDC####################
tran <- anti_join_original_variables(tran, taba118)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon5 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "UNIT ")
tran1$saonn5 <- stri_replace_all_fixed(tran1$saon5, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon5paonn <- stri_paste(tran1$saonn5, tran1$paonn, sep = " ")
tran1$saon5paonstreetn1 <- stri_trim_both(stri_paste(tran1$saon5paonn, tran1$streetn, sep = ", "))
tran1$saon5paonstreetn1 <- stri_replace_all_fixed(tran1$saon5paonstreetn1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba120 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon5paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 178 paonsaon2streetn=ADD1C ####################
tran <- anti_join_original_variables(tran, taba120)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1 <- tran1[stri_detect_regex(tran1$saon, "APARTMENT\\s[A-Z]"), ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$paonsaon2 <- stri_paste(tran1$paonn, tran1$saonn2, sep = " ")
tran1$paonsaon2streetn <- stri_trim_both(stri_paste(tran1$paonsaon2, tran1$streetn, sep = " "))
tran1$paonsaon2streetn <- stri_replace_all_fixed(tran1$paonsaon2streetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "['./ ]", "")

taba122 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "paonsaon2streetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 179 saon2paonstreetn2=ADD13C ####################
tran <- anti_join_original_variables(tran, taba122)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn2 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = ", "))
tran1$saon2paonstreetn2 <- stri_replace_all_fixed(tran1$saon2paonstreetn2, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add3, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba124 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)


rm(tran11)
rm(tran1)

###############matching rule 180  saonpaon66street=ADDC6####################
tran <- anti_join_original_variables(tran, taba124)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11 <- tran11[stri_detect_fixed(tran11$paon, ","), ]

tran11$paon61 <- beg2char(tran11$paon, ",")
tran11$paon62 <- char2end(tran11$paon, ",")

tran11$saonpaon62 <- stri_paste(tran11$saon, tran11$paon62, sep = ", ")
tran11$saonpaon66 <- stri_paste(tran11$saonpaon62, tran11$paon61, sep = " ")
tran11$saonpaon66street <- stri_paste(tran11$saonpaon66, tran11$street, sep = " ")
tran11$saonpaon66street <- stri_replace_all_regex(tran11$saonpaon66street, "[, ]", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, "'", "")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba125 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saonpaon66street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 181 saon1paonstreetn3=ADD12C ####################
tran <- anti_join_original_variables(tran, taba125)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = ", ")
tran1$saon1paonstreetn3 <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = " "))
tran1$saon1paonstreetn3 <- stri_replace_all_fixed(tran1$saon1paonstreetn3, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba126 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 182  saon2street=ADDC ####################
tran <- anti_join_original_variables(tran, taba126)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")

tran1$saon2street <- stri_paste(tran1$saon2, tran1$street, sep = ", ")
tran1$saon2street <- stri_replace_all_fixed(tran1$saon2street, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba127 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 183 saon1paonstreet=ADDRE###########################
tran <- anti_join_original_variables(tran, taba127)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]


tran11$saon1 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "FLAT ")

tran11$saon1paon <- stri_paste(tran11$saon1, tran11$paon, sep = ", ")
tran11$saon1paonstreet <- stri_trim_both(stri_paste(tran11$saon1paon, tran11$street, sep = " "))
tran11$saon1paonstreet <- stri_replace_all_fixed(tran11$saon1paonstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba128 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 184 saon2paonlo=ADDRE###########################
tran <- anti_join_original_variables(tran, taba128)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")

tran1$saon2paon <- stri_paste(tran1$saon2, tran1$paon, sep = " ")
tran1$saon2paonlo <- stri_paste(tran1$saon2paon, tran1$locality, sep = ", ")
tran1$saon2paonlo <- stri_replace_all_fixed(tran1$saon2paonlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba129 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 185 saon1paon=ADD12###########################
tran <- anti_join_original_variables(tran, taba129)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$sao1 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "FLAT ")

tran11$saon1paon <- stri_paste(tran11$sao1, tran11$paon, sep = ", ")

tran11$saon1paon <- stri_replace_all_fixed(tran11$saon1paon, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba130 <-
  inner_join(
    tran11,
    epc,
    by = c("postcode" = "postcode", "saon1paon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 186 saon1paon61street=ADD12###########################
tran <- anti_join_original_variables(tran, taba130)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$sao1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$paon61c <- beg2char(tran1$paon, ",")

tran1$saon1paon61 <- stri_paste(tran1$sao1, tran1$paon61c, sep = " ")
tran1$saon1paon61street <- stri_trim_both(stri_paste(tran1$saon1paon61, tran1$street, sep = ", "))
tran1$saon1paon61street <- stri_replace_all_fixed(tran1$saon1paon61street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba131 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 187 saon2paonstreet=ADD12 ###########################
tran <- anti_join_original_variables(tran, taba131)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype != "F", ]
tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")

tran1$saon2paon <- stri_paste(tran1$saon2, tran1$paon, sep = " ")
tran1$saon2paonstreet <- stri_paste(tran1$saon2paon, tran1$street, sep = ", ")
tran1$saon2paonstreet <- stri_replace_all_fixed(tran1$saon2paonstreet, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba132 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(!(propertytype.y %in% c("Flat", "Maisonette"))) |>
  select(transactionid, id)


rm(tran11)
rm(tran1)

###############matching rule 188 saon1paon1=ADD1C9###########################
tran <- anti_join_original_variables(tran, taba132)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$sao1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saon1paon1 <- stri_paste(tran1$sao1, tran1$paon, sep = " ")
tran1$saon1paon1 <- stri_replace_all_fixed(tran1$saon1paon1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add1, " ", "")

taba133 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 189 saon1paonstreetn2=ADD12C2###########################
tran <- anti_join_original_variables(tran, taba133)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")

tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = " ")
tran1$saon1paonstreetn2 <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = " "))
tran1$saon1paonstreetn2 <- stri_replace_all_regex(tran1$saon1paonstreetn2, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba134 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 190  psaonpaonstreet=ADDREC###########################
tran <- anti_join_original_variables(tran, taba134)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]

tran1$paon64 <- beg2char(tran1$paon, " ")
tran1$paon641 <- char2end(tran1$paon, " ")

tran1$psaon <- stri_paste(tran1$paon64, tran1$saon, sep = "")
tran1$psaonpaon <- stri_paste(tran1$psaon, tran1$paon641, sep = " ")
tran1$psaonpaonstreet <- stri_trim_both(stri_paste(tran1$psaonpaon, tran1$street, sep = ", "))
tran1$psaonpaonstreet <- stri_replace_all_regex(tran1$psaonpaonstreet, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba135 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "psaonpaonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 191 saon2paon62street=ADD12###########################
tran <- anti_join_original_variables(tran, taba135)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$paon62 <- char2end(tran1$paon, ",")

tran1$saon2paon62 <- stri_paste(tran1$saon2, tran1$paon62, sep = ", ")
tran1$saon2paon62street <- stri_paste(tran1$saon2paon62, tran1$street, sep = ", ")
tran1$saon2paon62street <- stri_replace_all_fixed(tran1$saon2paon62street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba136 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paon62street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran11)
rm(tran1)

###############matching rule 192 saon2paonstreet=ADD1262 ###########################
tran <- anti_join_original_variables(tran, taba136)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran11$saon2 <- stri_replace_all_fixed(tran11$saon, "APARTMENT ", "")

tran11$saon2paon <- stri_paste(tran11$saon2, tran11$paon, sep = " ")
tran11$saon2paonstreet <- stri_paste(tran11$saon2paon, tran11$street, sep = ", ")
tran11$saon2paonstreet <- stri_replace_all_fixed(tran11$saon2paonstreet, " ", "")

epc1 <- epc[stri_detect_fixed(epc$add2, ","), ]
epc11 <- epc1[stri_detect_regex(epc1$add2, "\\d+-\\d+"), ]
epc11$add262 <- char2end(epc11$add2, ",")
epc11$add1262 <- stri_paste(epc11$add1, epc11$add262, sep = ", ")
epc11$addressfinal <- stri_replace_all_fixed(epc11$add1262, " ", "")

taba137 <-
  inner_join(
    tran11,
    epc11,
    by = c("postcode" = "postcode", "saon2paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran11)

###############matching rule 193 saonpaonstreetn2=ADD7#############
tran <- anti_join_original_variables(tran, taba137)


tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saonpaonn1 <- stri_trim_both(stri_paste(tran$saonn, tran$paonn, sep = " "))
tran$saonpaonstreetn2 <- stri_trim_both(stri_paste(tran$saonpaonn1, tran$streetn, sep = ", "))
tran$saonpaonstreetn2 <- stri_replace_all_fixed(tran$saonpaonstreetn2, " ", "")

epc$add161x <- beg2char(epc$add1, ",")
epc$addressfinal <- stri_paste(epc$add161x, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba138 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreetn2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 194 flsaonpaonstreet=add1f61f2#################
tran <- anti_join_original_variables(tran, taba138)

tran1 <- tran[tran$propertytype == "F", ]
tran2 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]
tran2 <- tran2[!stri_detect_fixed(tran2$paon, ","), ]

tran2$flsaon <- stri_paste("FLAT ", tran2$saon, sep = "")
tran2$flsaonpaon <- stri_paste(tran2$flsaon, tran2$paon, sep = ", ")
tran2$flsaonpaonstreet <- stri_paste(tran2$flsaonpaon, tran2$street, sep = ", ")
tran2$flsaonpaonstreet <- stri_replace_all_regex(tran2$flsaonpaonstreet, "[, ]", "")

epc1 <- epc[stri_detect_fixed(epc$add1, "FLAT "), ]

epc1$add1f <- stri_replace_all_fixed(epc1$add1, "FLAT ", "")

epc1$add1f61 <- beg2char(epc1$add1f, " ")
epc1$add1f61 <- stri_replace_all_fixed(epc1$add1f61, ",", "")
epc1$add1f61f <- stri_paste("FLAT", epc1$add1f61, sep = " ")

epc1$add1f61f2 <- stri_paste(epc1$add1f61f, epc1$add2, sep = ", ")
epc1$add1f61f2 <- stri_replace_all_regex(epc1$add1f61f2, "[, ]", "")

taba139 <-
  inner_join(
    tran2,
    epc1,
    by = c("postcode" = "postcode", "flsaonpaonstreet" = "add1f61f2"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)
rm(tran2)
rm(epc1)

###############matching rule 195 psaon8street=ADDREC#################
tran <- anti_join_original_variables(tran, taba139)

tran1 <- tran[stri_detect_regex(tran$saon, "FLAT\\s[A-Z]$"), ]
tran1$fldsaon1 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "")

tran2 <- tran1[stri_detect_regex(tran1$paon, "^\\d"), ]

tran2$psaon8 <- stri_paste(tran2$paon, tran2$fldsaon1, sep = "")
tran2$psaon8street <- stri_paste(tran2$psaon8, tran2$street, sep = " ")
tran2$psaon8street <- stri_replace_all_regex(tran2$psaon8street, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba140 <-
  inner_join(
    tran2,
    epc,
    by = c("postcode" = "postcode", "psaon8street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 196 saonpaonstreet1=add12643#################
tran <- anti_join_original_variables(tran, taba140)

tran$saonpaon <- stri_paste(tran$saon, tran$paon, sep = ", ")
tran$saonpaonstreet1 <- stri_trim_both(stri_paste(tran$saonpaon, tran$street, sep = ", "))
tran$saonpaonstreet1 <- stri_replace_all_fixed(tran$saonpaonstreet1, " ", "")

epc$add264 <- char2end(epc$add2, " ")
epc$add1264 <- stri_paste(epc$add1, epc$add264, sep = ", ")
epc$add12643 <- stri_paste(epc$add1264, epc$add3, sep = ", ")
epc$add12643 <- stri_replace_all_fixed(epc$add12643, " ", "")

taba141 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "add12643"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 197 saonstreet=ADDRE#################
tran <- anti_join_original_variables(tran, taba141)

tran1 <- tran[tran$propertytype != "F", ]

tran1$saonstreet <- stri_paste(tran1$saon, tran1$street, sep = ", ")
tran1$saonstreet <- stri_replace_all_fixed(tran1$saonstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba142 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 198 saonstreetlo= ADDRE#################
tran <- anti_join_original_variables(tran, taba142)

tran$saonstreet <- stri_paste(tran$saon, tran$street, sep = ", ")
tran$saonstreetlo <- stri_paste(tran$saonstreet, tran$locality, sep = ", ")
tran$saonstreetlo <- stri_replace_all_fixed(tran$saonstreetlo, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba143 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonstreetlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 199 unsaonpaonstreet2=ADDRE##################
tran <- anti_join_original_variables(tran, taba143)

tran1 <- tran[stri_detect_regex(tran$saon, "^\\d"), ]

tran1$unsaon <- stri_paste("UNIT", tran1$saon, sep = " ")

tran1$unsaonpaon1 <- stri_trim_both(stri_paste(tran1$unsaon, tran1$paon, sep = " "))
tran1$unsaonpaonstreet2 <- stri_trim_both(stri_paste(tran1$unsaonpaon1, tran1$street, sep = ", "))
tran1$unsaonpaonstreet2 <- stri_replace_all_fixed(tran1$unsaonpaonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba144 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "unsaonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 200 flsaonpaonstreet2=ADD8##################
tran <- anti_join_original_variables(tran, taba144)

tran1 <- tran[tran$propertytype == "F", ]

tran1$flsaon3 <- stri_paste("FLAT", tran1$saon, sep = " ")

tran1$flsaonpaon <- stri_paste(tran1$flsaon3, tran1$paon, sep = " ")
tran1$flsaonpaonstreet2 <- stri_paste(tran1$flsaonpaon, tran1$street, sep = ", ")
tran1$flsaonpaonstreet2 <- stri_replace_all_fixed(tran1$flsaonpaonstreet2, " ", "")

epc$add1c <- stri_replace_all_fixed(epc$add1, "/", "")

epc$addressfinal <- stri_paste(epc$add1c, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba145 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 201 flsaonpaon1=ADD1C9#################
tran <- anti_join_original_variables(tran, taba145)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]

tran1$flsaon <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaonpaon1 <- stri_paste(tran1$flsaon, tran1$paon, sep = " ")
tran1$flsaonpaon1 <- stri_replace_all_fixed(tran1$flsaonpaon1, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add1, " ", "")

taba146 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 202 saonpaon1=fladd#################
tran <- anti_join_original_variables(tran, taba146)
# SS1 2HZ
tran1 <- tran[tran$propertytype == "F", ]

tran1$saonpaon1 <- stri_paste(tran1$saon, tran1$paon, sep = " ")
tran1$saonpaon1 <- stri_replace_all_fixed(tran1$saonpaon1, " ", "")

epc$fladd <- stri_paste("FLAT", epc$add, sep = " ")
epc$addressfinal <- stri_replace_all_fixed(epc$fladd, " ", "")

taba147 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 203 saonpaon1=fladd1c#################
tran <- anti_join_original_variables(tran, taba147)
#SS1 2HZ
tran1 <- tran[tran$propertytype == "F", ]

tran1$saonpaon1 <- stri_paste(tran1$saon, tran1$paon, sep = " ")
tran1$saonpaon1 <- stri_replace_all_fixed(tran1$saonpaon1, " ", "")

epc$fladd1 <- stri_paste("FLAT", epc$add1, sep = " ")
epc$addressfinal <- stri_trim_both(epc$fladd1)
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba148 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 204 saonpaonstreet3=fladd#################
tran <- anti_join_original_variables(tran, taba148)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[tran1$saon == "FLAT", ]

tran1$saonpaon <- stri_paste(tran1$saon, tran1$paon, sep = " ")
tran1$saonpaonstreet3 <- stri_paste(tran1$saonpaon, tran1$street, sep = " ")
tran1$saonpaonstreet3 <- stri_replace_all_fixed(tran1$saonpaonstreet3, " ", "")

epc$fladd <- stri_paste("FLAT", epc$add, sep = " ")
epc$addressfinal <- stri_replace_all_fixed(epc$fladd, " ", "")

taba149 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 205 saon7paon6street=ADDRE#################
tran <- anti_join_original_variables(tran, taba149)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$paon62 <- char2end(tran1$paon, ",")

tran1$saon7 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "APARTMENT ")
tran1$saon7paon6 <- stri_paste(tran1$saon7, tran1$paon62, sep = ", ")
tran1$saon7paon6street <- stri_trim_both(stri_paste(tran1$saon7paon6, tran1$street, sep = " "))
tran1$saon7paon6street <- stri_replace_all_fixed(tran1$saon7paon6street, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba150 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon7paon6street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 206 saon7paon6street=ADD12#################
tran <- anti_join_original_variables(tran, taba150)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$paon62 <- char2end(tran1$paon, ",")

tran1$saon7 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "APARTMENT ")
tran1$saon7paon6 <- stri_paste(tran1$saon7, tran1$paon62, sep = ", ")
tran1$saon7paon6street <- stri_trim_both(stri_paste(tran1$saon7paon6, tran1$street, sep = " "))
tran1$saon7paon6street <- stri_replace_all_fixed(tran1$saon7paon6street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba151 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon7paon6street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 207 saon8paonstreet2=ADDRE################
tran <- anti_join_original_variables(tran, taba151)

tran1 <- tran[stri_detect_regex(tran$saon, "LOFT\\s"), ]

tran1$saon8 <- stri_replace_all_fixed(tran1$saon, "LOFT ", "FLAT ")
tran1$saon8paon1 <- stri_trim_both(stri_paste(tran1$saon8, tran1$paon, sep = " "))
tran1$saon8paonstreet2 <- stri_trim_both(stri_paste(tran1$saon8paon1, tran1$street, sep = ", "))
tran1$saon8paonstreet2 <- stri_replace_all_fixed(tran1$saon8paonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba152 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon8paonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 208 saonpaonstreet2=fladd##################
tran <- anti_join_original_variables(tran, taba152)

epc[epc$postcode == "LA4 5JL", "add"] <- gsub(
  "QUEENS SQUARE",
  "QUEEN SQUARE",
  epc[epc$postcode == "LA4 5JL", "add"]
)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d"), ]

tran1$saonpaon1 <- stri_trim_both(stri_paste(tran1$saon, tran1$paon, sep = " "))
tran1$saonpaonstreet2 <- stri_trim_both(stri_paste(tran1$saonpaon1, tran1$street, sep = ", "))
tran1$saonpaonstreet2 <- stri_replace_all_fixed(tran1$saonpaonstreet2, " ", "")

epc$fladd <- stri_paste("FLAT", epc$add, sep = " ")
epc$addressfinal <- stri_replace_all_fixed(epc$fladd, " ", "")

taba153 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 209 paonlo=ADD12##################
tran <- anti_join_original_variables(tran, taba153)

tran1 <- tran[stri_detect_regex(tran$paon, "^\\d"), ]
tran1 <- tran1[tran1$street == "", ]

tran1$paonlo <- stri_paste(tran1$paon, tran1$locality, sep = ", ")
tran1$paonlo <- stri_replace_all_fixed(tran1$paonlo, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba154 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "paonlo" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 210 saonpaonstreet1=adddap ##################
tran <- anti_join_original_variables(tran, taba154)

epc[epc$postcode == "DE14 3DH", "add"] <- gsub(
  "BRANSTON GREEN,",
  "BRANSTON GREENS,",
  epc[epc$postcode == "DE14 3DH", "add"]
)
tran1 <- tran[tran$propertytype == "F", ]

tran1$saonpaon1 <- stri_trim_both(stri_paste(tran1$saon, tran1$paon, sep = ", "))
tran1$saonpaonstreet1 <- stri_trim_both(stri_paste(tran1$saonpaon1, tran1$street, sep = ", "))
tran1$saonpaonstreet1 <- stri_replace_all_fixed(tran1$saonpaonstreet1, " ", "")

epc$adddap <- stri_replace_all_fixed(epc$add, "APARTMENT ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$adddap, " ", "")

taba155 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 211  saonpaon61xstreet=fladdc #################
tran <- anti_join_original_variables(tran, taba155)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d"), ]

tran1$paon61x <- beg2char(tran1$paon, ",")
tran1$saonpaon61 <- stri_paste(tran1$saon, tran1$paon61x, sep = " ")
tran1$saonpaon61xstreet <- stri_paste(tran1$saonpaon61, tran1$street, sep = ", ")
tran1$saonpaon61xstreet <- stri_replace_all_regex(tran1$saonpaon61xstreet, "[, ]", "")

epc$fladd <- stri_paste("FLAT", epc$add, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$fladd, "[, ]", "")

taba156 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon61xstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 212 saonpaon2=fladdc##################
tran <- anti_join_original_variables(tran, taba156)

tran1 <- tran[tran$propertytype == "F", ]
tran1$saonpaon2 <- stri_paste(tran1$saon, tran1$paon, sep = ", ")
tran1$saonpaon2 <- stri_replace_all_regex(tran1$saonpaon2, "[, ]", "")

epc$fladd <- stri_paste("FLAT", epc$add, sep = " ")
epc$addressfinal <- stri_replace_all_regex(epc$fladd, "[, ]", "")

taba157 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 213  saonpaonstreet5 =apadd1632##################
tran <- anti_join_original_variables(tran, taba157)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$saon, "APARTMENT "), ]

tran1$saonpaon1 <- stri_trim_both(stri_paste(tran1$saon, tran1$paon, sep = ", "))
tran1$saonpaonstreet5 <- stri_trim_both(stri_paste(tran1$saonpaon1, tran1$street, sep = " "))
tran1$saonpaonstreet5 <- stri_replace_all_regex(tran1$saonpaonstreet5, "[, ]", "")

epc$add163 <- beg2char(epc$add1, " ")
epc$apadd63 <- stri_paste("APARTMENT", epc$add163, sep = " ")

epc$addressfinal <- stri_paste(epc$apadd63, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba158 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet5" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 214 saonpaonstreet11=ADD12##################
tran <- anti_join_original_variables(tran, taba158)

tran1 <- tran[tran$propertytype == "F", ]
tran1$paon11 <- stri_replace_all_fixed(tran1$paon, ",", "")

tran1$saonpaon11 <- stri_trim_both(stri_paste(tran1$saon, tran1$paon11, sep = " "))
tran1$saonpaonstreet11 <- stri_paste(tran1$saonpaon11, tran1$street, sep = " ")
tran1$saonpaonstreet11 <- stri_replace_all_fixed(tran1$saonpaonstreet11, " ", "")

epc[epc$postcode == "M9 7HQ", "add"] <- gsub(
  "FRESHFIELDSS",
  "FRESHFIELD",
  epc[epc$postcode == "M9 7HQ", "add"]
)

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba159 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet11" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rules 215 and 216 saonpaon61xstreet=ADD1262C##################
tran <- anti_join_original_variables(tran, taba159)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d"), ]

tran1$paon61x <- beg2char(tran1$paon, ",")
tran1$paon62 <- char2end(tran1$paon, ",")

tran1$saonpaon61 <- stri_paste(tran1$saon, tran1$paon61x, sep = " ")
tran1$saonpaon61xstreet <- stri_paste(tran1$saonpaon61, tran1$street, sep = ", ")
tran1$saonpaon61xstreet <- stri_replace_all_regex(tran1$saonpaon61xstreet, "[, ]", "")

epc$add262 <- char2end(epc$add2, ",")
epc$add261 <- beg2char(epc$add2, ",")

epc$addressfinal <- stri_paste(epc$add1, epc$add262, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[, ]", "")

taba160 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon61xstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  )

# matching rule 215
taba161 <-
  taba160 |>
  filter(stri_detect_regex(paon62, "\\d-\\d")) |>
  select(transactionid, id)

# matching rule 216
taba162 <-
  taba160 |>
  filter(stri_detect_regex(add261, "\\d-\\d")) |>
  select(transactionid, id)

rm(taba160)

###############matching rule 217 flsaonpaonstreet3=ADD12C5##################
tran <- anti_join_original_variables(tran, taba161)
tran <- anti_join_original_variables(tran, taba162)

tran1 <- tran[tran$propertytype == "F", ]

tran1$flsaon3 <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaonpaon <- stri_paste(tran1$flsaon3, tran1$paon, sep = " ")
tran1$flsaonpaonstreet3 <- stri_paste(tran1$flsaonpaon, tran1$street, sep = ", ")
tran1$flsaonpaonstreet3 <- stri_replace_all_regex(tran1$flsaonpaonstreet3, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "[., ]", "")

taba163 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 218 flsaonpaonstreet3=ADD12C1##################
tran <- anti_join_original_variables(tran, taba163)

tran1 <- tran[tran$propertytype == "F", ]

tran1$flsaon3 <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaonpaon <- stri_paste(tran1$flsaon3, tran1$paon, sep = " ")
tran1$flsaonpaonstreet3 <- stri_paste(tran1$flsaonpaon, tran1$street, sep = ", ")
tran1$flsaonpaonstreet3 <- stri_replace_all_regex(tran1$flsaonpaonstreet3, "[, ]", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./, ]", "")

taba164 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 219 saonstreet1=ADD1C7##################
tran <- anti_join_original_variables(tran, taba164)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saonstreet1 <- stri_paste(tran1$saon, tran1$street, sep = " ")
tran1$saonstreet1 <- stri_replace_all_regex(tran1$saonstreet1, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add1, "[, ]", "")

taba165 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 220 saonpaonstreet1=add1f61f3##################
tran <- anti_join_original_variables(tran, taba165)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, " - "), ]

tran1 <- tran1[stri_detect_regex(tran1$paon, "^\\d"), ]
tran1$saonpaon1 <- stri_trim_both(stri_paste(tran1$saon, tran1$paon, sep = ", "))
tran1$saonpaonstreet1 <- stri_trim_both(stri_paste(tran1$saonpaon1, tran1$street, sep = ", "))
tran1$saonpaonstreet1 <- stri_replace_all_fixed(tran1$saonpaonstreet1, " ", "")

epc1 <- epc[stri_detect_fixed(epc$add, "-"), ]
epc1 <- epc1[stri_detect_fixed(epc1$add1, "FLAT "), ]

epc1$add1df <- stri_replace_all_fixed(epc1$add1, "FLAT ", "")
epc1$add1f61 <- beg2char(epc1$add1df, " ")

epc1$add1f61 <- stri_replace_all_fixed(epc1$add1f61, ",", "")
epc1$add1n <- stri_paste("FLAT", epc1$add1f61, sep = " ")
epc1$addressfinal <- stri_paste(epc1$add1n, epc1$add2, sep = ", ")
epc1$addressfinal <- stri_replace_all_fixed(epc1$addressfinal, " ", "")

taba166 <-
  inner_join(
    tran1,
    epc1,
    by = c("postcode" = "postcode", "saonpaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 221 saonpaon62steet=ADDRE##################
tran <- anti_join_original_variables(tran, taba166)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, " - "), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$paon62 <- char2end(tran1$paon, ",")
tran1$saonpaon62 <- stri_paste(tran1$saon, tran1$paon62, sep = ", ")
tran1$saonpaon62steet <- stri_paste(tran1$saonpaon62, tran1$street, sep = ", ")
tran1$saonpaon62steet <- stri_replace_all_fixed(tran1$saonpaon62steet, " ", "")

epc1 <- epc[stri_detect_fixed(epc$add, "-"), ]

epc1$addressfinal <- stri_replace_all_fixed(epc1$add, " ", "")

taba167 <-
  inner_join(
    tran1,
    epc1,
    by = c("postcode" = "postcode", "saonpaon62steet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 222 saonstreet2=ADD1264##################
tran <- anti_join_original_variables(tran, taba167)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saonstreet2 <- stri_paste(tran1$saon, tran1$street, sep = ", ")
tran1$saonstreet2 <- stri_replace_all_regex(tran1$saonstreet2, "[, ]", "")

epc1 <- epc[stri_detect_regex(epc$add, "^\\d"), ]

epc1$add2641 <- char2end(epc1$add2, ",")
epc1$addressfinal <- stri_paste(epc1$add1, epc1$add2641, sep = ", ")
epc1$addressfinal <- stri_replace_all_regex(epc1$addressfinal, "[, ]", "")

taba168 <-
  inner_join(
    tran1,
    epc1,
    by = c("postcode" = "postcode", "saonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 223 flsaonpaon61street=ADDREC##################
tran <- anti_join_original_variables(tran, taba168)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$flsaon <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$paon61 <- beg2char(tran1$paon, ",")

tran1$flsaonpaon61 <- stri_paste(tran1$flsaon, tran1$paon61, sep = " ")
tran1$flsaonpaon61street <- stri_paste(tran1$flsaonpaon61, tran1$street, sep = ", ")
tran1$flsaonpaon61street <- stri_replace_all_regex(tran1$flsaonpaon61street, "[, ]", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[, ]", "")

taba169 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon61street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 224 saon4paonstreet=ADD12##################
tran <- anti_join_original_variables(tran, taba169)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$saon, "FLAT "), ]

tran1$saon4 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "")
tran1$saon4paon <- stri_paste(tran1$saon4, tran1$paon, sep = " ")
tran1$saon4paonstreet <- stri_trim_both(stri_paste(tran1$saon4paon, tran1$street, sep = " "))
tran1$saon4paonstreet <- stri_replace_all_fixed(tran1$saon4paonstreet, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba170 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon4paonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

###############matching rule 225 saonpaon61street1=ADD1263##################
tran <- anti_join_original_variables(tran, taba170)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, "-"), ]

tran1$paon61 <- beg2char(tran1$paon, ",")

tran1$saonpaon61 <- stri_paste(tran1$saon, tran1$paon61, sep = " ")

tran1$saonpaon61street1 <- stri_trim_both(stri_paste(tran1$saonpaon61, tran1$street, sep = ", "))
tran1$saonpaon61street1 <- stri_replace_all_fixed(tran1$saonpaon61street1, " ", "")

epc$add2641 <- char2end(epc$add2, ",")
epc$addressfinal <- stri_paste(epc$add1, epc$add2641, sep = ", ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba171 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon61street1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 226 flsaonpaon2=ADDRE##################
tran <- anti_join_original_variables(tran, taba171)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]

tran1$flsaon <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaonpaon2 <- stri_paste(tran1$flsaon, tran1$paon, sep = ", ")
tran1$flsaonpaon2 <- stri_replace_all_fixed(tran1$flsaonpaon2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba172 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 227 saonpaon3=ADD1##################
tran <- anti_join_original_variables(tran, taba172)

epc[epc$postcode == "GL52 8LJ", "add"] <- gsub(
  "CHURCHFIELD",
  "CHURCHFIELDS",
  epc[epc$postcode == "GL52 8LJ", "add"]
)

tran$saonpaon1 <- stri_paste(tran$saon, tran$paon, sep = ", ")

taba173 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaon1" = "add1"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 228 saonstreet3=ADDC##################
tran <- anti_join_original_variables(tran, taba173)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saonstreet3 <- stri_paste(tran1$saon, tran1$street, sep = " ")
tran1$saonstreet3 <- stri_replace_all_fixed(tran1$saonstreet3, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba174 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonstreet3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 229  flsaonpaon3=ADD12##################
tran <- anti_join_original_variables(tran, taba174)

tran1 <- tran[tran$propertytype == "F", ]
tran1$flsaon3 <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaonpaon3 <- stri_paste(tran1$flsaon3, tran1$paon, sep = ", ")
tran1$flsaonpaon3 <- stri_replace_all_fixed(tran1$flsaonpaon3, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba175 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 230  flsaonpaonstreet4=ADD1263 ##################
tran <- anti_join_original_variables(tran, taba175)

tran1 <- tran[tran$propertytype == "F", ]
tran1$flsaon3 <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$flsaon3paon <- stri_paste(tran1$flsaon3, tran1$paon, sep = ", ")
tran1$flsaonpaonstreet4 <- stri_paste(tran1$flsaon3paon, tran1$street, sep = ", ")
tran1$flsaonpaonstreet4 <- stri_replace_all_fixed(tran1$flsaonpaonstreet4, " ", "")

epc$add2641 <- char2end(epc$add2, ",")
epc$addressfinal <- stri_paste(epc$add1, epc$add2641, sep = ", ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba176 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaonstreet4" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 231  saonstreet=ADD1265##################
tran <- anti_join_original_variables(tran, taba176)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d"), ]
tran1 <- tran1[!stri_detect_fixed(tran1$paon, ","), ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "\\d"), ]

tran1$saonstreet <- stri_paste(tran1$saon, tran1$street, sep = ", ")
tran1$saonstreet <- stri_replace_all_fixed(tran1$saonstreet, " ", "")

epc$add264 <- char2end(epc$add2, " ")
epc$addressfinal <- stri_paste(epc$add1, epc$add264, sep = ", ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba177 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(
    postcode %in%
      (epc |>
        distinct(postcode, add2) |>
        count(postcode) |>
        filter(n == 1) |>
        pull(postcode))
  ) |>
  select(transactionid, id)

###############matching rule 232 paonsaonstreet=ADDRE##################
tran <- anti_join_original_variables(tran, taba177)

tran$paonsaon <- stri_paste(tran$paon, tran$saon, sep = "")
tran$paonsaonstreet <- stri_paste(tran$paonsaon, tran$street, sep = ", ")
tran$paonsaonstreet <- stri_replace_all_fixed(tran$paonsaonstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba178 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "paonsaonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 233  saonpaon61=ADD12##################
tran <- anti_join_original_variables(tran, taba178)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, "-"), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$paon61 <- beg2char(tran1$paon, ",")
tran1$saonpaon611 <- stri_paste(tran1$saon, tran1$paon61, sep = ",")
tran1$saonpaon611 <- stri_replace_all_fixed(tran1$saonpaon611, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba179 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon611" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 234  saon7paon=ADD12##################
tran <- anti_join_original_variables(tran, taba179)

tran1 <- tran[tran$propertytype == "F", ]

tran1$paon62 <- char2end(tran1$paon, ",")
tran1$saon7 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "APARTMENT ")
tran1$saon7paon <- stri_paste(tran1$saon7, tran1$paon, sep = ", ")
tran1$saon7paon <- stri_replace_all_fixed(tran1$saon7paon, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba180 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon7paon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 235 paonsaonstreet1=ADD12##################
tran <- anti_join_original_variables(tran, taba180)

tran1 <- tran[tran$propertytype == "F", ]

tran1$paonsaon <- stri_paste(tran1$paon, tran1$saon, sep = ", ")
tran1$paonsaonstreet1 <- stri_paste(tran1$paonsaon, tran1$street, sep = ", ")
tran1$paonsaonstreet1 <- stri_replace_all_fixed(tran1$paonsaonstreet1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba181 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "paonsaonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 236 flsaonpaon61street1=ADD12#################
tran <- anti_join_original_variables(tran, taba181)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$flsaon <- stri_paste("FLAT", tran1$saon, sep = " ")
tran1$paon61 <- beg2char(tran1$paon, ",")
tran1$flsaonpaon61 <- stri_paste(tran1$flsaon, tran1$paon61, sep = " ")
tran1$flsaonpaon61street <- stri_paste(tran1$flsaonpaon61, tran1$street, sep = ", ")
tran1$flsaonpaon61street1 <- stri_replace_all_fixed(tran1$flsaonpaon61street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba182 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "flsaonpaon61street1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 237  apsaonpaon=ADD12C6############
tran <- anti_join_original_variables(tran, taba182)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]

tran1$apsaon <- stri_paste("APARTMENT", tran1$saon, sep = " ")
tran1$apsaonpaon <- stri_paste(tran1$apsaon, tran1$paon, sep = " ")
tran1$apsaonpaon <- stri_replace_all_fixed(tran1$apsaonpaon, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = " ")
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba183 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "apsaonpaon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 238 saon1paon62street=ADD12##################
tran <- anti_join_original_variables(tran, taba183)

# only for OX4 3PP
epc[epc$postcode == "OX4 3PP", "add1"] <- gsub(
  "BROADFIELD HOUSE",
  "",
  epc[epc$postcode == "OX4 3PP", "add1"]
)

tran11 <- tran[stri_detect_fixed(tran$saon, "APARTMENT "), ]

tran1 <- tran11[tran11$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$paon62 <- char2end(tran1$paon, ",")
tran1$saon1paon62 <- stri_paste(tran1$saon1, tran1$paon62, sep = ", ")
tran1$saon1paon62street <- stri_trim_both(stri_paste(tran1$saon1paon62, tran1$street, sep = ", "))
tran1$saon1paon62street <- stri_replace_all_fixed(tran1$saon1paon62street, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ",") # the original code added a space ", " then immediately removed it
epc$addressfinal <- stri_replace_all_fixed(epc$addressfinal, " ", "")

taba184 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paon62street" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 239  saonstreet=ADDC5##################
tran <- anti_join_original_variables(tran, taba184)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[!stri_detect_regex(tran1$paon, "\\d"), ]

tran1$saonstreet <- stri_paste(tran1$saon, tran1$street, sep = ", ")
tran1$saonstreet <- stri_replace_all_fixed(tran1$saonstreet, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[./ ]", "")

taba185 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 240 apsaonpaon62street1=ADDC8##################
tran <- anti_join_original_variables(tran, taba185)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$apsaon <- stri_paste("APARTMENT", tran1$saon, sep = " ")
tran1$paon62 <- char2end(tran1$paon, ",")

tran1$apsaonpaon62 <- stri_paste(tran1$apsaon, tran1$paon62, sep = ", ")
tran1$apsaonpaon62street1 <- stri_paste(tran1$apsaonpaon62, tran1$street, sep = " ")
tran1$apsaonpaon62street1 <- stri_replace_all_fixed(tran1$apsaonpaon62street1, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['. ]", "")

taba186 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "apsaonpaon62street1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 241 saonpaonstreet2=ADDRE  ##################
tran <- anti_join_original_variables(tran, taba186)

epc[epc$postcode == "BH2 6LT", "add"] <- gsub(
  "RICHMOND HILL GATE",
  "RICHMOND GATE",
  epc[epc$postcode == "BH2 6LT", "add"]
)
epc[epc$postcode == "BS8 4UJ", "add"] <- gsub(
  "LEADING EDGE",
  "THE LEADING EDGE",
  epc[epc$postcode == "BS8 4UJ", "add"]
)
epc[epc$postcode == "CV8 1FD", "add"] <- gsub(
  "OAKLAND COURT,",
  "OAKLANDS COURT,",
  epc[epc$postcode == "CV8 1FD", "add"]
)
epc[epc$postcode == "SW1P 4DA", "add"] <- gsub(
  ",WESTMINSTER GREEN,",
  "",
  epc[epc$postcode == "SW1P 4DA", "add"]
) ## delete . in soan , delete apartment

epc[epc$postcode == "E3 2UT", "add"] <- gsub(
  "PARK EAST BUILDING BOW QUARTER",
  "PARK EAST BUILDING",
  epc[epc$postcode == "E3 2UT", "add"]
)
epc[epc$postcode == "E3 2US", "add"] <- gsub(
  "PARK EAST BUILDING BOW QUARTER",
  "PARK EAST BUILDING",
  epc[epc$postcode == "E3 2US", "add"]
)
epc[epc$postcode == "E3 2UR", "add"] <- gsub(
  "PARK EAST BUILDING BOW QUARTER",
  "PARK EAST BUILDING",
  epc[epc$postcode == "E3 2UR", "add"]
)
epc[epc$postcode == "LS2 7EW", "add"] <- gsub(
  "SPARROW WHARFE",
  " ",
  epc[epc$postcode == "LS2 7EW", "add"]
)
epc[epc$postcode == "SE13 7FF", "add1"] <- gsub(
  "B",
  "FLAT ",
  epc[epc$postcode == "SE13 7FF", "add1"]
)
epc[epc$postcode == "SE13 7FG", "add1"] <- gsub(
  "B",
  "FLAT ",
  epc[epc$postcode == "SE13 7FG", "add1"]
)

tran[tran$postcode == "PR9 0AU", "saon"] <- gsub(
  "THE APARTMENTS",
  "",
  tran[tran$postcode == "PR9 0AU", "saon"]
)

tran[tran$postcode == "B72 1AY", "paon"] <- gsub(
  "",
  "",
  tran[tran$postcode == "B72 1AY", "paon"]
) # this doesn't do anything!?

tran$saonpaon1 <- stri_trim_both(stri_paste(tran$saon, tran$paon, sep = " "))
tran$saonpaonstreet2 <- stri_trim_both(stri_paste(tran$saonpaon1, tran$street, sep = ", "))
tran$saonpaonstreet2 <- stri_replace_all_fixed(tran$saonpaonstreet2, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba187 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 242   saon2paonstreet1=ADDC9#########################
tran <- anti_join_original_variables(tran, taba187)

epc[epc$postcode == "B16 8EQ", "add"] <- gsub(
  "THE PURPLE APARTMENTS BROADWAY PLAZA",
  "BROADWAY PLAZA",
  epc[epc$postcode == "B16 8EQ", "add"]
)
epc[epc$postcode == "B16 8SU", "add"] <- gsub(
  "THE PURPLE APARTMENTS BROADWAY PLAZA",
  "BROADWAY PLAZA",
  epc[epc$postcode == "B16 8SU", "add"]
)
epc[epc$postcode == "B16 8SU", "add"] <- gsub(
  "THE RED APARTMENTS BROADWAY PLAZA",
  "BROADWAY PLAZA",
  epc[epc$postcode == "B16 8SU", "add"]
)
epc[epc$postcode == "AL1 1BH", "add"] <- gsub(
  "ALBANY GATE",
  "ALBENY GATE",
  epc[epc$postcode == "AL1 1BH", "add"]
)

epc[epc$postcode == "BN2 9SY", "add"] <- gsub(
  "OLD COLLEGE HOUSE",
  "",
  epc[epc$postcode == "BN2 9SY", "add"]
)

tran1 <- tran[tran$propertytype == "F", ]

tran1$paon61 <- beg2char(tran1$paon, ",")
tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saon2paon <- stri_paste(tran1$saon2, tran1$paon, sep = " ")
tran1$saon2paonstreet1 <- stri_paste(tran1$saon2paon, tran1$street, sep = ", ")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./]", "")

taba188 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreet1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 243  apsaonpaonstreet2=ADD1262cc##################
tran <- anti_join_original_variables(tran, taba188)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_regex(tran1$saon, "^\\d"), ]

tran1$apsaon <- stri_paste("APARTMENT ", tran1$saon, sep = "")

tran1$apsaonpaon <- stri_paste(tran1$apsaon, tran1$paon, sep = " ")
tran1$apsaonpaonstreet2 <- stri_paste(tran1$apsaonpaon, tran1$street, sep = ", ")
tran1$apsaonpaonstreet2 <- stri_replace_all_fixed(tran1$apsaonpaonstreet2, " ", "")

epc1 <- epc[stri_detect_fixed(epc$add2, ","), ]

epc1$add262 <- char2end(epc1$add2, ",")
epc1$addressfinal <- stri_paste(epc1$add1, epc1$add262, sep = ", ")
epc1$addressfinal <- stri_replace_all_regex(epc1$addressfinal, "[' ]", "")

taba189 <-
  inner_join(
    tran1,
    epc1,
    by = c("postcode" = "postcode", "apsaonpaonstreet2" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 244 psaonpstreet=ADDRE##################
tran <- anti_join_original_variables(tran, taba189)

tran1 <- tran[tran$propertytype == "F", ]
tran1 <- tran1[stri_detect_fixed(tran1$paon, ","), ]

tran1$paon61 <- beg2char(tran1$paon, ",")

tran1$paon6164 <- numextract(tran1$paon61)
tran1$paon6163 <- stri_replace_all_fixed(tran1$paon61, "\\d+", "")

tran1$paon62 <- char2end(tran1$paon, ",")

tran1$paon6164saon <- stri_paste(tran1$paon6164, tran1$saon, sep = "")
tran1$psaonp <- stri_paste(tran1$paon6164saon, tran1$paon6163, sep = " ")
tran1$psaonpp <- stri_paste(tran1$psaonp, tran1$paon62, sep = ",")
tran1$psaonpstreet <- stri_paste(tran1$psaonpp, tran1$street, sep = ", ")
tran1$psaonpstreet <- stri_replace_all_fixed(tran1$psaonpstreet, " ", "")

epc$addressfinal <- stri_replace_all_fixed(epc$add, " ", "")

taba190 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "psaonpstreet" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 245 saonpaon65street1=ADD12C#####################
tran <- anti_join_original_variables(tran, taba190)

tran1 <- tran[stri_detect_fixed(tran$paon, ","), ]

tran1$paon5 <- word(tran1$paon, -1)
tran1$saonpaon5 <- stri_paste(tran1$saon, tran1$paon5, sep = ", ")
tran1$saonpaon5street1 <- stri_paste(tran1$saonpaon5, tran1$street, sep = " ")
tran1$saonpaon5street1 <- stri_replace_all_fixed(tran1$saonpaon5street1, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba191 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saonpaon5street1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 246 saon2paonstreetn3=ADDC#####################
tran <- anti_join_original_variables(tran, taba191)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saon2 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "")
tran1$saonn2 <- stri_replace_all_fixed(tran1$saon2, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon2paonn1 <- stri_trim_both(stri_paste(tran1$saonn2, tran1$paonn, sep = " "))
tran1$saon2paonstreetn3 <- stri_trim_both(stri_paste(tran1$saon2paonn1, tran1$streetn, sep = " "))
tran1$saon2paonstreetn3 <- stri_replace_all_fixed(tran1$saon2paonstreetn3, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba192 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon2paonstreetn3" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

rm(tran1)

###############matching rule 247 saonpaonn=ADD12C######################
tran <- anti_join_original_variables(tran, taba192)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$saonpaonn <- stri_paste(tran$saonn, tran$paonn, sep = ", ")
tran$saonpaonn <- stri_replace_all_fixed(tran$saonpaonn, " ", "")

epc$addressfinal <- stri_paste(epc$add1, epc$add2, sep = ", ")
epc$addressfinal <- stri_replace_all_regex(epc$addressfinal, "['./ ]", "")

taba193 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 248  saon1paonstreetn1=ADDC###########################
tran <- anti_join_original_variables(tran, taba193)

tran$saon1 <- stri_replace_all_fixed(tran$saon, "APARTMENT ", "FLAT ")
tran$saonn1 <- stri_replace_all_fixed(tran$saon1, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$saon1paonn <- stri_paste(tran$saonn1, tran$paonn, sep = ", ")
tran$saon1paonstreetn1 <- stri_trim_both(stri_paste(tran$saon1paonn, tran$streetn, sep = ", "))
tran$saon1paonstreetn1 <- stri_replace_all_fixed(
  tran$saon1paonstreetn1,
  " ",
  ""
)

epc$addressfinal <- stri_replace_all_regex(epc$add, "['./ ]", "")

taba194 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 249  saon4paonstreetn1=ADDC4#################
tran <- anti_join_original_variables(tran, taba194)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saon4 <- stri_replace_all_fixed(tran1$saon, "FLAT ", "")
tran1 <- tran1[!stri_detect_regex(tran1$paon, "^\\d"), ]

tran1$saonn4 <- stri_replace_all_fixed(tran1$saon4, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon4paonn1 <- stri_paste(tran1$saonn4, tran1$paonn, sep = " ")
tran1$saon4paonstreetn1 <- stri_trim_both(stri_paste(tran1$saon4paonn1, tran1$streetn, sep = ", "))
tran1$saon4paonstreetn1 <- stri_replace_all_fixed(tran1$saon4paonstreetn1, " ", "")

epc1 <- epc[!stri_detect_regex(epc$add, "\\d+-\\d+"), ]
epc1$add <- stri_trim_both(epc1$add)

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba195 <-
  inner_join(
    tran1,
    epc1,
    by = c("postcode" = "postcode", "saon4paonstreetn1" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  filter(propertytype.y %in% c("Flat", "Maisonette")) |>
  select(transactionid, id)

rm(epc1)
rm(tran1)

###############matching rule 250 saon1paonstreetn=ADDC4############################
tran <- anti_join_original_variables(tran, taba195)

tran1 <- tran[tran$propertytype == "F", ]

tran1$saon1 <- stri_replace_all_fixed(tran1$saon, "APARTMENT ", "FLAT ")
tran1$saonn1 <- stri_replace_all_fixed(tran1$saon1, "/", "")
tran1$paonn <- stri_replace_all_regex(tran1$paon, "['.]", "")
tran1$streetn <- stri_replace_all_fixed(tran1$street, "'", "")
tran1$saon1paonn <- stri_paste(tran1$saonn1, tran1$paonn, sep = " ")
tran1$saon1paonstreetn <- stri_trim_both(stri_paste(tran1$saon1paonn, tran1$streetn, sep = ", "))
tran1$saon1paonstreetn <- stri_replace_all_fixed(tran1$saon1paonstreetn, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba196 <-
  inner_join(
    tran1,
    epc,
    by = c("postcode" = "postcode", "saon1paonstreetn" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

###############matching rule 251 saonpaonlon=ADDC4 #########################
tran <- anti_join_original_variables(tran, taba196)

tran$saonn <- stri_replace_all_fixed(tran$saon, "/", "")
tran$paonn <- stri_replace_all_regex(tran$paon, "['.]", "")
tran$streetn <- stri_replace_all_fixed(tran$street, "'", "")
tran$localityn <- stri_replace_all_regex(tran$locality, "['.]", "")
tran$saonpaonn1 <- stri_paste(tran$saonn, tran$paonn, sep = " ")
tran$saonpaonlon <- stri_paste(tran$saonpaonn1, tran$localityn, sep = ", ")
tran$saonpaonlon <- stri_replace_all_fixed(tran$saonpaonlon, " ", "")

epc$addressfinal <- stri_replace_all_regex(epc$add, "[-./ ]", "")

taba197 <-
  inner_join(
    tran,
    epc,
    by = c("postcode" = "postcode", "saonpaonlon" = "addressfinal"),
    na_matches = "never",
    relationship = "many-to-many"
  ) |>
  select(transactionid, id)

tran <- anti_join_original_variables(tran, taba197)

###############combine the result from stage 4 ################

# Create a list
taba_list <- mget(ls(pattern = "^taba"))

#casa5 is the linked result through stage4
casa5 <- reduce(taba_list, union)

# Tidy up
rm(list = setdiff(ls(), c("con", "casa1", "casa2", "casa3", "casa4", "casa5", "tran")))

# Save casa5 in duckDB as casabin5
dbWriteTable(con, "casabin5", casa5, overwrite = TRUE, row.names = FALSE)

# Save the unmatched tran
dbWriteTable(con, "tran3binleft", tran, overwrite = TRUE, row.names = FALSE)

rm(tran)

# ------------------combine the results from four stages------------------------------

casa <- reduce(list(casa1, casa2, casa3, casa4, casa5), union)

# casa is the final linked result
dbWriteTable(con, "casa", casa, overwrite = TRUE, row.names = FALSE)

rm(casa1, casa2, casa3, casa4, casa5, casa)

# ------------------data linkage ending----------------------------

# ==================end ==================

dbDisconnect(con)
rm(con)
