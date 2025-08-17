# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)
library(data.table)
library(DBI)
library(duckdb)
library(GGally)

options(scipen = 100)

# Get data
con <-
  dbConnect(
    duckdb::duckdb(),
    dbdir = "Data/datajournal.duckdb"
  )

tran <- dbGetQuery(con, "select * from transactions")
epc <- dbGetQuery(con, "select * from epc")
casa <- dbGetQuery(con, "select * from casa")

dbDisconnect(con)
rm(con)

# Join data and set keys & indices
tran_casa_epc <-
  tran |>
  inner_join(casa, by = "transactionid") |>
  inner_join(epc, by = "id") |>
  distinct()

rm(tran, epc, casa)

setDT(tran_casa_epc)

tran_casa_epc[, dateoftransfer := as.Date(dateoftransfer)]

setkey(tran_casa_epc, transactionid)
setindex(tran_casa_epc, inspectiondate, lodgementdate, dateoftransfer)

# Pre-compute absolute differences
tran_casa_epc[,
  abs_inspection_diff := abs(as.numeric(difftime(
    inspectiondate,
    dateoftransfer,
    units = "days"
  )))
]

tran_casa_epc[,
  abs_lodgement_diff := abs(as.numeric(difftime(
    lodgementdate,
    dateoftransfer,
    units = "days"
  )))
]

# Unique matches
unique_idx <- tran_casa_epc[, .I[.N == 1], by = transactionid]$V1

# Multiple matches
multi_idx <- tran_casa_epc[, .I[.N > 1], by = transactionid]$transactionid

# Tie-breaking logic
resolve_ties <- function(dt) {
  idx <- which(dt$abs_inspection_diff == min(dt$abs_inspection_diff))
  if (length(idx) == 1) return(idx)

  dt <- dt[idx]
  idx2 <- which(dt$abs_lodgement_diff == min(dt$abs_lodgement_diff))
  if (length(idx2) == 1) return(idx[idx2])

  dt <- dt[idx2]
  if (all(!is.na(dt$numberrooms))) {
    return(idx[idx2[which.max(dt$numberrooms)]])
  }

  return(idx[idx2[which.max(dt$tfarea)]])
}

# inspectiondate before dateoftransfer
before_idx <- tran_casa_epc[
  transactionid %in%
    multi_idx &
    !is.na(tfarea) &
    tfarea > 0 &
    inspectiondate <= dateoftransfer,
  .I[resolve_ties(.SD)],
  by = transactionid
]$V1

# inspectiondate after dateoftransfer
after_idx <- tran_casa_epc[
  transactionid %in%
    multi_idx &
    !is.na(tfarea) &
    tfarea > 0 &
    inspectiondate > dateoftransfer &
    !(transactionid %in% tran_casa_epc[before_idx, transactionid]),
  .I[resolve_ties(.SD)],
  by = transactionid
]$V1

# Combine all selected indices
final_idx <- c(unique_idx, before_idx, after_idx)

# Final result
ppd <- tran_casa_epc[final_idx]

# Clean up
rm(list = setdiff(ls(), c("ppd")))

# Create additional fields
ppd[, `:=`(
  priceper = price / tfarea,
  floorper = tfarea / numberrooms
)]

# Pair correlations
ppd |>
  select(numberrooms, floorper, priceper, tfarea) |>
  slice_sample(n = 100000) |>
  ggpairs()

# Data quality filters, plot again to see the result
ppd <-
  ppd |>
  filter(
    !((!is.na(numberrooms) & numberrooms > 20) |
      (!is.na(tfarea) & tfarea < 10) |
      (!is.na(tfarea) & tfarea > 1000) |
      (!is.na(priceper) & priceper > 60000) |
      (!is.na(floorper) & floorper < 5) |
      (!is.na(floorper) & floorper > 250))
  )

# Tidy names
setnames(ppd, "dateoftransfer", "Date of Transfer")
setnames(ppd, "postcode.x", "Postcode")
setnames(ppd, "county", "County")
setnames(ppd, "district", "District")
setnames(ppd, "price", "Price")
setnames(ppd, "categorytype", "PPD Category Type")

# Select only the required fields
ppd <-
  ppd[
    order(Postcode),
    .(
      `Date of Transfer`,
      Postcode,
      County,
      District,
      Price,
      tfarea,
      priceper,
      `PPD Category Type`
    )
  ]

# Write csv
ppd |>
  write_csv(
    "Data/pp-psqm.csv"
  )
