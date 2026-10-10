# ------------------------------------------------
# PPD EPC price per sqm.R, based on PPD_EPC_linkage.R
# ------------------------------------------------
# Code provided as is and can be used or modified freely. This code only allows to used for academic purpose.
# ------------------------------------------------
# Author: Bin Chi, Adam Dennett
# UCL Centre for Advanced Spatial Analysis
# bin.chi.16@ucl.ac.uk; a.dennett@ucl.ac.uk
# Date: 20/12/2019

# Re-factored for 18x faster speed, by Chris Beech; summer 2025, published 2025-08-17
# https://github.com/chris-beech-housing/Land-Registry-PPD-EPC-price-per-square-metre

# DuckDB version, October 2026, Claude Code: the whole pipeline in one script, in three parts.
#   Part 1, load: the EPC certificates and the price paid data are read and tidied, and written to the
#     transactions and epc tables
#   Part 2, link: Bin's 250 matching rules, then stage 5, the unit signature rule, match transactions to
#     EPCs and write the casa and casa_rules tables
#   Part 3, final PPD: one EPC is chosen for each matched transaction, and pp-psqm.csv and the match
#     percentage graph are written
# Each part has its own connection to the database, closed at its end, so its temporary tables are
# freed and its settings apply to it alone.

# -------------------------------------------------

# Re-start R session to ensure only the necessary libraries are attached
library(tidyverse)
library(DBI)
library(duckdb)
library(GGally)

options(scipen = 100)

setwd("/Users/chrisbeech/Documents/UK housing analysis")

## All EPC certificates-YYYY.csv files, from https://get-energy-performance-data.communities.gov.uk/
epc_dir <- "Data/domestic-csv"

## England and Wales price paid data: https://www.gov.uk/guidance/about-the-price-paid-data
ppd_path <- "Data/pp-complete.csv"
last_date <- "2026-09-01" # transfers before this date: pp-complete.csv runs to August 2026

## Bin Chi's corrections for addresses written differently in the EPCs and the Land Registry PPD
rulechi_path <- "Data/rulechi.csv"

# Stage 5, the unit signature rule, runs after the 250 rules; FALSE leaves it out
use_unit_signature_rule <- TRUE

db_path <- "Data/datajournal.duckdb"

## Outputs
psqm_path <- "Data/pp-psqm.csv"
graph_path <- "Graphs/Main analysis/EPC match percentage.png"

t_start <- Sys.time()

# ------------------------------------------------
# Helpers
# ------------------------------------------------

# Part 2's matching rules each run as one SQL statement inside DuckDB, which uses all CPU cores. The
# rules, their order, their numbers and the tabaN names are those of the retired
# "3. PPD_EPC_linkage fast.R", so the two can be compared rule by rule. casa differs from that file's
# only by the corrections to char2end() (see create_macros()) and to rules 244 and 249, and by
# stage 5: the unit signature rule, which matches some of the transactions the 250 rules leave unmatched
# (see unit_signature_rule(); use_unit_signature_rule <- FALSE leaves it out).
#
# How the R code maps to a rule() call:
#   tran = c(...)  the transaction columns the R code built for the join, one per line, in order; a
#                  later step can use an earlier one by name (DuckDB lateral column aliases)
#   epc = c(...)   the same for the EPC side
#   by = c(x = "y") the join columns (postcode is always joined too, unless by_postcode = FALSE)
#   tran_where / epc_where  the row subsets (tran221, tran1, epc1, ...); where = the post-join filter()
#   drop_matched() is anti_join_original_variables()
#
# stringi/R -> SQL:
#   stri_paste(a, b, sep = s)                a || s || b
#   stri_replace_all_fixed(x, p, r)          replace(x, p, r)
#   stri_replace_all_regex(x, "[abc]", "")   translate(x, 'abc', '')   (deletes each listed character)
#   stri_trim_both(x)                        trim_ws(x)
#   beg2char(x, c), char2end(x, c)           beg2char(x, c), char2end(x, c)
#   stri_extract_first/last_regex(x, "\\S+") first_token(x), last_token(x)
#   numextract(x)                            numextract(x)
#   stri_detect_fixed(x, p)                  contains(x, p)
#   stri_detect_regex(x, p)                  regexp_matches(x, icu(p))
# icu() marks a pattern written with ICU's \d \D \s \S \w, as in stringi; run_sql() rewrites them to
# the RE2 equivalents before DuckDB sees the SQL. The SQL helpers keep R's NA behaviour: NULL never
# joins.
#
# Troubleshooting: casa_rules records which rule produced each pair, and peek_rule() shows a rule's
# transaction and EPC keys for one postcode, side by side.

# ICU's White_Space set, which stringi uses for \s and stri_trim_both(); RE2's \s is ASCII only
WS <- paste0(
  "\\t\\n\\x0B\\f\\r \\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}",
  "\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}"
)

ICU_TO_RE2 <- c(
  "\\s" = paste0("[", WS, "]"),
  "\\S" = paste0("[^", WS, "]"),
  "\\d" = "\\p{Nd}",
  "\\D" = "\\P{Nd}",
  "\\w" = "[\\p{L}\\p{Nl}\\p{M}\\p{Nd}\\p{Pc}\\x{200C}\\x{200D}]"
)

# The SQL equivalents of the R helpers and stringi calls used by the rules. beg2char() and char2end()
# behave as qdap's, which Bin Chi's code uses: when the character is absent both return the text
# unchanged, so char2end("64", ",") is "64". The stringi version's char2end() returned "" instead, which
# dropped the house number from rules such as 124 and 129 when the PAON had no comma.
create_macros <- function(conn) {
  macros <- c(
    sprintf("trim_ws(x) AS regexp_replace(x, '^[%s]+|[%s]+$', '', 'g')", WS, WS),
    "beg2char(x, c) AS split_part(x, c, 1)",
    paste(
      "char2end(x, c) AS CASE WHEN x IS NULL THEN NULL",
      "WHEN contains(x, c) THEN substr(x, strpos(x, c) + length(c)) ELSE x END"
    ),
    sprintf("first_token(x) AS nullif(regexp_extract(x, '[^%s]+', 0), '')", WS),
    sprintf("last_token(x) AS nullif(regexp_extract(x, '([^%s]+)[%s]*$', 1), '')", WS, WS),
    "numextract(x) AS nullif(regexp_extract(x, '-*\\p{Nd}+\\.*\\p{Nd}*', 0), '')"
  )
  for (m in macros) dbExecute(conn, paste("CREATE OR REPLACE TEMP MACRO", m))
}

# Rewrite icu('...') patterns to RE2 literals
icu_to_re2 <- function(sql) {
  m <- gregexpr("icu\\('((?:[^']|'')*)'\\)", sql, perl = TRUE)
  regmatches(sql, m) <- lapply(regmatches(sql, m), function(hits) {
    vapply(hits, function(h) {
      p <- sub("^icu\\('(.*)'\\)$", "\\1", h)
      for (k in names(ICU_TO_RE2)) p <- gsub(k, ICU_TO_RE2[[k]], p, fixed = TRUE)
      paste0("'", p, "'")
    }, "")
  })
  sql
}

run_sql <- function(sql, ...) invisible(dbExecute(con, icu_to_re2(sql), ...))

sq <- function(s) paste0("'", gsub("'", "''", s, fixed = TRUE), "'")

cur <- new.env()
rule_defs <- list()

start_stage <- function(stage) {
  cur$stage <- stage
  cur$tran <- paste0("s", stage, "_tran")
  cur$epc <- paste0("s", stage, "_epc")
  cur$t0 <- Sys.time()
}

end_stage <- function(stage) {
  n <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM matches WHERE stage = %s", sq(stage)))$n
  message(sprintf("stage %s: %s pairs in %.0fs", stage, format(n, big.mark = ","),
                  as.numeric(difftime(Sys.time(), cur$t0, units = "secs"))))
}

# One matching rule: the inner_join() |> filter() |> select(transactionid, id) of the R version
rule <- function(name, tran = character(), epc = character(), by, tran_from = cur$tran,
                 tran_where = NULL, epc_from = cur$epc, epc_where = NULL, where = NULL,
                 by_postcode = TRUE) {
  def <- list(
    name = name, stage = cur$stage, tran = tran, epc = epc, by = by, tran_from = tran_from,
    tran_where = tran_where, epc_from = epc_from, epc_where = epc_where, where = where,
    by_postcode = by_postcode
  )
  rule_defs[[paste(cur$stage, name)]] <<- def
  ids <- paste0("t.transactionid, e.id, ", sq(cur$stage), ", ", sq(name))
  run_sql(paste0("INSERT INTO matches ", rule_select(def, "DISTINCT ", ids)))
}

rule_side <- function(from, steps, where, cols = "*") {
  sel <- paste(c(cols, if (length(steps)) paste(steps, "AS", names(steps))), collapse = ", ")
  if (length(where) > 1) where <- paste0("(", where, ")")
  where <- if (length(where)) paste0(" WHERE ", paste(where, collapse = " AND "))
  paste0("SELECT ", sel, " FROM ", from, where)
}

rule_select <- function(def, ..., tran_from = def$tran_from, epc_from = def$epc_from, extra = NULL,
                        tran_cols = "*", epc_cols = "*") {
  on <- c(if (def$by_postcode) "t.postcode = e.postcode", paste0("t.", names(def$by), " = e.", def$by))
  tran <- rule_side(tran_from, def$tran, c(def$tran_where, extra$tran), tran_cols)
  epc <- rule_side(epc_from, def$epc, c(def$epc_where, extra$epc), epc_cols)
  paste0(
    "SELECT ", ..., " FROM (", tran, ") t JOIN (", epc, ") e ON ",
    paste(on, collapse = " AND "),
    if (length(def$where)) paste0(" WHERE ", def$where)
  )
}

# anti_join_original_variables(): remove the transactions a rule matched
drop_matched <- function(table, name) {
  run_sql(sprintf(
    paste(
      "DELETE FROM %s WHERE transactionid IN",
      "(SELECT transactionid FROM matches WHERE stage = %s AND rule = %s)"
    ),
    table, sq(cur$stage), sq(name)
  ))
}

make_table <- function(name, from, where = NULL) {
  run_sql(sprintf("CREATE OR REPLACE TEMP TABLE %s AS SELECT * FROM %s%s", name, from,
                  if (!is.null(where)) paste(" WHERE", where) else ""))
}

load_table <- function(name, sql) run_sql(sprintf("CREATE OR REPLACE TEMP TABLE %s AS %s", name, sql))

write_table <- function(name, from, where = NULL) {
  run_sql(sprintf("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s%s", name, from,
                  if (!is.null(where)) paste(" WHERE", where) else ""))
}

drop_tables <- function(...) for (t in c(...)) run_sql(sprintf("DROP TABLE IF EXISTS %s", t))

update_table <- function(table, ...) {
  sets <- list(...)
  for (col in names(sets)) run_sql(sprintf("UPDATE %s SET %s = %s", table, col, sets[[col]]))
}

# table[table$postcode == postcode, col] <- gsub(pattern, replacement, ...); the patterns are
# literal text apart from '.', so regexp_replace() ('s': '.' also matches a newline, as in R) gives
# the same result
fix <- function(table, postcode, col, pattern, replacement) {
  run_sql(
    sprintf("UPDATE %s SET %s = regexp_replace(%s, ?, ?, 'gs') WHERE postcode = ?", table, col, col),
    params = list(pattern, replacement, postcode)
  )
}

# The rulechi.csv corrections. Every row is applied, each once to the original EPC rows it matches, and
# the corrected rows are added beside the originals rather than replacing them. Where the Land Registry
# uses two spellings of a street (e.g. WOODLAND PARK and WOODLANDS PARK in BB7 9UG) rulechi.csv has a
# row for each, and both spellings can then match. Bin Chi's loop applied only the first row of each
# postcode, in place, so a second row for the same postcode was never used. Duplicate rows of
# rulechi.csv, and corrected rows identical to an existing row, are left out. ('s': '.' also matches a
# newline, as in R.)
apply_rulechi <- function(table, rules) {
  dbWriteTable(con, "rule_rows", unique(rules[, c("postcode", "add1", "street")]), temporary = TRUE,
               overwrite = TRUE)
  run_sql(sprintf(
    "
    INSERT INTO %s
    SELECT DISTINCT e.* REPLACE (regexp_replace(e.add1, r.add1, r.street, 'gs') AS add1)
    FROM %s e JOIN rule_rows r ON e.postcode = r.postcode
    WHERE regexp_matches(e.add1, r.add1, 's')
    EXCEPT
    SELECT * FROM %s
    ",
    table, table, table
  ))
}

# casa is stored in a fixed order: when several EPCs tie on every criterion in part 3, the lowest id is
# taken, as the retired 4. Create final PPD.R does
write_casa <- function() {
  run_sql("SET preserve_insertion_order = true")
  run_sql(paste(
    "CREATE OR REPLACE TABLE casa AS",
    "SELECT DISTINCT transactionid, id FROM matches ORDER BY transactionid, id"
  ))
  run_sql(paste(
    "CREATE OR REPLACE TABLE casa_rules AS",
    "SELECT DISTINCT * FROM matches ORDER BY stage, rule, transactionid, id"
  ))
}

# Stage 5, the unit signature rule (October 2026, Claude Code), for transactions stages 1-4 leave
# unmatched. The unit signature of an address is its unit numbers (12, 12A, G2, B206), single letters and
# position words (GROUND, FIRST, TOP, BASEMENT, REAR, PLOT, ANNEX ...), in order. Addresses are cleaned
# first: full stops and apostrophes dropped (ST. ANN'S -> ST ANNS), 12/A read as 12A, other punctuation
# as spaces. A transaction address is matched to an EPC address in the same postcode when, together:
#   - the SAON and PAON have the same unit signature as the EPC address, read without the street and
#     locality (so a street such as LOWER LANE adds nothing)
#   - the street appears in the EPC address. Where the street is empty, the street of another sale of
#     the same postcode, SAON and PAON is used (e.g. 1995-2003 records with the street empty and the
#     locality FOXGROVE ROAD); with no such sale the address is left unmatched
#   - every other word of the SAON and PAON (building and house names) appears in the EPC address,
#     outside its street and locality
#   - the property types agree (flat with Flat/Maisonette, house with House/Bungalow/Park home)
#   - the EPC address is not already matched by stages 1-4 to a different property (a different SAON or
#     PAON); a match to another sale of the same property, e.g. one whose street is recorded, is fine
#   - exactly one EPC address in the postcode passes these checks
# The transaction is then paired with every certificate at that EPC address, as the other rules do. On
# the addresses stages 1-4 match, the same checks pick the same EPC address 99.95% of the time.
unit_signature_rule <- function() {
  run_sql("CREATE OR REPLACE TEMP MACRO norm(s) AS regexp_replace(upper(s), '[^A-Z0-9]', '', 'g')")
  run_sql("
    CREATE OR REPLACE TEMP MACRO clean(s) AS trim(regexp_replace(regexp_replace(regexp_replace(
      regexp_replace(upper(s), '([0-9]+)/([A-Z])\\b', '\\1\\2', 'g'),
      '[.''`]', '', 'g'), '[^A-Z0-9]', ' ', 'g'), ' +', ' ', 'g'))
  ")
  run_sql("
    CREATE OR REPLACE TEMP MACRO osig(s) AS list_transform(
      regexp_extract_all(
        upper(s),
        '\\b(?:[A-Z]?[0-9]+[A-Z]?|[A-Z]|BASEMENT|LOWER|GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|TOP|' ||
        'UPPER|MIDDLE|HALL|ATTIC|GARDEN|REAR|FRONT|PLOT|ANNEX|ANNEXE)\\b'
      ),
      w -> CASE w WHEN 'UPPER' THEN 'TOP' WHEN 'ANNEXE' THEN 'ANNEX' ELSE w END
    )
  ")
  # the unit numbers of a signature, in order
  run_sql("
    CREATE OR REPLACE TEMP MACRO nums(s) AS list_filter(osig(s), w -> regexp_matches(w, '[0-9]'))
  ")
  # a cleaned address without the last occurrence of the street and of the locality, as whole words, so a
  # house name such as LOWER LANE COTTAGE keeps its words when the street is LOWER LANE
  run_sql("
    CREATE OR REPLACE TEMP MACRO cut_last(x, w) AS
      CASE
        WHEN w = '  ' OR strpos(x, w) = 0 THEN x
        ELSE left(x, length(x) - strpos(reverse(x), reverse(w)) - length(w) + 1) || ' ' ||
          substr(x, length(x) - strpos(reverse(x), reverse(w)) + 2)
      END
  ")
  run_sql("
    CREATE OR REPLACE TEMP MACRO strip(a, street, locality) AS
      cut_last(cut_last(' ' || a || ' ', ' ' || clean(street) || ' '), ' ' || clean(locality) || ' ')
  ")
  # the other words of the SAON and PAON
  run_sql("
    CREATE OR REPLACE TEMP MACRO names(s) AS list_filter(
      regexp_extract_all(upper(s), '\\b[A-Z]{2,}\\b'),
      w -> w NOT IN (
        'FLAT', 'FLATS', 'APARTMENT', 'APT', 'UNIT', 'ROOM', 'STUDIO', 'MAISONETTE', 'FLOOR', 'AND',
        'AT', 'THE', 'OF', 'NO', 'BASEMENT', 'LOWER', 'GROUND', 'FIRST', 'SECOND', 'THIRD', 'FOURTH',
        'FIFTH', 'TOP', 'UPPER', 'MIDDLE', 'HALL', 'ATTIC', 'GARDEN', 'REAR', 'FRONT', 'PLOT', 'ANNEX',
        'ANNEXE'
      )
    )
  ")
  run_sql("
    CREATE OR REPLACE TEMP MACRO type_ok(t, e) AS
      (t = 'F' AND e IN ('Flat', 'Maisonette'))
      OR (t IN ('D', 'S', 'T') AND e IN ('House', 'Bungalow', 'Park home'))
  ")

  # EPC addresses, with their unit numbers as text for the join, and the property (SAON | PAON) that
  # stages 1-4 matched each to, if any ('' when matched to more than one)
  run_sql("
    CREATE OR REPLACE TEMP TABLE us_epc_owner AS
    SELECT e.postcode, norm(e.add) AS eadd,
           CASE
             WHEN count(DISTINCT t.saon || ' | ' || t.paon) = 1 THEN any_value(t.saon || ' | ' || t.paon)
             ELSE ''
           END AS owner
    FROM matches m JOIN transactions t USING (transactionid) JOIN epc e USING (id)
    GROUP BY e.postcode, norm(e.add)
  ")
  run_sql("
    CREATE OR REPLACE TEMP TABLE us_epc AS
    SELECT e.postcode, clean(e.add) AS cadd, norm(e.add) AS eadd,
           array_to_string(nums(clean(e.add)), ' ') AS enums,
           list_distinct(list(e.propertytype)) AS etypes, any_value(o.owner) AS owner
    FROM epc e
    LEFT JOIN us_epc_owner o ON o.postcode = e.postcode AND o.eadd = norm(e.add)
    GROUP BY e.postcode, clean(e.add), norm(e.add)
  ")

  # The street of another sale of the same property, for records whose street is empty. The join below
  # is on postcode, SAON and PAON only: a condition on the street inside it makes DuckDB use a nested
  # loop
  run_sql("
    CREATE OR REPLACE TEMP TABLE us_street AS
    SELECT postcode, saon, paon, first(street ORDER BY yearchi DESC, street) AS street
    FROM transactions WHERE street <> ''
    GROUP BY postcode, saon, paon
  ")

  # Unmatched transaction addresses
  run_sql("
    CREATE OR REPLACE TEMP TABLE us_unmatched AS
    SELECT
      a.*,
      coalesce(nullif(a.street, ''), s.street, '') AS street_used,
      osig(clean(a.saon || ' ' || a.paon)) AS psig,
      array_to_string(nums(clean(a.saon || ' ' || a.paon)), ' ') AS pnums,
      names(clean(a.saon || ' ' || a.paon)) AS pnames
    FROM (
      SELECT
        t.postcode, t.propertytype, t.paon, t.saon, t.street,
        first(t.locality ORDER BY t.yearchi DESC, t.locality) AS locality, list(t.transactionid) AS tids
      FROM transactions t
      ANTI JOIN matches m ON m.transactionid = t.transactionid
      GROUP BY t.postcode, t.propertytype, t.paon, t.saon, t.street
    ) a
    LEFT JOIN us_street s ON s.postcode = a.postcode AND s.saon = a.saon AND s.paon = a.paon
  ")

  # The EPC addresses that pass the checks, for each unmatched transaction address
  run_sql("
    CREATE OR REPLACE TEMP TABLE us_candidates AS
    SELECT t.postcode, t.propertytype, t.paon, t.saon, t.street,
           list_sort(list_distinct(list(e.eadd))) AS eadds
    FROM us_unmatched t
    JOIN us_epc e ON e.postcode = t.postcode AND e.enums = t.pnums
    WHERE len(t.psig) > 0
      AND (e.owner IS NULL OR e.owner = t.saon || ' | ' || t.paon)
      AND t.street_used <> ''
      AND contains(e.eadd, norm(t.street_used))
      AND osig(strip(e.cadd, t.street_used, t.locality)) = t.psig
      AND list_bool_and(list_transform(
        t.pnames, w -> contains(norm(strip(e.cadd, t.street_used, t.locality)), w)
      )) IS NOT FALSE
      AND list_bool_or(list_transform(e.etypes, et -> type_ok(t.propertytype, et)))
    GROUP BY t.postcode, t.propertytype, t.paon, t.saon, t.street
  ")

  # Each matched transaction with every certificate at its EPC address
  run_sql("
    INSERT INTO matches
    SELECT DISTINCT u.transactionid, e.id, '5', 'unit_signature'
    FROM (
      SELECT c.postcode, c.eadds[1] AS eadd, unnest(t.tids) AS transactionid
      FROM us_candidates c
      JOIN us_unmatched t USING (postcode, propertytype, paon, saon, street)
      WHERE len(c.eadds) = 1
    ) u
    JOIN epc e ON e.postcode = u.postcode AND norm(e.add) = u.eadd
  ")

  drop_tables("us_epc_owner", "us_epc", "us_street", "us_unmatched", "us_candidates")
}

# peek_rule("taba4", "AL1 1AA") returns, for one postcode, the keys a rule builds on each side and the
# pairs they join. It reads the full transactions and epc tables, so it ignores rows removed by earlier
# rules, the row subsets of stage tables that no longer exist, and the stage 4 EPC corrections.
peek_rule <- function(name, postcode, stage = NULL, conn = con) {
  keys <- if (is.null(stage)) {
    names(rule_defs)[sub("^\\S+ ", "", names(rule_defs)) == name]
  } else {
    paste(stage, name)
  }
  if (length(keys) != 1) stop("give the stage: ", paste(keys, collapse = ", "))
  def <- rule_defs[[keys]]
  create_macros(conn)
  pc <- paste0("postcode = ", sq(postcode))
  tran_cols <- "transactionid, postcode, propertytype, paon, saon, street, locality"
  epc_cols <- "add1, add2, add3, add, postcode, propertytype, id"
  side <- function(from, steps, cols) {
    dbGetQuery(conn, icu_to_re2(rule_side(from, steps, pc, cols)))
  }
  list(
    tran = side("transactions", def$tran, tran_cols),
    epc = side("epc", def$epc, epc_cols),
    matched = dbGetQuery(conn, icu_to_re2(rule_select(
      def, "t.*, e.*", tran_from = "transactions", epc_from = "epc", extra = list(tran = pc, epc = pc),
      tran_cols = tran_cols, epc_cols = epc_cols
    )))
  )
}

# Part 3's tie-breaking logic, as resolve_ties() in the retired 4. Create final PPD.R, for each
# transaction's candidate EPCs:
#   1. the smallest absolute inspection difference; if one row has it, that row
#   2. of those, the smallest absolute lodgement difference (none if a lodgement date is missing, as
#      min() is then NA in R); if one row has it, that row
#   3. of those, the most rooms, or the largest floor area if any number of rooms is missing
#   4. of those, the lowest EPC id: R's which.max() takes the first row, which is the lowest id as
#      part 2 stores casa in (transactionid, id) order
resolve_ties <- function(candidates) {
  sprintf(
    "
    WITH s1 AS (
      SELECT * FROM (%s)
      QUALIFY abs_inspection_diff = min(abs_inspection_diff) OVER (PARTITION BY transactionid)
    ),
    s2 AS (
      SELECT * FROM s1
      QUALIFY count(*) OVER (PARTITION BY transactionid) = 1
        OR (count(*) FILTER (WHERE abs_lodgement_diff IS NULL) OVER (PARTITION BY transactionid) = 0
            AND abs_lodgement_diff = min(abs_lodgement_diff) OVER (PARTITION BY transactionid))
    ),
    s3 AS (
      SELECT *, bool_or(numberrooms IS NULL) OVER (PARTITION BY transactionid) AS rooms_na FROM s2
    )
    SELECT * EXCLUDE (rooms_na) FROM s3
    QUALIFY row_number() OVER (
      PARTITION BY transactionid
      ORDER BY CASE WHEN rooms_na THEN tfarea ELSE numberrooms END DESC, id
    ) = 1",
    candidates
  )
}

# ------------------------------------------------
# Part 1, load
# ------------------------------------------------
# The EPC certificates and the price paid data are read and tidied inside DuckDB, with no intermediate
# files, and written straight to the database. The tables are the same as those of the retired
# 1. Combine EPC certificates.R and 2. Add PPD and EPC to database.R, which read the files with readr,
# so readr's conventions are kept: fields are trimmed of spaces and tabs, empty and NA fields are
# missing, duplicates keep their first occurrence, rows that tie in a sort keep their file order, and
# the factor columns' levels are in order of first appearance. These rely on rowid being the file
# order, which needs DuckDB's default preserve_insertion_order = true (part 2 turns it off for its own
# connection).

t_part <- Sys.time()

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
run_sql(sprintf("SET temp_directory = '%s'", tempdir())) # not the project folder

# A field as readr reads it: trimmed of spaces and tabs, and missing when empty or NA
run_sql("CREATE OR REPLACE TEMP MACRO field(x) AS nullif(nullif(trim(x, ' \t'), ''), 'NA')")

# trim_ws(), as stri_trim_both()
create_macros(con)

# brackets replaced by a space, e.g. for a handful of Flat 1(2 xyz) type addresses
run_sql(
  "CREATE OR REPLACE TEMP MACRO no_brackets(x) AS regexp_replace(x, '[\\[\\]\\(\\)\\{\\}]', ' ', 'g')"
)

## EPC certificates

# The ten columns of every file. DuckDB reads the files in name order and keeps their rows in order,
# so rowid numbers them as list.files() and bind_rows() order them
run_sql(sprintf(
  "
  CREATE OR REPLACE TEMP TABLE certificate_files AS
  SELECT
    field(address1) AS address1, field(address2) AS address2, field(address3) AS address3,
    field(address) AS address, field(postcode) AS postcode, field(property_type) AS property_type,
    TRY_CAST(field(inspection_date) AS DATE) AS inspection_date,
    TRY_CAST(field(lodgement_date) AS DATE) AS lodgement_date,
    TRY_CAST(field(total_floor_area) AS DOUBLE) AS total_floor_area,
    TRY_CAST(field(number_habitable_rooms) AS DOUBLE) AS number_habitable_rooms
  FROM read_csv('%s/certificates-*.csv', all_varchar = true, union_by_name = true)
  ",
  path.expand(epc_dir)
))
n_certificates <- dbGetQuery(con, "SELECT count(*) FROM certificate_files")[[1]]

# distinct() keeps the first of each set of duplicates; an id in arrange() order, which keeps tied rows
# in their order; upper-case addresses, missing text as '', addresses trimmed, and brackets replaced
# by a space in every text column. Every certificate then has its own id, so no rows are duplicates
run_sql("
  CREATE OR REPLACE TEMP TABLE epc_tidy AS
  SELECT
    no_brackets(trim_ws(coalesce(upper(address1), ''))) AS add1,
    no_brackets(trim_ws(coalesce(upper(address2), ''))) AS add2,
    no_brackets(trim_ws(coalesce(upper(address3), ''))) AS add3,
    no_brackets(trim_ws(coalesce(upper(address), ''))) AS add,
    no_brackets(coalesce(postcode, '')) AS postcode,
    no_brackets(coalesce(property_type, '')) AS propertytype,
    inspection_date AS inspectiondate, lodgement_date AS lodgementdate, total_floor_area AS tfarea,
    number_habitable_rooms AS numberrooms,
    CAST(
      row_number() OVER (ORDER BY lodgement_date NULLS LAST, total_floor_area NULLS LAST, position)
      AS DOUBLE
    ) AS id
  FROM (SELECT *, min(rowid) AS position FROM certificate_files GROUP BY ALL)
")
run_sql("DROP TABLE certificate_files")

## Price paid data

# DuckDB keeps the rows in file order, so rowid gives each row's position
run_sql(sprintf(
  "
  CREATE OR REPLACE TEMP TABLE ppd_raw AS
  SELECT
    TRY_CAST(field(price) AS INTEGER) AS price,
    TRY_CAST(field(dateoftransfer) AS TIMESTAMP) AS dateoftransfer,
    field(postcode) AS postcode, field(propertytype) AS propertytype, field(oldnew) AS oldnew,
    field(duration) AS duration, field(paon) AS paon, field(saon) AS saon, field(street) AS street,
    field(locality) AS locality, field(towncity) AS towncity, field(district) AS district,
    field(county) AS county, field(categorytype) AS categorytype, field(recordstatus) AS recordstatus
  FROM read_csv(
    '%s', header = false, all_varchar = true,
    names = ['transaction_id', 'price', 'dateoftransfer', 'postcode', 'propertytype', 'oldnew',
             'duration', 'paon', 'saon', 'street', 'locality', 'towncity', 'district', 'county',
             'categorytype', 'recordstatus']
  )
  ",
  ppd_path
))

# The factor columns become ENUMs with col_factor()'s levels: the values in order of first appearance
enum_of <- function(column) {
  levels <- dbGetQuery(con, sprintf(
    "SELECT %s AS level FROM ppd_raw WHERE %s IS NOT NULL GROUP BY 1 ORDER BY min(rowid)", column, column
  ))$level
  sprintf("ENUM(%s)", paste0("'", gsub("'", "''", levels), "'", collapse = ", "))
}
enums <- sapply(c("propertytype", "oldnew", "duration", "categorytype", "recordstatus"), enum_of)

# distinct() (the transaction identifier is dropped first: there are duplicate rows with identical
# columns except for the second part of this id), remove 'Other' property types and Additional Price
# Paid transactions, arrange by postcode, transfers before last_date, then transactionid in that order
run_sql(sprintf(
  "
  CREATE OR REPLACE TEMP TABLE ppd_tidy AS
  SELECT
    CAST(row_number() OVER (ORDER BY postcode NULLS LAST, position) AS INTEGER) AS transactionid,
    price, dateoftransfer, coalesce(postcode, '') AS postcode,
    CAST(propertytype AS %s) AS propertytype, CAST(oldnew AS %s) AS oldnew,
    CAST(duration AS %s) AS duration, coalesce(paon, '') AS paon,
    no_brackets(coalesce(saon, '')) AS saon, coalesce(street, '') AS street,
    coalesce(locality, '') AS locality, coalesce(towncity, '') AS towncity,
    coalesce(district, '') AS district, coalesce(county, '') AS county,
    CAST(categorytype AS %s) AS categorytype, CAST(recordstatus AS %s) AS recordstatus,
    CAST(year(dateoftransfer) AS DOUBLE) AS yearchi
  FROM (SELECT *, min(rowid) AS position FROM ppd_raw GROUP BY ALL) -- distinct(), keeping the first
  WHERE propertytype <> 'O' AND categorytype <> 'B' AND dateoftransfer < TIMESTAMP '%s'
  ",
  enums[["propertytype"]], enums[["oldnew"]], enums[["duration"]], enums[["categorytype"]],
  enums[["recordstatus"]], last_date
))
run_sql("DROP TABLE ppd_raw")

## Keep only epc and ppd records with postcodes in common, and write them to the database. The
# duplicates were removed above, so the rows are already distinct

run_sql("
  CREATE OR REPLACE TABLE epc AS
  SELECT * FROM epc_tidy WHERE postcode IN (SELECT postcode FROM ppd_tidy)
  ORDER BY postcode, id
")

run_sql("
  CREATE OR REPLACE TABLE transactions AS
  SELECT * FROM ppd_tidy WHERE postcode IN (SELECT postcode FROM epc_tidy)
  ORDER BY transactionid
")

message(
  "certificates read: ", format(n_certificates, big.mark = ","),
  "; transactions: ", format(dbGetQuery(con, "SELECT count(*) FROM transactions")[[1]], big.mark = ","),
  "; epc: ", format(dbGetQuery(con, "SELECT count(*) FROM epc")[[1]], big.mark = ",")
)
message("part 1, load: ", format(round(Sys.time() - t_part, 1)))

dbDisconnect(con, shutdown = TRUE)

# ------------------------------------------------
# Part 2, link
# ------------------------------------------------

t_part <- Sys.time()

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
run_sql("SET preserve_insertion_order = false")
create_macros(con)

# Bin Chi's address corrections, used in stages 3a and 3b
rules <- read.csv(rulechi_path) |> mutate(across(c(postcode, add1, street), trimws))

run_sql(
  "CREATE OR REPLACE TEMP TABLE matches (transactionid INTEGER, id DOUBLE, stage VARCHAR, rule VARCHAR)"
)

# The Land Registry PPD and the Domestic EPCs, with only the variables used for matching
load_table(
  "s1_tran",
  "SELECT transactionid, postcode, propertytype, paon, saon, street, locality FROM transactions"
)
load_table("s1_epc", "SELECT add1, add2, add3, add, postcode, propertytype, id FROM epc")

# ------------------stage 1------------------------------
start_stage("1")
###############matching rule 1 saonpaonstreet= ADDRE################
rule("taba1",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet = "replace(trim_ws(saonpaon || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet = "addressfinal")
)

###############matching rule 2 saonpaonstreet1= ADDRE################
rule("taba2",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "replace(trim_ws(saonpaon || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet1 = "addressfinal")
)

###############matching rule 3  saonpaonstreet2= ADDRE ################
rule("taba3",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal")
)
drop_matched("s1_tran", "taba1")
drop_matched("s1_tran", "taba2")
drop_matched("s1_tran", "taba3")

###############matching rule 4 saonpaonstreetn=ADDC################
rule("taba4",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn = "replace(trim_ws(saonpaonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetn = "addressfinal")
)

###############matching rule 5 saonpaonstreetn1=ADDC ################
rule("taba5",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn1 = "replace(trim_ws(saonpaonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetn1 = "addressfinal")
)

###############matching rule 6 saonpaonstreetn2=ADDC################
rule("taba6",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn2 = "replace(trim_ws(saonpaonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetn2 = "addressfinal")
)
drop_matched("s1_tran", "taba4")
drop_matched("s1_tran", "taba5")
drop_matched("s1_tran", "taba6")

###############matching rule 7 saonpaonlo =ADDRE################
rule("taba7",
  tran = c(
    saonpaon1 = "saon || ' ' || paon",
    saonpaonlo = "replace(saonpaon1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonlo = "addressfinal")
)

###############matching rule 8 saonpaonlon= ADDC  ################
drop_matched("s1_tran", "taba7")
rule("taba8",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    localityn = "translate(locality, '''.', '')",
    saonpaonn1 = "saonn || ' ' || paonn",
    saonpaonlon = "replace(saonpaonn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonlon = "addressfinal")
)

################matching rule 9 saonpaonlon = ADDCC####################
drop_matched("s1_tran", "taba8")
rule("taba9",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    localityn = "translate(locality, '''.', '')",
    saonpaonn1 = "saonn || ' ' || paonn",
    saonpaonlon = "replace(saonpaonn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(saonpaonlon = "addressfinal")
)

###############matching rule 10 saonpaonstreet = ADD12################
drop_matched("s1_tran", "taba9")
rule("taba10",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet = "replace(trim_ws(saonpaon || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet = "addressfinal")
)

###############matching rule 11 saonpaonstreet1 = ADD12################
drop_matched("s1_tran", "taba10")
rule("taba11",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "replace(trim_ws(saonpaon || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet1 = "addressfinal")
)

###############matching rule 12 saonpaonstreet2= ADD12 ################
drop_matched("s1_tran", "taba11")
rule("taba12",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal")
)

###############matching rule 13 saonpaonstreetn =ADD12C################
drop_matched("s1_tran", "taba12")
rule("taba13",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "trim_ws(saonn || ',' || paonn)",
    saonpaonstreetn = "replace(trim_ws(saonpaonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonstreetn = "addressfinal")
)

###############matching rule 14 saonpaonstreetn1=ADD12C ################
drop_matched("s1_tran", "taba13")
rule("taba14",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn1 = "replace(trim_ws(saonpaonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonstreetn1 = "addressfinal")
)

###############matching rule 15 saonpaonstreetn2=ADD12C  ################
drop_matched("s1_tran", "taba14")
rule("taba15",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn2 = "replace(trim_ws(saonpaonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonstreetn2 = "addressfinal")
)

###############matching rule 16 saonpaonstreet3= ADD12################
drop_matched("s1_tran", "taba15")
rule("taba16",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet3 = "replace(trim_ws(saonpaon1 || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet3 = "addressfinal")
)

###############matching rule 17 saonpaonstreetn3 = ADD12C1################
drop_matched("s1_tran", "taba16")
rule("taba17",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn3 = "replace(trim_ws(saonpaonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(saonpaonstreetn3 = "addressfinal")
)

###############matching rule 18 saonpaonstreetlo =ADDRE################
drop_matched("s1_tran", "taba17")
rule("taba18",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "trim_ws(saonpaon || ', ' || street)",
    saonpaonstreetlo = "replace(saonpaonstreet1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreetlo = "addressfinal")
)

###############matching rule 19 saonpaonstreetnlo= ADDC ################
drop_matched("s1_tran", "taba18")
rule("taba19",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn1 = "trim_ws(saonpaonn || ', ' || streetn)",
    localityn = "translate(locality, '''.', '')",
    saonpaonstreetnlo = "replace(saonpaonstreetn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetnlo = "addressfinal")
)

###############matching rule 20 saonpaonstreetlo =ADD12################
drop_matched("s1_tran", "taba19")
rule("taba20",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "trim_ws(saonpaon || ', ' || street)",
    saonpaonstreetlo = "replace(saonpaonstreet1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreetlo = "addressfinal")
)

###############matching rule 21 saonpaonstreet3 = ADDRE ################
drop_matched("s1_tran", "taba20")
rule("taba21",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet3 = "replace(trim_ws(saonpaon1 || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet3 = "addressfinal")
)

###############matching rule 22 saonpaonstreetn3= ADDC################
drop_matched("s1_tran", "taba21")
rule("taba22",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn3 = "replace(trim_ws(saonpaonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetn3 = "addressfinal")
)

###############matching rule 23 saonpaonlo=ADD12################
drop_matched("s1_tran", "taba22")
rule("taba23",
  tran = c(
    saonpaon1 = "saon || ' ' || paon",
    saonpaonlo = "replace(saonpaon1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonlo = "addressfinal")
)

###############matching rule 24 saonpaonlon = ADD12C ################
drop_matched("s1_tran", "taba23")
rule("taba24",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    localityn = "translate(locality, '''.', '')",
    saonpaonn1 = "saonn || ' ' || paonn",
    saonpaonlon = "replace(saonpaonn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonlon = "addressfinal")
)

###############matching rule 25 saonpaon1 = ADDRE################
drop_matched("s1_tran", "taba24")
rule("taba25",
  tran = c(
    saonpaon1 = "replace(saon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaon1 = "addressfinal")
)

###############matching rule 26 saonpaonstreet31 = ADDREC################
drop_matched("s1_tran", "taba25")
rule("taba26",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet3 = "trim_ws(saonpaon1 || ' ' || street)",
    saonpaonstreet31 = "translate(saonpaonstreet3, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(saonpaonstreet31 = "addressfinal")
)

###############matching rule 27 saonpaonstreetn31 = ADDC3################
drop_matched("s1_tran", "taba26")
rule("taba27",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn3 = "trim_ws(saonpaonn1 || ' ' || streetn)",
    saonpaonstreetn31 = "translate(saonpaonstreetn3, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(saonpaonstreetn31 = "addressfinal")
)
drop_matched("s1_tran", "taba27")

###############combine the results of matching rules 1 to 27 ################

end_stage("1")

# -------------------stage 2------------------------------
###############prepare for stage 2###################
start_stage("2")
# tran2 is the transaction records which saon is null
make_table("s2_tran", "s1_tran", where = "saon = ''")

# tran3, the transaction records which saon is not null, are saved for stage 4
write_table("tranbin3", "s1_tran", where = "saon <> ''")

# EPCs in the postcodes still to match
make_table("s2_epc", "s1_epc", where = "postcode IN (SELECT postcode FROM s2_tran)")
drop_tables("s1_tran", "s1_epc")

###############matching rule 28 paonstreetlo = ADDRE#################
rule("taba28",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo = "replace(paonstreet || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonstreetlo = "addressfinal")
)

###############matching rule 29 paonstreetnlo  = ADDC#################
drop_matched("s2_tran", "taba28")
rule("taba29",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paonstreetn = "paonn || ', ' || streetn",
    paonstreetnlo = "replace(paonstreetn || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(paonstreetnlo = "addressfinal")
)

###############matching rule 30 paonstreetlo =ADD12#################
drop_matched("s2_tran", "taba29")
rule("taba30",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo = "replace(paonstreet || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paonstreetlo = "addressfinal")
)

###############matching rule 31 paonstreetnlo = ADD12C#################
drop_matched("s2_tran", "taba30")
rule("taba31",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paonstreetn = "paonn || ', ' || streetn",
    paonstreetnlo = "replace(paonstreetn || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(paonstreetnlo = "addressfinal")
)

###############matching rule 32 paonstreetlo1 = ADDRE#################
drop_matched("s2_tran", "taba31")
rule("taba32",
  tran = c(
    paonstreet1 = "paon || ' ' || street",
    paonstreetlo1 = "replace(paonstreet1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonstreetlo1 = "addressfinal")
)

###############matching rule 33 paonstreetnlo1 = ADDC#################
drop_matched("s2_tran", "taba32")
rule("taba33",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paonstreetn1 = "paonn || ' ' || streetn",
    paonstreetnlo1 = "replace(paonstreetn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(paonstreetnlo1 = "addressfinal")
)

###############matching rule 34 paonstreetlo1 = ADD12#################
drop_matched("s2_tran", "taba33")
rule("taba34",
  tran = c(
    paonstreet1 = "paon || ' ' || street",
    paonstreetlo1 = "replace(paonstreet1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paonstreetlo1 = "addressfinal")
)

###############matching rule 35 paonstreetnlo1 = ADD12C#################
drop_matched("s2_tran", "taba34")
rule("taba35",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paonstreetn1 = "paonn || ' ' || streetn",
    paonstreetnlo1 = "replace(paonstreetn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(paonstreetnlo1 = "addressfinal")
)

###############matching rule 36 paonstreetlo2== ADD12C2#################
drop_matched("s2_tran", "taba35")
rule("taba36",
  tran = c(
    paonstreet1 = "paon || ' ' || street",
    paonstreetlo2 = "translate(paonstreet1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, ', ', '')"
  ),
  by = c(paonstreetlo2 = "addressfinal")
)

###############matching rule 37 paonstreetlo2= ADDREC################
drop_matched("s2_tran", "taba36")
rule("taba37",
  tran = c(
    paonstreet1 = "paon || ' ' || street",
    paonstreetlo2 = "translate(paonstreet1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paonstreetlo2 = "addressfinal")
)

###############matching rule 38 paonstreetn=ADD12C3#################
drop_matched("s2_tran", "taba37")
rule("taba38",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    paonstreetn = "replace(paonn || ', ' || streetn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '-''./ ', '')"
  ),
  by = c(paonstreetn = "addressfinal")
)

###############matching rule 39 for the street is null, paonn3 =ADD1CC#################
drop_matched("s2_tran", "taba38")
rule("taba39",
  tran_where = "street = ''",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    paonn3 = "translate(paonn, '- ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '-''./ ', '')"
  ),
  by = c(paonn3 = "addressfinal")
)

###############matching rule 40  paon66=ADD1CC#################
drop_matched("s2_tran", "taba39")
rule("taba40",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon66 = "replace(paon62 || ', ' || paon61, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '-''./ ', '')"
  ),
  by = c(paon66 = "addressfinal")
)
drop_matched("s2_tran", "taba40")

######combine the results of matching rules 28 to 40########

end_stage("2")

# ------------------stage 3------------------------------
###############prepare for stage 3 #############
start_stage("3a")
# Select the transactions for which the property type is not Flats/Maisonettes
make_table("s3a_tran", "s2_tran", where = "propertytype <> 'F'")

# Save the transactions for which the property type is Flats/Maisonettes for stage 3b
write_table("tranbin24", "s2_tran", where = "propertytype = 'F'")

# EPCs in the postcodes still to match
make_table("s3a_epc", "s2_epc", where = "postcode IN (SELECT postcode FROM s3a_tran)")
drop_tables("s2_tran", "s2_epc")

## Stage 3a transactions with null saon for Detached, Semi-Detached and Terraced
###############matching rule 41 paon65streetlo=ADDRE#########
rule("taba41",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "paon65 || ', ' || street",
    paon65streetlo = "replace(paon65street || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paon65streetlo = "addressfinal")
)

###############matching rule 42 paon65streetlo=ADD12#################
drop_matched("s3a_tran", "taba41")
rule("taba42",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "paon65 || ', ' || street",
    paon65streetlo = "replace(paon65street || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paon65streetlo = "addressfinal")
)

###############matching rule 43 paon65streetnlo=ADDCC#################
drop_matched("s3a_tran", "taba42")
rule("taba43",
  tran_where = "contains(paon, ',')",
  tran = c(
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paon65n = "last_token(paon)",
    paon65street = "paon65n || ', ' || streetn",
    paon65streetnlo = "replace(paon65street || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(paon65streetnlo = "addressfinal")
)

###############matching rule 44 paon65streetlo1=ADDREC#################
drop_matched("s3a_tran", "taba43")
rule("taba44",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "paon65 || ' ' || street",
    paon65streetlo1 = "translate(paon65street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paon65streetlo1 = "addressfinal")
)

###############matching rule 45 paon61streetlo=ADDC#################
drop_matched("s3a_tran", "taba44")
rule("taba45",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ', ' || street",
    paon61streetlo = "replace(paon61street1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(paon61streetlo = "addressfinal")
)

###############matching rule 46 paon61streetlo1=ADDREC#################
drop_matched("s3a_tran", "taba45")
rule("taba46",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal")
)

###############matching rule 47 paon61streetlo1=ADDC3#################
drop_matched("s3a_tran", "taba46")
rule("taba47",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal")
)

###############matching rule 48 paon61streetlo1=ADD12C1#################
drop_matched("s3a_tran", "taba47")
rule("taba48",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal")
)

###############matching rule 49 paon61lo=ADD12C#################
drop_matched("s3a_tran", "taba48")
rule("taba49",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61lo = "replace(paon61 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(paon61lo = "addressfinal")
)

###############matching rule 50 paon61street= ADD12C1#################
drop_matched("s3a_tran", "taba49")
rule("taba50",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street = "translate(paon61 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(paon61street = "addressfinal")
)

###############matching rule 51 paon61street= ADD13C1#################
drop_matched("s3a_tran", "taba50")
rule("taba51",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street = "translate(paon61 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, '''./, ', '')"
  ),
  by = c(paon61street = "addressfinal")
)

###############matching rule 52 paon65street = ADDC3#################
drop_matched("s3a_tran", "taba51")
rule("taba52",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "translate(paon65 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(paon65street = "addressfinal")
)

###############matching rule 53 paon65street= ADD1C2#################
drop_matched("s3a_tran", "taba52")
rule("taba53",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "translate(paon65 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./, ', '')"
  ),
  by = c(paon65street = "addressfinal")
)

###############matching rule 54 paon66streetlo=ADDCCC#################
drop_matched("s3a_tran", "taba53")
rule("taba54",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon66 = "paon62 || ' ' || paon61",
    paon66street = "paon66 || ' ' || street",
    paon66streetlo = "translate(paon66street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '-''./, ', '')"
  ),
  by = c(paon66streetlo = "addressfinal")
)

###############matching rule 55 paon66streetlo =ADD12C3#################
drop_matched("s3a_tran", "taba54")
rule("taba55",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon66 = "paon62 || ' ' || paon61",
    paon66street = "paon66 || ' ' || street",
    paon66streetlo = "translate(paon66street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add2, '-''./, ', '')"
  ),
  by = c(paon66streetlo = "addressfinal")
)

###############matching rule 56  paon65streetlo1 =ADD23C1#################
drop_matched("s3a_tran", "taba55")
rule("taba56",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    paon65street = "paon65 || ' ' || street",
    paon65streetlo = "translate(paon65street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add2 || ' ' || add3, '''./, ', '')"
  ),
  by = c(paon65streetlo = "addressfinal"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 57 paon61new=ADD1#################
drop_matched("s3a_tran", "taba56")
rule("taba57",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61new = "'THE' || ' ' || paon61"
  ),
  by = c(paon61new = "add1"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 58 paonstreetlo3= ADD12new#################
drop_matched("s3a_tran", "taba57")
rule("taba58",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo3 = "translate(paonstreet || ', ' || locality, ', ', '')"
  ),
  epc = c(
    add2new = "replace(add2, '-', '')",
    addressfinal = "translate(add1 || ' ' || add2new, '''./, ', '')"
  ),
  by = c(paonstreetlo3 = "addressfinal")
)

###############matching rule 59  paonstreetlo3= ADD13C1#################
drop_matched("s3a_tran", "taba58")
rule("taba59",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo3 = "translate(paonstreet || ', ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add3, '''./, ', '')"
  ),
  by = c(paonstreetlo3 = "addressfinal")
)

###############matching rule 60 paonstreetlo3 = ADD13C2#################
drop_matched("s3a_tran", "taba59")
rule("taba60",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo3 = "translate(paonstreet || ', ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add3, ', ', '')"
  ),
  by = c(paonstreetlo3 = "addressfinal")
)

###############matching rule 61  paonstreet=ADD1C3#################
drop_matched("s3a_tran", "taba60")
rule("taba61",
  tran = c(
    paonstreet = "translate(paon || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, ', ', '')"
  ),
  by = c(paonstreet = "addressfinal")
)

###############matching rule 62 PAON=ADD1#################
drop_matched("s3a_tran", "taba61")
rule("taba62",
  by = c(paon = "add1")
)

###############matching rule 63  paonstreetlo3 =ADD662#################
drop_matched("s3a_tran", "taba62")
rule("taba63",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreetlo3 = "translate(paonstreet || ', ' || locality, ', ', '')"
  ),
  epc_where = "contains(add1, ',') AND contains(char2end(add1, ','), '-')",
  epc = c(
    add161 = "beg2char(add1, ',')",
    add162 = "replace(char2end(add1, ','), '-', '')",
    add66 = "add161 || ', ' || add162",
    addressfinal = "translate(add66 || ', ' || add2, ', ', '')"
  ),
  by = c(paonstreetlo3 = "addressfinal")
)

###############matching rule 64 paonstreet= ADD67#################
drop_matched("s3a_tran", "taba63")
rule("taba64",
  tran = c(
    paonstreet = "translate(paon || ', ' || street, ', ', '')"
  ),
  epc_where = "contains(add1, ',') AND contains(char2end(add1, ','), '.')",
  epc = c(
    add161 = "beg2char(add1, ',')",
    add165 = "replace(char2end(add1, ','), '.', '')",
    add67 = "add161 || ', ' || add165",
    addressfinal = "translate(add67, ', ', '')"
  ),
  by = c(paonstreet = "addressfinal")
)

###############matching rule 65  paonstreet= ADDSP12#################
drop_matched("s3a_tran", "taba64")
rule("taba65",
  tran_where = "street <> ''",
  tran = c(
    paonstreet = "translate(paon || ', ' || street, ', ', '')"
  ),
  epc_where = "NOT regexp_matches(add2, icu('^\\d'))
      AND NOT regexp_matches(add1, icu('\\s\\D(\\s|[,]|>)'))",
  epc = c(
    add1sp = "beg2char(add1, ' ')",
    addsp12 = "add1sp || ', ' || add2",
    addressfinal = "translate(addsp12, ', ', '')"
  ),
  by = c(paonstreet = "addressfinal"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 66 paonstreetn1=ADD1C4#################
drop_matched("s3a_tran", "taba65")
# The stringi version reads tran22$paonstreet here, which only works through R's `$` partial matching
# to tran22$paonstreetn1; paonstreetn1 is the column Bin Chi's original uses, and is used directly here
rule("taba66",
  tran = c(
    streetn1 = "translate(street, '-''.', '')",
    paonstreetn1 = "translate(paon || ', ' || streetn1, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, ''', ', '')"
  ),
  by = c(paonstreetn1 = "addressfinal")
)

###############matching rule 67 paonstreet=ADDU#################
drop_matched("s3a_tran", "taba66")
rule("taba67",
  tran = c(
    paonstreet = "translate(paon || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(replace(add, 'UNIT ', ''), ', ', '')"
  ),
  by = c(paonstreet = "addressfinal"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 68 paonstreet1=ADD68   #################
drop_matched("s3a_tran", "taba67")
rule("taba68",
  tran = c(
    paonstreet1 = "replace(paon || ', ' || street, ' ', '')"
  ),
  epc = c(
    add162 = "char2end(add1, ',')",
    add63 = "replace(replace(add162, '-', ''), '.', '')",
    add161 = "beg2char(add1, ',')",
    addressfinal = "translate(add161 || ', ' || add63, ''' ', '')"
  ),
  by = c(paonstreet1 = "addressfinal")
)

###############matching rule 69  paonstreet1=ADD69#################
drop_matched("s3a_tran", "taba68")
rule("taba69",
  tran = c(
    paonstreet1 = "replace(paon || ', ' || street, ' ', '')"
  ),
  epc = c(
    add1nn = "replace(replace(add1, 'NO ', ''), ',', '')",
    addressfinal = "replace(add1nn || ', ' || add2, ' ', '')"
  ),
  by = c(paonstreet1 = "addressfinal")
)

###############prepare for address string in EPCs, written different in Land Registry PPD#################
drop_matched("s3a_tran", "taba69")
dbWriteTable(con, "rules", rules, temporary = TRUE, overwrite = TRUE)
make_table("s3a_epc1", "s3a_epc", where = "postcode IN (SELECT postcode FROM rules)")

### create back the right address string
apply_rulechi("s3a_epc1", rules)

###############matching rule 70 paonstreet1 =ADD1C5################
rule("taba70",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonstreet1 = "replace(paon || ', ' || street, ' ', '')"
  ),
  epc_from = "s3a_epc1",
  epc = c(
    addressfinal = "translate(add1, '. ', '')"
  ),
  by = c(paonstreet1 = "addressfinal")
)

###############matching rule 71 paonstreet2=ADD1C5#################
drop_matched("s3a_tran", "taba70")
rule("taba71",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonstreet2 = "replace(paon || ' ' || street, ' ', '')"
  ),
  epc_from = "s3a_epc1",
  epc = c(
    addressfinal = "translate(add1, '. ', '')"
  ),
  by = c(paonstreet2 = "addressfinal")
)

###############matching rule 72 paonn2=ADD1C6#################
drop_matched("s3a_tran", "taba71")
rule("taba72",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonn2 = "translate(paon, ', ', '')"
  ),
  epc_from = "s3a_epc1",
  epc = c(
    addressfinal = "translate(replace(add1, 'UNIT ', ''), ', ', '')"
  ),
  by = c(paonn2 = "addressfinal"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 73 paonstreet3=ADDCCC #################
drop_matched("s3a_tran", "taba72")
rule("taba11111",
  tran = c(
    paonstreet3 = "translate(paon || ' ' || street, ', ', '')"
  ),
  epc_where = "NOT regexp_matches(add, icu('\\d+-\\d+'))",
  epc = c(
    addressfinal = "translate(add, '-''./, ', '')"
  ),
  by = c(paonstreet3 = "addressfinal"),
  where = "t.saon = ''"
)
drop_matched("s3a_tran", "taba11111")

###############combine the results of matching rules 41 to 73################

end_stage("3a")

## Stage 3b transactions with null saon for Flats/Maisonettes
start_stage("3b")
drop_tables("s3a_tran", "s3a_epc", "s3a_epc1")
load_table("s3b_tran", "select * from tranbin24")
load_table(
  "s3b_epc",
  "select add1, add2, add3, add, postcode, propertytype, id
   from epc
   where postcode in (select distinct postcode from tranbin24)"
)

###############matching rule 74  paon62streetlo= ADDRE###############
rule("taba41",
  tran_where = "contains(paon, ',')
      AND NOT regexp_matches(beg2char(paon, ','), icu('FLAT\\s'))
      AND NOT contains(beg2char(paon, ','), 'FLOOR')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    paon62street = "paon62 || ', ' || street",
    paon62streetlo = "replace(paon62street || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paon62streetlo = "addressfinal")
)

###############matching rule 75  paon62streetlo=ADD12#################
drop_matched("s3b_tran", "taba41")
rule("taba42",
  tran_where = "contains(paon, ',')
      AND NOT regexp_matches(beg2char(paon, ','), icu('FLAT\\s'))
      AND NOT contains(beg2char(paon, ','), 'FLOOR')
      AND NOT regexp_matches(beg2char(paon, ','), icu('\\d'))",
  tran = c(
    paon62 = "char2end(paon, ',')",
    paon62street = "paon62 || ', ' || street",
    paon62streetlo = "replace(paon62street || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paon62streetlo = "addressfinal")
)

###############matching rule 76  paon65streetnlo=ADDCC#################
drop_matched("s3b_tran", "taba42")
rule("taba43",
  tran_where = "contains(paon, ',')
      AND NOT regexp_matches(beg2char(translate(paon, '''.', ''), ','), icu('FLAT\\s'))
      AND NOT contains(beg2char(translate(paon, '''.', ''), ','), 'FLOOR')
      AND NOT regexp_matches(beg2char(translate(paon, '''.', ''), ','), icu('\\d'))",
  tran = c(
    streetn = "replace(street, '''', '')",
    localityn = "translate(locality, '''.', '')",
    paon65n = "last_token(paon)",
    paon65street = "paon65n || ', ' || streetn",
    paon65streetnlo = "replace(paon65street || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(paon65streetnlo = "addressfinal")
)

###############matching rule 77 paon62streetlo1=ADDREC#################
drop_matched("s3b_tran", "taba43")
rule("taba44",
  tran_where = "contains(paon, ',')
      AND NOT regexp_matches(beg2char(paon, ','), icu('FLAT\\s'))
      AND NOT contains(beg2char(paon, ','), 'FLOOR')
      AND NOT regexp_matches(beg2char(paon, ','), icu('\\d'))",
  tran = c(
    paon62 = "char2end(paon, ',')",
    paon62street = "paon62 || ' ' || street",
    paon62streetlo1 = "translate(paon62street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paon62streetlo1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 78 paon61streetlo =ADDC #################
drop_matched("s3b_tran", "taba44")
rule("taba45",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ', ' || street",
    paon61streetlo = "replace(paon61street1 || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(paon61streetlo = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 79 paon61streetlo1=ADDREC#################
drop_matched("s3b_tran", "taba45")
rule("taba46",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 80 paon61streetlo1=ADDC3#################
drop_matched("s3b_tran", "taba46")
rule("taba47",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal")
)

###############matching rule 81 paon61streetlo1=ADD12C1#################
drop_matched("s3b_tran", "taba47")
rule("taba48",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street1 = "paon61 || ' ' || street",
    paon61streetlo1 = "translate(paon61street1 || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(paon61streetlo1 = "addressfinal")
)

###############matching rule 82 paon61street= ADD13C1#################
drop_matched("s3b_tran", "taba48")
rule("taba51",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon61street = "translate(paon61 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, '''./, ', '')"
  ),
  by = c(paon61street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 83 paon66streetlo= ADDCCC#################
drop_matched("s3b_tran", "taba51")
rule("taba52",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon66 = "paon62 || ' ' || paon61",
    paon66street = "paon66 || ' ' || street",
    paon66streetlo = "translate(paon66street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./, ', '')"
  ),
  by = c(paon66streetlo = "addressfinal")
)

###############matching rule 84 paon66streetlo =ADD12C3#################
drop_matched("s3b_tran", "taba52")
rule("taba53",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon66 = "paon62 || ' ' || paon61",
    paon66street = "paon66 || ' ' || street",
    paon66streetlo = "translate(paon66street || ' ' || locality, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add2, '-''./ ', '')"
  ),
  by = c(paon66streetlo = "addressfinal")
)

###############matching rule 85 paonfstreet=ADDRE#################
drop_matched("s3b_tran", "taba53")
rule("taba54",
  tran = c(
    paonflat = "'FLAT' || ' ' || paon",
    paonfstreet = "replace(paonflat || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonfstreet = "addressfinal")
)

###############matching rule 86 paonfstreet= ADD12#################
drop_matched("s3b_tran", "taba54")
rule("taba55",
  tran = c(
    paonflat = "'FLAT ' || paon",
    paonfstreet = "replace(paonflat || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paonfstreet = "addressfinal")
)

###############matching rule 87 paonapstreet =ADDRE#################
drop_matched("s3b_tran", "taba55")
rule("taba56",
  tran = c(
    paonap = "'APARTMENT' || ' ' || paon",
    paonapstreet = "replace(paonap || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonapstreet = "addressfinal")
)

###############matching rule 88 paonfstreet1= ADDRE#################
drop_matched("s3b_tran", "taba56")
rule("taba57",
  tran = c(
    paonflat = "'FLAT' || ' ' || paon",
    paonfstreet1 = "replace(paonflat || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonfstreet1 = "addressfinal")
)

###############matching rule 89 paonstreet= ADD1C7#################
drop_matched("s3b_tran", "taba57")
rule("taba58",
  tran = c(
    paonstreet = "translate(paon || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, ', ', '')"
  ),
  by = c(paonstreet = "addressfinal")
)

###############matching rule 90 paonstreetn1= ADD1C7#################
drop_matched("s3b_tran", "taba58")
# As in rule 66, the stringi version reaches tran22$paonstreetn1 through R's `$` partial matching of
# tran22$paonstreet; paonstreetn1 is used directly here
rule("taba59",
  tran = c(
    streetn1 = "translate(street, '-''.', '')",
    paonstreetn1 = "translate(paon || ', ' || streetn1, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, ', ', '')"
  ),
  by = c(paonstreetn1 = "addressfinal")
)

###############matching rule 91 paonstreetn1= ADD1C8 #################
drop_matched("s3b_tran", "taba59")
rule("taba60",
  tran = c(
    streetn1 = "translate(street, '-''.', '')",
    paonstreet = "paon || ', ' || streetn1",
    paonstreetn1 = "translate(paonstreet, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '., ', '')"
  ),
  by = c(paonstreetn1 = "addressfinal")
)

###############for the address string written different in EPCs #################
drop_matched("s3b_tran", "taba60")
dbWriteTable(con, "rules", rules, temporary = TRUE, overwrite = TRUE)
make_table("s3b_epc1", "s3b_epc", where = "postcode IN (SELECT postcode FROM rules)")

### create back the right address string
apply_rulechi("s3b_epc1", rules)

###############matching rule 92 paonstreet1=ADD1C5################
rule("taba61",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonstreet1 = "replace(paon || ', ' || street, ' ', '')"
  ),
  epc_from = "s3b_epc1",
  epc = c(
    addressfinal = "translate(add1, '. ', '')"
  ),
  by = c(paonstreet1 = "addressfinal")
)

###############matching rule 93 paonstreet2=ADD1C5#################
drop_matched("s3b_tran", "taba61")
rule("taba62",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonstreet2 = "replace(paon || ' ' || street, ' ', '')"
  ),
  epc_from = "s3b_epc1",
  epc = c(
    addressfinal = "translate(add1, '. ', '')"
  ),
  by = c(paonstreet2 = "addressfinal")
)

###############matching rule 94 PAON=ADD1df1 SE5 7QS#################
drop_matched("s3b_tran", "taba62")
rule("taba63",
  tran_where = "postcode IN (SELECT postcode FROM rules) AND postcode = 'SE5 7QS'",
  tran = c(
    add12 = "paon"
  ),
  epc_from = "s3b_epc1",
  epc_where = "postcode = 'SE5 7QS'",
  epc = c(
    add11 = "replace(add1, 'FLAT ', '')",
    add12 = "replace(first_token(add11), ',', '')"
  ),
  by = c(add12 = "add12"),
  by_postcode = FALSE
)

###############matching rule 95 paonn2=ADD1du#################
drop_matched("s3b_tran", "taba63")
update_table("s3b_epc1", add1 = "replace(add1, 'G01', 'G1')")
update_table("s3b_epc1", add1 = "replace(add1, 'G02', 'G2')")
update_table("s3b_epc1", add1 = "replace(add1, 'G03', 'G3')")
update_table("s3b_epc1", add1 = "replace(add1, 'G04', 'G4')")
update_table("s3b_epc1", add1 = "replace(add1, 'G05', 'G5')")
update_table("s3b_epc1", add1 = "replace(add1, 'G06', 'G6')")
update_table("s3b_epc1", add1 = "replace(add1, 'G07', 'G7')")
update_table("s3b_epc1", add1 = "replace(add1, 'G08', 'G8')")
update_table("s3b_epc1", add1 = "replace(add1, 'G09', 'G9')")
update_table("s3b_epc1", add1 = "replace(add1, 'H01', 'H1')")
update_table("s3b_epc1", add1 = "replace(add1, 'H02', 'H2')")
update_table("s3b_epc1", add1 = "replace(add1, 'H03', 'H3')")
update_table("s3b_epc1", add1 = "replace(add1, 'H04', 'H4')")
update_table("s3b_epc1", add1 = "replace(add1, 'H05', 'H5')")
update_table("s3b_epc1", add1 = "replace(add1, 'H06', 'H6')")
update_table("s3b_epc1", add1 = "replace(add1, 'H07', 'H7')")
update_table("s3b_epc1", add1 = "replace(add1, 'H08', 'H8')")
update_table("s3b_epc1", add1 = "replace(add1, 'H09', 'H9')")
update_table("s3b_epc1", add1 = "replace(add1, 'I01', 'I1')")
update_table("s3b_epc1", add1 = "replace(add1, 'I02', 'I2')")
update_table("s3b_epc1", add1 = "replace(add1, 'I03', 'I3')")
update_table("s3b_epc1", add1 = "replace(add1, 'I04', 'I4')")
update_table("s3b_epc1", add1 = "replace(add1, 'I05', 'I5')")
update_table("s3b_epc1", add1 = "replace(add1, 'I06', 'I6')")
update_table("s3b_epc1", add1 = "replace(add1, 'I07', 'I7')")
update_table("s3b_epc1", add1 = "replace(add1, 'I08', 'I8')")
update_table("s3b_epc1", add1 = "replace(add1, 'I09', 'I9')")
update_table("s3b_epc1", add1 = "replace(add1, 'J01', 'J1')")
update_table("s3b_epc1", add1 = "replace(add1, 'J02', 'J2')")
update_table("s3b_epc1", add1 = "replace(add1, 'J03', 'J3')")
update_table("s3b_epc1", add1 = "replace(add1, 'J04', 'J4')")
update_table("s3b_epc1", add1 = "replace(add1, 'J05', 'J5')")
update_table("s3b_epc1", add1 = "replace(add1, 'J06', 'J6')")
update_table("s3b_epc1", add1 = "replace(add1, 'J07', 'J7')")
update_table("s3b_epc1", add1 = "replace(add1, 'J08', 'J8')")
update_table("s3b_epc1", add1 = "replace(add1, 'J09', 'J9')")
update_table("s3b_epc1", add1 = "replace(add1, 'k01', 'k1')")
update_table("s3b_epc1", add1 = "replace(add1, 'k02', 'k2')")
update_table("s3b_epc1", add1 = "replace(add1, 'k03', 'k3')")
update_table("s3b_epc1", add1 = "replace(add1, 'k04', 'k4')")
update_table("s3b_epc1", add1 = "replace(add1, 'k05', 'k5')")
update_table("s3b_epc1", add1 = "replace(add1, 'k06', 'k6')")
update_table("s3b_epc1", add1 = "replace(add1, 'k07', 'k7')")
update_table("s3b_epc1", add1 = "replace(add1, 'k08', 'k8')")
update_table("s3b_epc1", add1 = "replace(add1, 'k09', 'k9')")
rule("taba64",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paonn2 = "translate(paon, ', ', '')"
  ),
  epc_from = "s3b_epc1",
  epc = c(
    addressfinal = "translate(replace(add1, 'UNIT ', ''), ', ', '')"
  ),
  by = c(paonn2 = "addressfinal")
)

###############matching rule 96 paon61c=ADD1C9#################
drop_matched("s3b_tran", "taba64")
rule("taba65",
  tran_where = "postcode IN (SELECT postcode FROM rules)",
  tran = c(
    paon61 = "replace(beg2char(paon, ','), ' ', '')"
  ),
  epc_from = "s3b_epc1",
  epc = c(
    addressfinal = "replace(add1, ' ', '')"
  ),
  by = c(paon61 = "addressfinal")
)

###############matching rule 97 paonfstreet1 =ADD1C9#################
drop_matched("s3b_tran", "taba65")
rule("taba66",
  tran_where = "regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    paonflat = "'FLAT' || ' ' || paon",
    paonfstreet1 = "replace(paonflat || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1, ' ', '')"
  ),
  by = c(paonfstreet1 = "addressfinal")
)

###############matching rule 98 paonfstreetn5=ADD1C #################
drop_matched("s3b_tran", "taba66")
rule("taba67",
  tran_where = "regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    paonflat = "'FLAT' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    paonfstreetn5 = "replace(paonflat || ' ' || streetn5, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./ ', '')"
  ),
  by = c(paonfstreetn5 = "addressfinal")
)

###############matching rule 99  paonstreet3=ADD1632#################
drop_matched("s3b_tran", "taba67")
rule("taba68",
  tran = c(
    paonstreet3 = "translate(paon || ' ' || street, ', ', '')"
  ),
  epc = c(
    add163 = "beg2char(add1, ' ')",
    addressfinal = "translate(add163 || ' ' || add2, ', ', '')"
  ),
  by = c(paonstreet3 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 100 paonapstreet1=ADD12C2#################
drop_matched("s3b_tran", "taba68")
rule("taba69",
  tran_where = "regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    paonap = "'APARTMENT' || ' ' || paon",
    paonapstreet1 = "replace(paonap || ' ' || street, ' ', '')"
  ),
  epc_where = "NOT regexp_matches(add2, icu('^\\d+'))",
  epc = c(
    addressfinal = "replace(add1 || ' ' || add2, ' ', '')"
  ),
  by = c(paonapstreet1 = "addressfinal")
)

###############matching rule 101 paonapstreetn5=ADD12C1#################
drop_matched("s3b_tran", "taba69")
rule("taba70",
  tran_where = "regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    paonap = "'APARTMENT' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    paonapstreet5 = "replace(paonap || ' ' || streetn5, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(paonapstreet5 = "addressfinal")
)

###############matching rule 102 paonn2 =ADDC3#################
drop_matched("s3b_tran", "taba70")
rule("taba71",
  tran = c(
    paonn2 = "translate(paon, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(paonn2 = "addressfinal")
)

###############matching rule 103 paonstreet3 =flADD#################
drop_matched("s3b_tran", "taba71")
rule("taba72",
  tran = c(
    paonstreet = "translate(paon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate('FLAT ' || ', ' || add, ', ', '')"
  ),
  by = c(paonstreet = "addressfinal")
)

###############matching rule 104 paonn2=ADD2611#################
drop_matched("s3b_tran", "taba72")
rule("taba73",
  tran = c(
    paonn2 = "translate(paon, ', ', '')"
  ),
  epc_where = "contains(add2, ',')",
  epc = c(
    add261 = "beg2char(add2, ',')",
    add2166 = "add261 || ', ' || add1",
    addressfinal = "translate(add2166, ', ', '')"
  ),
  by = c(paonn2 = "addressfinal")
)

###############matching rule 105 paonstreet3=flADD13#################
drop_matched("s3b_tran", "taba73")
rule("taba74",
  tran = c(
    paonstreet = "paon || ' ' || street",
    paonstreet3 = "translate(paonstreet, ', ', '')"
  ),
  epc = c(
    add1f = "'FLAT' || ' ' || add1",
    add31 = "translate(add3, '''./', '')",
    addressfinal = "translate(add1f || ' ' || add31, ', ', '')"
  ),
  by = c(paonstreet3 = "addressfinal")
)

###############matching rule 106 paonstreet3=ADD13C2#################
drop_matched("s3b_tran", "taba74")
rule("taba75",
  tran = c(
    paonstreet = "paon || ' ' || street",
    paonstreet3 = "translate(paonstreet, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add3, ', ', '')"
  ),
  by = c(paonstreet3 = "addressfinal")
)

###############matching rule 107 paonstreet4=ADDC3#################
drop_matched("s3b_tran", "taba75")
rule("taba76",
  tran = c(
    paonstreet = "paon || ' ' || street",
    paonstreet4 = "translate(replace(paonstreet, 'FLAT', 'APARTMENT'), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(paonstreet4 = "addressfinal")
)

###############matching rule 108 paonfstreetn5=ADD1C2#################
drop_matched("s3b_tran", "taba76")
rule("taba77",
  tran_where = "regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    paonfl = "'FLAT' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    paonfstreetn5 = "replace(paonfl || ' ' || streetn5, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./, ', '')"
  ),
  by = c(paonfstreetn5 = "addressfinal")
)

###############matching rule 109 paonapstreet2=ADD12C2#################
drop_matched("s3b_tran", "taba77")
rule("taba78",
  tran = c(
    paonap = "'APARTMENT' || ' ' || paon",
    paonapstreet1 = "paonap || ' ' || street",
    paonapstreet2 = "translate(paonapstreet1, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add2, ', ', '')"
  ),
  by = c(paonapstreet2 = "addressfinal")
)

###############matching rule 110 paonn2=ADD1C2#################
drop_matched("s3b_tran", "taba78")
rule("taba79",
  tran = c(
    paonn2 = "translate(paon, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./, ', '')"
  ),
  by = c(paonn2 = "addressfinal")
)

###############matching rule 111 paonf1streetn5=ADD12C#################
drop_matched("s3b_tran", "taba79")
rule("taba80",
  tran = c(
    paonfl1 = "'FLAT,' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    paonf1streetn5 = "replace(paonfl1 || ', ' || streetn5, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(paonf1streetn5 = "addressfinal")
)

###############matching rule 112 paonfstreetn6=ADD12C#################
drop_matched("s3b_tran", "taba80")
rule("taba81",
  tran = c(
    paonfl = "'FLAT' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    paonfstreetn6 = "replace(paonfl || ', ' || streetn5, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(paonfstreetn6 = "addressfinal")
)

###############matching rule 113 flpaon3streetn5=ADDC10#################
drop_matched("s3b_tran", "taba81")
rule("taba82",
  tran_where = "regexp_matches(paon, icu('^\\d+')) AND NOT contains(paon, '-')",
  tran = c(
    paonfl = "'FLAT' || ' ' || paon",
    streetn5 = "translate(street, '''./', '')",
    flpaon3streetn5 = "translate(paonfl || ' ' || streetn5, '- ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-,''./ ', '')"
  ),
  by = c(flpaon3streetn5 = "addressfinal"),
  where = "NOT regexp_matches(e.add, icu('\\d+,\\s\\d+'))"
)

###############matching rule 114 paonstreet1=ADD1C#################
drop_matched("s3b_tran", "taba82")
rule("taba83",
  tran = c(
    paonstreet = "paon || ', ' || street",
    paonstreet1 = "replace(paonstreet, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./ ', '')"
  ),
  by = c(paonstreet1 = "addressfinal")
)

###############matching rule 115 paonstreet2=ADD5#################
drop_matched("s3b_tran", "taba83")
make_table("s3b_epc1_2", "s3b_epc",
  where = "postcode IN (SELECT postcode FROM s3b_tran WHERE regexp_matches(paon, icu('^\\d+[A-Z]')))"
)
rule("taba84",
  tran_where = "regexp_matches(paon, icu('^\\d+[A-Z]'))",
  tran = c(
    paonstreet2 = "replace(paon || ' ' || street, ' ', '')"
  ),
  epc_from = "s3b_epc1_2",
  epc_where = "regexp_matches(add1, icu('FLAT\\s[A-Z]$'))",
  epc = c(
    add1ff = "replace(add1, 'FLAT ', '')",
    add263 = "replace(beg2char(add2, ' '), ',', '')",
    add2611 = "add263 || add1ff",
    add264 = "char2end(add2, ' ')",
    addressfinal = "replace(add2611 || add264, ' ', '')"
  ),
  by = c(paonstreet2 = "addressfinal")
)

###############matching rule 116 paonstreet2=apADD1#################
drop_matched("s3b_tran", "taba84")
rule("taba85",
  tran = c(
    paonstreet2 = "replace(paon || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-,''./ ', '')"
  ),
  by = c(paonstreet2 = "addressfinal")
)

###############matching rule 117 paonstreet2=ADD1C2#################
drop_matched("s3b_tran", "taba85")
rule("taba86",
  tran = c(
    paonstreet2 = "replace(paon || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./, ', '')"
  ),
  by = c(paonstreet2 = "addressfinal")
)

###############matching rule 118 paonapstreet2=ADD13C2#################
drop_matched("s3b_tran", "taba86")
rule("taba87",
  tran = c(
    paonap = "'APARTMENT' || ' ' || paon",
    paonapstreet2 = "translate(paonap || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ' ' || add3, ', ', '')"
  ),
  by = c(paonapstreet2 = "addressfinal")
)

###############matching rule 119 paonstreet3=ADDr66#################
drop_matched("s3b_tran", "taba87")
rule("taba88",
  tran = c(
    paonstreet1 = "paon || ' ' || street",
    paonstreet3 = "translate(paonstreet1, ', ', '')"
  ),
  epc_where = "contains(add, ',')",
  epc = c(
    add61 = "beg2char(add, ',')",
    add62 = "translate(char2end(add, ','), '-''./', '')",
    adda66 = "add61 || ', ' || add62",
    addressfinal = "translate(adda66, ', ', '')"
  ),
  by = c(paonstreet3 = "addressfinal")
)
drop_matched("s3b_tran", "taba88")

###############combine the results of matching rules 74 to 119################

#casa4 is the linked result from matching rules 74 to 119
end_stage("3b")

# ------------------stage 4------------------------------
###############prepare for stage 4 #############
start_stage("4")
drop_tables("s3b_tran", "s3b_epc", "s3b_epc1", "s3b_epc1_2")
load_table("s4_tran", "select * from tranbin3")
load_table(
  "s4_epc",
  "select add1, add2, add3, add, postcode, propertytype, id
   from epc
   where postcode in (select distinct postcode from tranbin3)"
)

####### correct the unmatched address components in EPCs###
fix("s4_epc", "AL7 3EQ", "add", "SALVISBURG COURT", "SALVISBERG COURT")
fix("s4_epc", "AL1 3GL", "add1", "WHITELY COURT", "WHITLEY COURT")
fix("s4_epc", "B1 1LR", "add", "CONCORDE HOUSE", "CONCORD HOUSE")
fix("s4_epc", "B38 8BH", "add", "ST. NICOLAS GARDENS", "ST NICHOLAS GARDENS")
fix("s4_epc", "B5 6AB", "add", "BROSMGROVE STREET", "BROMSGROVE STREET")
fix("s4_epc", "B16 8FT", "add", "SHERBOURNE STREET", "SHERBORNE STREET")
fix("s4_epc", "BB5 1NA", "add", "ST. JAMES COURT", "ST JAMES COURT WEST")
fix("s4_epc", "B5 7SE", "add", "THE BOULVARD", "THE BOULEVARD")
fix("s4_epc", "B73 6AD", "add", "KING EDWARD SQUARE", "KING EDWARDS SQUARE")
fix("s4_epc", "BH12 1HS", "add", "OVERBURY MANOR", "OVERBERRY MANOR")
fix("s4_epc", "BB3 0LB", "add", "CARR HALL COTTAGE", "CARR HALL COTTAGES")
fix("s4_epc", "BS2 0EH", "add", "THE OLD DRILL HALL", "THE DRILL HALL")
fix("s4_epc", "BS5 7HU", "add", "FROST & REED HOUSE", "FROST AND REED HOUSE")
fix("s4_epc", "BA1 6AX", "add", "GROVENOR PLACE", "GROSVENOR PLACE")
fix("s4_epc", "CM12 0BQ", "add", "LANGTHORNS", "LANGTHORNES")
fix("s4_epc", "CB8 8AB", "add", "THE GALLOPS", "NORTH LODGE")
fix("s4_epc", "CO14 8JZ", "add", "NAZE APARTMENTS", "NAZE REACH")
fix("s4_epc", "CV1 5PE", "add", "CAISTOR HALL", "CAISTER HALL")
fix("s4_epc", "CW12 1QQ", "add", "EDWARDS MILL", "EDWARD MILL")
fix("s4_epc", "CO11 1AL", "add1", "NO 1 THE MALTINGS", "1 THE MALTINGS")
fix("s4_epc", "CO11 1AL", "add", "NO 1 THE MALTINGS", "1 THE MALTINGS")
fix("s4_epc", "CO2 8TN", "add", "CAROLYNE COURT", "CAROLYN COURT")
fix("s4_epc", "CT11 9NH", "add", "CANONBURY ROAD", "CANNONBURY ROAD")
fix("s4_epc", "CA11 0XB", "add", "WHITBARROW VILLAGE", "WHITBARROW")
fix("s4_epc", "CA11 0XB", "add", "KIRKSTONE COTTAGES", "KIRKSTONE COTTAGE")

##more complex
fix("s4_epc", "DA11 0BX", "add", "MELBOURNE QUAYS", "MELBOURNE QUAY")
fix("s4_epc", "DN34 4ST", "add", "WESTLANDS,", "WESTLANDS HOUSE,")
fix("s4_epc", "FY4 4FN", "add", "HAWESIDE LANE", "HAWES SIDE LANE")
fix("s4_epc", "IG1 2FJ", "add", "ICON BUILDINGS", "ICON BUILDING")
fix("s4_epc", "GL54 1EJ", "add", "NEWLANDS", "NEWLANDS HOUSE")
fix("s4_epc", "GL54 1EJ", "add2", "NEWLANDS", "NEWLANDS HOUSE")

#half and half need keep, ap to flat
fix("s4_epc", "HD7 5UN", "add1", "TITANIC MILLS", "TITANIC MILL")
fix("s4_epc", "HR8 1BY", "add", "LEADON BANK CENTRE", "LEADON BANK")
fix("s4_epc", "HU15 2QW", "add2", "CLAYTON FOLD", "CLAYTONS FOLD")
fix("s4_epc", "HA9 7HJ", "add", "CRESWELL HOUSE", "CRESSWELL HOUSE")
fix("s4_epc", "LE5 2LZ", "add", "RAILWAY COTTAGES", "THE RAILWAY COTTAGES")
fix("s4_epc", "LE5 2LZ", "add", "THE THE RAILWAY COTTAGES", "THE RAILWAY COTTAGES")
fix("s4_epc", "LS9 8BT", "add", "ST. PETERS SQUARE", "ST PETER'S STREET")

# half and half, 2019 write right
fix("s4_epc", "L18 8BP", "add", "BRUCKLAY HOUSE", "BRUCKLEY HOUSE")

#half right
fix("s4_epc", "L8 7NZ", "add", "FAULKNER SQUARE", "FALKNER SQUARE")

#one wrong
fix("s4_epc", "LA23 3GQ", "add", "MARTINS COURT", "ST MARTINS COURT")
fix("s4_epc", "LE2 3JB", "add", "THE STONEY GATE GRANGE", "STONEYGATE GRANGE")
fix("s4_epc", "LE5 1BH", "add", "KESTRAL LANE", "KESTREL LANE")
fix("s4_epc", "M3 7BY", "add", "BLOCK 7", "BLOCK 7 SPECTRUM")
fix("s4_epc", "M3 7BY", "add", "BLOCK 7 SPECTRUM SPECTRUM", "BLOCK 7 SPECTRUM")
fix("s4_epc", "M9 7HQ", "add", "FRESHFIELD", "FRESHFIELDS")
fix("s4_epc", "N1 7SH", "add", "ROYLE BUILDING", "ROYLE HOUSE")
fix("s4_epc", "M3 7DZ", "add", "BLOCK 9,", "BLOCK 9 SPECTRUM,")
fix("s4_epc", "M3 7EE", "add", "BLOCK 11,", "BLOCK 11 SPECTRUM,")
fix("s4_epc", "N20 0RX", "add", "PARKWOOD FLATS,", "PARKWOOD,")
fix("s4_epc", "N1 0GL", "add", "KINGS QUARTER, 170", "KINGS QUARTER APARTMENTS, 170")
fix("s4_epc", "NW9 5UF", "add", "GABRIEL CLOSE", "GABRIEL COURT")
fix("s4_epc", "HD3 4ZW", "add1", "QUARRY BANK MILL", "QUARRY BANK")
fix("s4_epc", "OX16 5DR", "add", "HOLLIES COURT,", "THE HOLLIES COURT,")
fix("s4_epc", "OX2 6AQ", "add", "FOUNDRY HOUSE", "FOUNDRY HOUSE EAGLE WORKS")
fix("s4_epc", "PO12 4BT", "add", "IRWIN HEIGTS", "IRWIN HEIGHTS")
fix("s4_epc", "PE1 4YS", "add", "CATHEDRAL GREEN COURT", "CATHEDRAL GREEN")
fix("s4_epc", "PO1 3SZ", "add", "THE CRESCENT", "THE CRESCENT BUILDING")
fix("s4_epc", "RG10 9GA", "add", "OLD SILK MILL, ", "THE OLD SILK MILL,")
fix("s4_epc", "SE1 2YE", "add", "BUTLERS WHARF,", "BUTLERS WHARF BUILDING,")
fix("s4_epc", "SW7 1RH", "add", "THE KINIGHTSBRIDGE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIDGE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIDGE APPARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIGDE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIGE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTBRIDGE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIDGE APRTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "KNIGHTSBRIDGE APPARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIDGE APARTMENTS, 119", ", 199")
fix("s4_epc", "SW7 1RH", "add", "THE KNIGHTSBRIDGE, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", "KNIGHTSBRIDGE APARTMENTS, 199", ", 199")
fix("s4_epc", "SW7 1RH", "add", ",", " ")
fix("s4_epc", "SE1 4PF", "add", "MADISON APARTMENTS, 5-27", "THE MADISON, 5 - 27")
fix("s4_epc", "SW1V 3JL", "add", "THE PANORAMIC, 152", ", 152")
fix("s4_epc", "SW11 3YL", "add", "IMONTEVETRO, 100", "MONTEVETRO BUILDING, 100")
fix("s4_epc", "S10 5DQ", "add", "KING COURT,", "KINGS COURT,")
fix("s4_epc", "SW4 7EF", "add", "GREENWAY APARTMENTS", "GREENAWAY APARTMENTS")
fix("s4_epc", "SL6 8DJ", "add", "KESTRAL COURT", "KESTREL COURT")
fix("s4_epc", "SW1X 0DH", "add", "LENOX GARDENS", "LENNOX GARDENS")
fix("s4_epc", "SK9 2AS", "add", "OAK LAWNS,", "OAK LAWN,")
fix("s4_epc", "SE16 7BT", "add", "FAIRMOUNT HOUSE,", "FAIRMONT HOUSE,")
fix("s4_epc", "SE8 4HN", "add", "MILLBANK LANE,", "MILL LANE")
fix("s4_epc", "SE8 4HP", "add", "MILLBANK LANE,", "MILL LANE")
fix("s4_epc", "SG4 9SB", "add", "BALIOL CHAMBERS", "BALLIOL CHAMBERS")
fix("s4_epc", "SO41 0WR", "add", "HOMEGRANGE,", "HOMEGRANGE HOUSE,")
fix("s4_epc", "SE16 3AD", "add", "CROWN PLACE APARTMENTS,", "CROWN PLACE,")
fix("s4_epc", "KT5 8DG", "add", "GUILDFORD AVENUE", "GUILFORD AVENUE")
fix("s4_epc", "TN22 1JT", "add", "SHAFTSBURY COURT", "SHAFTESBURY COURT")
fix("s4_epc", "TW10 7PE", "add", "SECRETT HOUSE", "SECRETTS HOUSE")
fix("s4_epc", "TN34 1JN", "add", "QUEENS APARTMENTS,", "THE QUEENS APARTMENTS,")
fix("s4_epc", "TR13 9JR", "add", "THE MONTEREY, ", "MONTEREY,")
fix("s4_epc", "TN38 0LU", "add", "BOSCOBLE ROAD", "BOSCOBEL ROAD")
fix("s4_epc", "WR10 1EH", "add", "REDLAND HOUSE", "REDLANDS HOUSE")
fix("s4_epc", "EN1 2PL", "add", "ASPLEY HOUSE", "APSLEY HOUSE")
fix("s4_epc", "E3 3TU", "add", "GREENFELL COURT", "GRENFELL COURT")
fix("s4_epc", "HR9 6AW", "add", "OLD STABLES", "THE OLD STABLES")
fix("s4_epc", "HA2 0ES", "add", "EAST CROFT,", "EAST CROFT HOUSE,")
fix("s4_epc", "IP4 1FP", "add", "THE FOUNDRY", "FOUNDRY")

###############matching rule 120 saonpaonstreet2=ADDRE###########################
rule("taba89",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal")
)

###############matching rule 121 saonpaonstreet2=ADD12###########################
drop_matched("s4_tran", "taba89")
rule("taba90",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal")
)

###############matching rule 122 saonpaonstreetn=ADDC ###########################
drop_matched("s4_tran", "taba90")
rule("taba91",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn = "replace(trim_ws(saonpaonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaonstreetn = "addressfinal")
)

###############matching rule 123 saonpaon65street=ADD12C ###########################
drop_matched("s4_tran", "taba91")
rule("taba92",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon65 = "last_token(paon)",
    saonpaon65 = "saon || ', ' || paon65",
    saonpaon65street = "replace(saonpaon65 || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaon65street = "addressfinal")
)

###############matching rule 124 saonpaon62cstreetn2=ADD13C ###########################
drop_matched("s4_tran", "taba92")
rule("taba93",
  tran = c(
    streetn = "replace(street, '''', '')",
    paon62c = "char2end(paon, ',')",
    saonpaon6 = "saon || ', ' || paon62c",
    saonpaon62cstreetn2 = "replace(trim_ws(saonpaon6 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, '''./ ', '')"
  ),
  by = c(saonpaon62cstreetn2 = "addressfinal")
)

###############matching rule 125 saonpaonstreetn=ADD6###########################
drop_matched("s4_tran", "taba93")
rule("taba941",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn = "replace(trim_ws(saonpaonn || ' ' || streetn), ' ', '')"
  ),
  epc_where = "contains(add3, ',')",
  epc = c(
    add361 = "beg2char(add3, ',')",
    addressfinal = "translate(add1 || ', ' || add2 || ', ' || add361, '''./ ', '')"
  ),
  by = c(saonpaonstreetn = "addressfinal")
)

###############matching rule 126 saonpaonstreetn=ADDCC#################
rule("taba9411",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn = "saonn || ', ' || paonn",
    saonpaonstreetn = "replace(trim_ws(saonpaonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(saonpaonstreetn = "addressfinal")
)

###############matching rule 127 saonpaon61xstreet=ADD12C2#################
rule("taba942",
  tran = c(
    paon61x = "beg2char(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61x",
    saonpaon61street = "translate(saonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ',' || add2, ', ', '')"
  ),
  by = c(saonpaon61street = "addressfinal")
)

###############matching rule 128 saonpaon61xstreet=ADDREC#################
rule("taba943",
  tran = c(
    paon61x = "beg2char(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61x",
    saonpaon61street = "translate(saonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(saonpaon61street = "addressfinal")
)

###############matching rule 129 saonpaon62cstreetn=ADD7##########
rule("taba9431",
  tran = c(
    streetn = "replace(street, '''', '')",
    paon62c = "char2end(paon, ',')",
    saonpaon62 = "saon || ' ' || paon62c",
    saonpaon62streetn = "replace(trim_ws(saonpaon62 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    add161 = "beg2char(add1, ',')",
    add12com = "replace(add161 || ' ' || add2, ' ', '')"
  ),
  by = c(saonpaon62streetn = "add12com")
)

###############matching rule 130 saonpaonstreet1=ADD13C2#################
rule("taba944",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "translate(trim_ws(saonpaon || ', ' || street), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, ', ', '')"
  ),
  by = c(saonpaonstreet1 = "addressfinal")
)

###############matching rule 131 saonpaon1=ADD1C9#################
rule("taba945",
  tran_where = "NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    saonpaon1 = "replace(saon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1, ' ', '')"
  ),
  by = c(saonpaon1 = "addressfinal")
)

###############matching rule 132 saonpaonn=ADDC4#################
rule("taba9451",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonpaonn = "replace(saonn || ', ' || paonn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(saonpaonn = "addressfinal")
)

###############matching rule 133 paonstreetn=ADDC4 #################
# The R version builds tab120 (the join), then tab121 (saon FLAT), tab122/tab123 (the other
# transactions, filtered) and tab124 (saon APARTMENT), and combines them into taba9452
rule("taba9452",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    paonstreetn = "replace(paonn || ', ' || streetn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(paonstreetn = "addressfinal"),
  where = "t.saon = 'FLAT'
      OR (t.saon <> 'FLAT'
          AND NOT contains(t.saon, 'FLOOR')
          AND NOT contains(t.saon, 'UPPER')
          AND NOT contains(t.saon, 'BASEMENT')
          AND NOT contains(t.saon, 'LOWER')
          AND NOT regexp_matches(t.saon, icu('FLAT \\d'))
          AND NOT regexp_matches(t.saon, icu('FLAT \\w'))
          AND NOT contains(t.saon, 'FLAT')
          AND NOT regexp_matches(t.saon, icu('-*\\d+\\.*\\d*'))
          AND t.propertytype <> 'F'
          AND e.propertytype <> 'Flat'
          AND e.propertytype <> 'Maisonette')
      OR (t.saon = 'APARTMENT' AND e.propertytype IN ('Flat', 'Maisonette'))"
)
drop_matched("s4_tran", "taba941")
drop_matched("s4_tran", "taba9411")
drop_matched("s4_tran", "taba942")
drop_matched("s4_tran", "taba943")
drop_matched("s4_tran", "taba9431")
drop_matched("s4_tran", "taba944")
drop_matched("s4_tran", "taba945")
drop_matched("s4_tran", "taba9451")
drop_matched("s4_tran", "taba9452")

###############matching rule 134 saon2paon61street= ADDCC ################
rule("taba9453",
  tran_where = "contains(paon, ',') AND propertytype = 'F'",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon61 = "saon2 || ' ' || paon61",
    saon2paon61street = "replace(saon2paon61 || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(saon2paon61street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 135 saonpaonn=ADD12C#################
drop_matched("s4_tran", "taba9453")
rule("taba9454",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonpaonn = "replace(saonn || ', ' || paonn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonn = "addressfinal")
)

###############matching rule 136 flsaonpaonstreet0=ADD#################
drop_matched("s4_tran", "taba9454")
rule("taba946",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d+'))",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    flsaonpaon = "flsaon || ', ' || paon",
    flsaonpaonstreet = "flsaonpaon || ', ' || street"
  ),
  by = c(flsaonpaonstreet = "add")
)

###############matching rule 137 flsaon1paonstreetn2=ADDCC#################
drop_matched("s4_tran", "taba946")
rule("taba947",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d+'))",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn2 = "translate(street, '-''', '')",
    flsaon1 = "'FLAT' || ' ' || saonn",
    flsaon1paonn = "flsaon1 || ', ' || paonn",
    flsaon1paonstreetn2 = "replace(flsaon1paonn || ', ' || streetn2, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-''./ ', '')"
  ),
  by = c(flsaon1paonstreetn2 = "addressfinal")
)

###############matching rule 138 flsaonpaonstreet1= ADDREC#################
drop_matched("s4_tran", "taba947")
rule("taba948",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d+'))",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    flsaonpaon = "flsaon || ' ' || paon",
    flsaonpaonstreet1 = "translate(flsaonpaon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(flsaonpaonstreet1 = "addressfinal")
)

###############matching rule 139 flsaonpaon62street1 = ADDREC#################
drop_matched("s4_tran", "taba948")
rule("taba949",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d+')) AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    flsaon = "'FLAT' || ' ' || saon",
    flsaonpaon62 = "flsaon || ' ' || paon62",
    flsaonpaon62street1 = "translate(flsaonpaon62 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(flsaonpaon62street1 = "addressfinal")
)

###############matching rule 140  fldsaonpaonstreet1=ADDREC#################
drop_matched("s4_tran", "taba949")
rule("taba950",
  tran_where = "propertytype = 'F'
      AND contains(saon, 'FLAT ')
      AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    fldsaon = "replace(saon, 'FLAT ', '')",
    fldsaonpaon = "fldsaon || ' ' || paon",
    fldsaonpaonstreet1 = "translate(fldsaonpaon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(fldsaonpaonstreet1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 141 saon7paonstreet1=ADDRE#################
drop_matched("s4_tran", "taba950")
rule("taba951",
  tran_where = "propertytype = 'F' AND contains(saon, 'FLAT ')",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon = "saon7 || ', ' || paon",
    saon7paonstreet1 = "replace(saon7paon || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saon7paonstreet1 = "addressfinal")
)

###############matching rule 142 saon7paonstreet2=ADDREC#################
drop_matched("s4_tran", "taba951")
rule("taba952",
  tran_where = "propertytype = 'F' AND contains(saon, 'FLAT ')",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon = "saon7 || ' ' || paon",
    saon7paonstreet2 = "translate(saon7paon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(saon7paonstreet2 = "addressfinal")
)

###############matching rule 143 apsaonpaonstreet1=ADDREC#################
drop_matched("s4_tran", "taba952")
rule("taba953",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(saon, icu('^\\d'))",
  tran = c(
    apsaon = "'APARTMENT' || ' ' || saon",
    apsaonpaon = "apsaon || ' ' || paon",
    apsaonpaonstreet1 = "translate(apsaonpaon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(apsaonpaonstreet1 = "addressfinal")
)

###############matching rule 144 saon7paonstreet2=ADD12C2 #################
drop_matched("s4_tran", "taba953")
rule("taba954",
  tran_where = "propertytype = 'F' AND contains(saon, 'FLAT ')",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon = "saon7 || ' ' || paon",
    saon7paonstreet2 = "translate(saon7paon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ',' || add2, ', ', '')"
  ),
  by = c(saon7paonstreet2 = "addressfinal")
)

###############matching rule 145 apsaonpaonstreet1=ADD12C2#################
drop_matched("s4_tran", "taba954")
rule("taba955",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d'))",
  tran = c(
    apsaon = "'APARTMENT' || ' ' || saon",
    apsaonpaon = "apsaon || ' ' || paon",
    apsaonpaonstreet1 = "translate(apsaonpaon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ',' || add2, ', ', '')"
  ),
  by = c(apsaonpaonstreet1 = "addressfinal")
)

###############matching rule 146 saon7paonstreetn=ADDC4#################
drop_matched("s4_tran", "taba955")
rule("taba956",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon71 = "replace(saon7, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon7paonn = "saon71 || ', ' || paonn",
    saon7paonstreetn = "replace(trim_ws(saon7paonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(saon7paonstreetn = "addressfinal")
)

###############matching rule 147 saon7paonn=ADD12C4#################
drop_matched("s4_tran", "taba956")
rule("taba957",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    paonn = "translate(paon, '''.', '')",
    saon7paonn = "replace(saon7 || ', ' || paonn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '-./ ', '')"
  ),
  by = c(saon7paonn = "addressfinal")
)

###############matching rule 148 saon4paonstreetn=ADDC4#################
drop_matched("s4_tran", "taba957")
rule("taba958",
  tran = c(
    fldsaon1 = "replace(saon, 'FLAT ', '')",
    saonn4 = "replace(fldsaon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon4paonn = "saonn4 || ', ' || paonn",
    saon4paonstreetn = "replace(trim_ws(saon4paonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(saon4paonstreetn = "addressfinal")
)

###############matching rule 149 apsaonpaon6streetn=ADDC4#################
drop_matched("s4_tran", "taba958")
rule("taba961",
  tran_where = "contains(paon, ',')",
  tran = c(
    streetn = "replace(street, '''', '')",
    paon62 = "char2end(paon, ',')",
    apsaon = "'APARTMENT' || ' ' || saon",
    apsaonpaon6 = "apsaon || ', ' || paon62",
    apsaonpaon6streetn = "replace(trim_ws(apsaonpaon6 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(apsaonpaon6streetn = "addressfinal")
)

###############matching rule 150 flsaonpaonstreetn=ADDC4#################
drop_matched("s4_tran", "taba961")
rule("taba962",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    flsaonpaon = "flsaon || ', ' || paonn",
    flsaonpaonstreetn = "replace(trim_ws(flsaonpaon || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(flsaonpaonstreetn = "addressfinal")
)

###############matching rule 151 saon4paonstreetn3=ADDC5#################
drop_matched("s4_tran", "taba962")
rule("taba963",
  tran_where = "NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    saon4 = "replace(saon, 'FLAT ', '')",
    saonn4 = "replace(saon4, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon4paonn = "saonn4 || ' ' || paonn",
    saon4paonstreetn3 = "replace(trim_ws(saon4paonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, './ ', '')"
  ),
  by = c(saon4paonstreetn3 = "addressfinal")
)

###############matching rule 152 saon4paonstreetn4=ADD12C#################
drop_matched("s4_tran", "taba963")
rule("taba964",
  tran = c(
    saon4 = "replace(saon, 'FLAT ', '')",
    saonn4 = "replace(saon4, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon4paonn = "saonn4 || ', ' || paonn",
    saon4paonstreetn4 = "replace(trim_ws(saon4paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon4paonstreetn4 = "addressfinal")
)

###############matching rule 153  saon4paonstreetn1=ADD12C#################
drop_matched("s4_tran", "taba964")
rule("taba965",
  tran_where = "propertytype = 'F'",
  tran = c(
    saon4 = "replace(saon, 'FLAT ', '')",
    saonn4 = "replace(saon4, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon4paonn1 = "saonn4 || ' ' || paonn",
    saon4paonstreetn1 = "replace(trim_ws(saon4paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon4paonstreetn1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)
drop_matched("s4_tran", "taba965")

###############matching rule 154  saon1paonstreetn=ADDC#################
rule("taba94",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ' ' || paonn",
    saon1paonstreetn = "replace(trim_ws(saon1paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon1paonstreetn = "addressfinal")
)

###############matching rule 155 saon1paonstreetn=ADD12C###########################
drop_matched("s4_tran", "taba94")
rule("taba95",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ' ' || paonn",
    saon1paonstreetn = "replace(trim_ws(saon1paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon1paonstreetn = "addressfinal")
)

###############matching rule 156  saon1paonstreetn1=ADDC ####################
drop_matched("s4_tran", "taba95")
rule("taba96",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn1 = "saonn1 || ', ' || paonn",
    saon1paonstreetn1 = "replace(trim_ws(saon1paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon1paonstreetn1 = "addressfinal")
)

###############matching rule 157  saon1paonstreetn1=ADD12C####################
drop_matched("s4_tran", "taba96")
rule("taba97",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn1 = "saonn1 || ', ' || paonn",
    saon1paonstreetn1 = "replace(trim_ws(saon1paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon1paonstreetn1 = "addressfinal")
)

###############matching rule 158  saon1paonstreetn2=ADDC3 ####################
drop_matched("s4_tran", "taba97")
rule("taba98",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ' ' || paonn",
    saon1paonstreetn2 = "translate(trim_ws(saon1paonn || ' ' || streetn), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ',''./ ', '')"
  ),
  by = c(saon1paonstreetn2 = "addressfinal")
)

###############matching rule 159  saon1paonstreetn2=ADD12C1####################
drop_matched("s4_tran", "taba98")
rule("taba99",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn1 || ' ' || paonn)",
    saon1paonstreetn2 = "replace(trim_ws(saonpaonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(saon1paonstreetn2 = "addressfinal")
)

###############matching rule 160 saon2paon61street=ADD12C ####################
drop_matched("s4_tran", "taba99")
rule("taba100",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon61 = "saon2 || ' ' || paon61",
    saon2paon61street = "replace(saon2paon61 || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paon61street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 161  saon2paonstreetn3=ADDC####################
rule("taba101",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn3 = "replace(trim_ws(saon2paonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon2paonstreetn3 = "addressfinal")
)

###############matching rule 162  saon2paonstreetn3=ADD12C####################
rule("taba102",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn3 = "replace(trim_ws(saon2paonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paonstreetn3 = "addressfinal")
)

###############matching rule 163 saon2paonstreetn2=ADDC####################
rule("taba103",
  tran_where = "contains(saon, 'APARTMENT ')
      AND propertytype = 'F'
      AND NOT regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn2 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon2paonstreetn2 = "addressfinal")
)

###############matching rule 164 saon2paonstreetn2=ADD12C ####################
rule("taba104",
  tran_where = "contains(saon, 'APARTMENT ')
      AND propertytype = 'F'
      AND NOT regexp_matches(paon, icu('^\\d+'))",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn2 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paonstreetn2 = "addressfinal")
)

###############matching rule 165 saonn2paonn1=ADDC ####################
rule("taba105",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonn2paonn1 = "replace(saonn2 || ' ' || paonn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonn2paonn1 = "addressfinal")
)
drop_matched("s4_tran", "taba100")
drop_matched("s4_tran", "taba101")
drop_matched("s4_tran", "taba102")
drop_matched("s4_tran", "taba103")
drop_matched("s4_tran", "taba104")
drop_matched("s4_tran", "taba105")

###############matching rule 166  saonpaon62street=ADD12C####################
rule("taba107",
  tran_where = "contains(saon, 'APARTMENT ') AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    saonpaon6 = "saon || ', ' || paon62",
    saonpaon62street = "replace(saonpaon6 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaon62street = "addressfinal")
)

###############matching rule 167  saon1paonstreet6n1=ADD12C####################
drop_matched("s4_tran", "taba107")
rule("taba108",
  tran_where = "contains(saon, 'APARTMENT ') AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    saon1 = "replace(replace(saon, 'APARTMENT ', 'FLAT '), '/', '')",
    saonpaonn6 = "saon1 || ', ' || paon62",
    saon1paonstreet6n1 = "replace(saonpaonn6 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon1paonstreet6n1 = "addressfinal")
)

###############matching rule 168 saon2paonstreetn=ADD12C ####################
drop_matched("s4_tran", "taba108")
rule("taba109",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonn2paon1 = "saonn2 || ', ' || paonn",
    saon2paonstreetn = "replace(saonn2paon1 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paonstreetn = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 169 saonn3paonnstreet=ADD13C ####################
drop_matched("s4_tran", "taba109")
rule("taba110",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saonn3 = "translate(saon, './', '')",
    paonn = "translate(paon, '''.', '')",
    saonn3paonn = "saonn3 || ', ' || paonn",
    saonn3paonnstreet = "replace(saonn3paonn || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, '''./ ', '')"
  ),
  by = c(saonn3paonnstreet = "addressfinal")
)

###############matching rule 170 saonn2paonn1streetn=ADDC ####################
drop_matched("s4_tran", "taba110")
rule("taba111",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonn2paonn1 = "saonn2 || ', ' || paonn",
    saonn2paonn1streetn = "replace(saonn2paonn1 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonn2paonn1streetn = "addressfinal")
)

###############matching rule 171  paon62saonpstreet= ADDREC####################
drop_matched("s4_tran", "taba111")
rule("taba112",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    paon62saon = "paon62 || ' ' || saon",
    paon62saonp = "paon62saon || ' ' || paon61",
    paon62saonpstreet = "translate(trim_ws(paon62saonp || ', ' || street), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(paon62saonpstreet = "addressfinal")
)

###############matching rule 172 saonpaon62streetn1=ADDC ####################
drop_matched("s4_tran", "taba112")
rule("taba113",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    paon62 = "char2end(paonn, ',')",
    saonpaon62 = "saon || ', ' || paon62",
    saonpaon62streetn1 = "replace(trim_ws(saonpaon62 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonpaon62streetn1 = "addressfinal")
)

###############matching rule 173 saon1paonstreet6n=ADD12C####################
drop_matched("s4_tran", "taba113")
rule("taba114",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    paon62c = "char2end(paonn, ',')",
    saon1paonn6 = "saonn1 || ', ' || paon62c",
    saon1paonstreet6n = "replace(trim_ws(saon1paonn6 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon1paonstreet6n = "addressfinal")
)

###############matching rule 174  saon2paonstreetn4=ADDC####################
drop_matched("s4_tran", "taba114")
rule("taba115",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ', ' || paonn)",
    saon2paonstreetn4 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon2paonstreetn4 = "addressfinal")
)

###############matching rule 175 saon2paonstreetn4=ADD12C ####################
drop_matched("s4_tran", "taba115")
rule("taba116",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ', ' || paonn)",
    saon2paonstreetn4 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paonstreetn4 = "addressfinal")
)

###############matching rule 176 saon2paonstreetn4=ADD1num2####################
drop_matched("s4_tran", "taba116")
rule("taba118",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "saonn2 || ', ' || paonn",
    saon2paonstreetn4 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    add1num = "numextract(add1)",
    addressfinal = "translate(add1num || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon2paonstreetn4 = "addressfinal"),
  where = "NOT regexp_matches(e.add1, icu('\\d+(A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T)'))"
)

###############matching rule 177  saon5paonstreetn1=ADDC####################
drop_matched("s4_tran", "taba118")
rule("taba120",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon5 = "replace(saon, 'APARTMENT ', 'UNIT ')",
    saonn5 = "replace(saon5, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon5paonn = "saonn5 || ' ' || paonn",
    saon5paonstreetn1 = "replace(trim_ws(saon5paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon5paonstreetn1 = "addressfinal")
)

###############matching rule 178 paonsaon2streetn=ADD1C ####################
drop_matched("s4_tran", "taba120")
rule("taba122",
  tran_where = "contains(saon, 'APARTMENT ')
      AND propertytype = 'F'
      AND regexp_matches(saon, icu('APARTMENT\\s[A-Z]'))",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    paonsaon2 = "paonn || ' ' || saonn2",
    paonsaon2streetn = "replace(trim_ws(paonsaon2 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, '''./ ', '')"
  ),
  by = c(paonsaon2streetn = "addressfinal")
)

###############matching rule 179 saon2paonstreetn2=ADD13C ####################
drop_matched("s4_tran", "taba122")
rule("taba124",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn2 = "replace(trim_ws(saon2paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add3, '''./ ', '')"
  ),
  by = c(saon2paonstreetn2 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 180  saonpaon66street=ADDC6####################
drop_matched("s4_tran", "taba124")
rule("taba125",
  tran_where = "contains(saon, 'APARTMENT ') AND contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    saonpaon62 = "saon || ', ' || paon62",
    saonpaon66 = "saonpaon62 || ' ' || paon61",
    saonpaon66street = "translate(saonpaon66 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(replace(add, '''', ''), ', ', '')"
  ),
  by = c(saonpaon66street = "addressfinal")
)

###############matching rule 181 saon1paonstreetn3=ADD12C ####################
drop_matched("s4_tran", "taba125")
rule("taba126",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ', ' || paonn",
    saon1paonstreetn3 = "replace(trim_ws(saon1paonn || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saon1paonstreetn3 = "addressfinal")
)

###############matching rule 182  saon2street=ADDC ####################
drop_matched("s4_tran", "taba126")
rule("taba127",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2street = "replace(saon2 || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon2street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 183 saon1paonstreet=ADDRE###########################
drop_matched("s4_tran", "taba127")
rule("taba128",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saon1paon = "saon1 || ', ' || paon",
    saon1paonstreet = "replace(trim_ws(saon1paon || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saon1paonstreet = "addressfinal")
)

###############matching rule 184 saon2paonlo=ADDRE###########################
drop_matched("s4_tran", "taba128")
rule("taba129",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon = "saon2 || ' ' || paon",
    saon2paonlo = "replace(saon2paon || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saon2paonlo = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 185 saon1paon=ADD12###########################
drop_matched("s4_tran", "taba129")
rule("taba130",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    sao1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saon1paon = "replace(sao1 || ', ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon1paon = "addressfinal")
)

###############matching rule 186 saon1paon61street=ADD12###########################
drop_matched("s4_tran", "taba130")
rule("taba131",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    sao1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    paon61c = "beg2char(paon, ',')",
    saon1paon61 = "sao1 || ' ' || paon61c",
    saon1paon61street = "replace(trim_ws(saon1paon61 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon1paon61street = "addressfinal")
)

###############matching rule 187 saon2paonstreet=ADD12 ###########################
drop_matched("s4_tran", "taba131")
rule("taba132",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype <> 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon = "saon2 || ' ' || paon",
    saon2paonstreet = "replace(saon2paon || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon2paonstreet = "addressfinal"),
  where = "NOT (e.propertytype IN ('Flat', 'Maisonette'))"
)

###############matching rule 188 saon1paon1=ADD1C9###########################
drop_matched("s4_tran", "taba132")
rule("taba133",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    sao1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saon1paon1 = "replace(sao1 || ' ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1, ' ', '')"
  ),
  by = c(saon1paon1 = "addressfinal")
)

###############matching rule 189 saon1paonstreetn2=ADD12C2###########################
drop_matched("s4_tran", "taba133")
rule("taba134",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ' ' || paonn",
    saon1paonstreetn2 = "translate(trim_ws(saon1paonn || ' ' || streetn), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, ', ', '')"
  ),
  by = c(saon1paonstreetn2 = "addressfinal")
)

###############matching rule 190  psaonpaonstreet=ADDREC###########################
drop_matched("s4_tran", "taba134")
rule("taba135",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F'",
  tran = c(
    paon64 = "beg2char(paon, ' ')",
    paon641 = "char2end(paon, ' ')",
    psaon = "paon64 || saon",
    psaonpaon = "psaon || ' ' || paon641",
    psaonpaonstreet = "translate(trim_ws(psaonpaon || ', ' || street), ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(psaonpaonstreet = "addressfinal")
)

###############matching rule 191 saon2paon62street=ADD12###########################
drop_matched("s4_tran", "taba135")
rule("taba136",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    paon62 = "char2end(paon, ',')",
    saon2paon62 = "saon2 || ', ' || paon62",
    saon2paon62street = "replace(saon2paon62 || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon2paon62street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 192 saon2paonstreet=ADD1262 ###########################
drop_matched("s4_tran", "taba136")
rule("taba137",
  tran_where = "contains(saon, 'APARTMENT ')",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon = "saon2 || ' ' || paon",
    saon2paonstreet = "replace(saon2paon || ', ' || street, ' ', '')"
  ),
  epc_where = "contains(add2, ',') AND regexp_matches(add2, icu('\\d+-\\d+'))",
  epc = c(
    add262 = "char2end(add2, ',')",
    add1262 = "add1 || ', ' || add262",
    addressfinal = "replace(add1262, ' ', '')"
  ),
  by = c(saon2paonstreet = "addressfinal")
)

###############matching rule 193 saonpaonstreetn2=ADD7#############
drop_matched("s4_tran", "taba137")
rule("taba138",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saonpaonn1 = "trim_ws(saonn || ' ' || paonn)",
    saonpaonstreetn2 = "replace(trim_ws(saonpaonn1 || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    add161x = "beg2char(add1, ',')",
    addressfinal = "replace(add161x || ' ' || add2, ' ', '')"
  ),
  by = c(saonpaonstreetn2 = "addressfinal")
)

###############matching rule 194 flsaonpaonstreet=add1f61f2#################
drop_matched("s4_tran", "taba138")
rule("taba139",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d')) AND NOT contains(paon, ',')",
  tran = c(
    flsaon = "'FLAT ' || saon",
    flsaonpaon = "flsaon || ', ' || paon",
    flsaonpaonstreet = "translate(flsaonpaon || ', ' || street, ', ', '')"
  ),
  epc_where = "contains(add1, 'FLAT ')",
  epc = c(
    add1f = "replace(add1, 'FLAT ', '')",
    add1f61 = "replace(beg2char(add1f, ' '), ',', '')",
    add1f61f = "'FLAT' || ' ' || add1f61",
    add1f61f2 = "translate(add1f61f || ', ' || add2, ', ', '')"
  ),
  by = c(flsaonpaonstreet = "add1f61f2"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 195 psaon8street=ADDREC#################
drop_matched("s4_tran", "taba139")
rule("taba140",
  tran_where = "regexp_matches(saon, icu('FLAT\\s[A-Z]$')) AND regexp_matches(paon, icu('^\\d'))",
  tran = c(
    fldsaon1 = "replace(saon, 'FLAT ', '')",
    psaon8 = "paon || fldsaon1",
    psaon8street = "translate(psaon8 || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(psaon8street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 196 saonpaonstreet1=add12643#################
drop_matched("s4_tran", "taba140")
rule("taba141",
  tran = c(
    saonpaon = "saon || ', ' || paon",
    saonpaonstreet1 = "replace(trim_ws(saonpaon || ', ' || street), ' ', '')"
  ),
  epc = c(
    add264 = "char2end(add2, ' ')",
    add1264 = "add1 || ', ' || add264",
    add12643 = "replace(add1264 || ', ' || add3, ' ', '')"
  ),
  by = c(saonpaonstreet1 = "add12643"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 197 saonstreet=ADDRE#################
drop_matched("s4_tran", "taba141")
rule("taba142",
  tran_where = "propertytype <> 'F'",
  tran = c(
    saonstreet = "replace(saon || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonstreet = "addressfinal")
)

###############matching rule 198 saonstreetlo= ADDRE#################
drop_matched("s4_tran", "taba142")
rule("taba143",
  tran = c(
    saonstreet = "saon || ', ' || street",
    saonstreetlo = "replace(saonstreet || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonstreetlo = "addressfinal")
)

###############matching rule 199 unsaonpaonstreet2=ADDRE##################
drop_matched("s4_tran", "taba143")
rule("taba144",
  tran_where = "regexp_matches(saon, icu('^\\d'))",
  tran = c(
    unsaon = "'UNIT' || ' ' || saon",
    unsaonpaon1 = "trim_ws(unsaon || ' ' || paon)",
    unsaonpaonstreet2 = "replace(trim_ws(unsaonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(unsaonpaonstreet2 = "addressfinal")
)

###############matching rule 200 flsaonpaonstreet2=ADD8##################
drop_matched("s4_tran", "taba144")
rule("taba145",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon3 = "'FLAT' || ' ' || saon",
    flsaonpaon = "flsaon3 || ' ' || paon",
    flsaonpaonstreet2 = "replace(flsaonpaon || ', ' || street, ' ', '')"
  ),
  epc = c(
    add1c = "replace(add1, '/', '')",
    addressfinal = "replace(add1c || ', ' || add2, ' ', '')"
  ),
  by = c(flsaonpaonstreet2 = "addressfinal")
)

###############matching rule 201 flsaonpaon1=ADD1C9#################
drop_matched("s4_tran", "taba145")
rule("taba146",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d'))",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    flsaonpaon1 = "replace(flsaon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1, ' ', '')"
  ),
  by = c(flsaonpaon1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 202 saonpaon1=fladd#################
drop_matched("s4_tran", "taba146")

# SS1 2HZ
rule("taba147",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonpaon1 = "replace(saon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    fladd = "'FLAT' || ' ' || add",
    addressfinal = "replace(fladd, ' ', '')"
  ),
  by = c(saonpaon1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 203 saonpaon1=fladd1c#################
drop_matched("s4_tran", "taba147")

#SS1 2HZ
rule("taba148",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonpaon1 = "replace(saon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    fladd1 = "'FLAT' || ' ' || add1",
    addressfinal = "replace(trim_ws(fladd1), ' ', '')"
  ),
  by = c(saonpaon1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 204 saonpaonstreet3=fladd#################
drop_matched("s4_tran", "taba148")
rule("taba149",
  tran_where = "propertytype = 'F' AND saon = 'FLAT'",
  tran = c(
    saonpaon = "saon || ' ' || paon",
    saonpaonstreet3 = "replace(saonpaon || ' ' || street, ' ', '')"
  ),
  epc = c(
    fladd = "'FLAT' || ' ' || add",
    addressfinal = "replace(fladd, ' ', '')"
  ),
  by = c(saonpaonstreet3 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 205 saon7paon6street=ADDRE#################
drop_matched("s4_tran", "taba149")
rule("taba150",
  tran_where = "propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon6 = "saon7 || ', ' || paon62",
    saon7paon6street = "replace(trim_ws(saon7paon6 || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saon7paon6street = "addressfinal")
)

###############matching rule 206 saon7paon6street=ADD12#################
drop_matched("s4_tran", "taba150")
rule("taba151",
  tran_where = "propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon6 = "saon7 || ', ' || paon62",
    saon7paon6street = "replace(trim_ws(saon7paon6 || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon7paon6street = "addressfinal")
)

###############matching rule 207 saon8paonstreet2=ADDRE################
drop_matched("s4_tran", "taba151")
rule("taba152",
  tran_where = "regexp_matches(saon, icu('LOFT\\s'))",
  tran = c(
    saon8 = "replace(saon, 'LOFT ', 'FLAT ')",
    saon8paon1 = "trim_ws(saon8 || ' ' || paon)",
    saon8paonstreet2 = "replace(trim_ws(saon8paon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saon8paonstreet2 = "addressfinal")
)

###############matching rule 208 saonpaonstreet2=fladd##################
drop_matched("s4_tran", "taba152")
fix("s4_epc", "LA4 5JL", "add", "QUEENS SQUARE", "QUEEN SQUARE")
rule("taba153",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    fladd = "'FLAT' || ' ' || add",
    addressfinal = "replace(fladd, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 209 paonlo=ADD12##################
drop_matched("s4_tran", "taba153")
rule("taba154",
  tran_where = "regexp_matches(paon, icu('^\\d')) AND street = ''",
  tran = c(
    paonlo = "replace(paon || ', ' || locality, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paonlo = "addressfinal")
)

###############matching rule 210 saonpaonstreet1=adddap ##################
drop_matched("s4_tran", "taba154")
fix("s4_epc", "DE14 3DH", "add", "BRANSTON GREEN,", "BRANSTON GREENS,")
rule("taba155",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonpaon1 = "trim_ws(saon || ', ' || paon)",
    saonpaonstreet1 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    adddap = "replace(add, 'APARTMENT ', '')",
    addressfinal = "replace(adddap, ' ', '')"
  ),
  by = c(saonpaonstreet1 = "addressfinal")
)

###############matching rule 211  saonpaon61xstreet=fladdc #################
drop_matched("s4_tran", "taba155")
rule("taba156",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    paon61x = "beg2char(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61x",
    saonpaon61xstreet = "translate(saonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    fladd = "'FLAT' || ' ' || add",
    addressfinal = "translate(fladd, ', ', '')"
  ),
  by = c(saonpaon61xstreet = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 212 saonpaon2=fladdc##################
drop_matched("s4_tran", "taba156")
rule("taba157",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonpaon2 = "translate(saon || ', ' || paon, ', ', '')"
  ),
  epc = c(
    fladd = "'FLAT' || ' ' || add",
    addressfinal = "translate(fladd, ', ', '')"
  ),
  by = c(saonpaon2 = "addressfinal")
)

###############matching rule 213  saonpaonstreet5 =apadd1632##################
drop_matched("s4_tran", "taba157")
rule("taba158",
  tran_where = "propertytype = 'F' AND contains(saon, 'APARTMENT ')",
  tran = c(
    saonpaon1 = "trim_ws(saon || ', ' || paon)",
    saonpaonstreet5 = "translate(trim_ws(saonpaon1 || ' ' || street), ', ', '')"
  ),
  epc = c(
    add163 = "beg2char(add1, ' ')",
    apadd63 = "'APARTMENT' || ' ' || add163",
    addressfinal = "translate(apadd63 || ', ' || add2, ', ', '')"
  ),
  by = c(saonpaonstreet5 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 214 saonpaonstreet11=ADD12##################
drop_matched("s4_tran", "taba158")
fix("s4_epc", "M9 7HQ", "add", "FRESHFIELDSS", "FRESHFIELD")
rule("taba159",
  tran_where = "propertytype = 'F'",
  tran = c(
    paon11 = "replace(paon, ',', '')",
    saonpaon11 = "trim_ws(saon || ' ' || paon11)",
    saonpaonstreet11 = "replace(saonpaon11 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet11 = "addressfinal")
)

###############matching rules 215 and 216 saonpaon61xstreet=ADD1262C##################
drop_matched("s4_tran", "taba159")

# matching rule 215
rule("taba161",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    paon61x = "beg2char(paon, ',')",
    paon62 = "char2end(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61x",
    saonpaon61xstreet = "translate(saonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    add262 = "char2end(add2, ',')",
    addressfinal = "translate(add1 || ', ' || add262, ', ', '')"
  ),
  by = c(saonpaon61xstreet = "addressfinal"),
  where = "regexp_matches(t.paon62, icu('\\d-\\d'))"
)

# matching rule 216
rule("taba162",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    paon61x = "beg2char(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61x",
    saonpaon61xstreet = "translate(saonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    add262 = "char2end(add2, ',')",
    add261 = "beg2char(add2, ',')",
    addressfinal = "translate(add1 || ', ' || add262, ', ', '')"
  ),
  by = c(saonpaon61xstreet = "addressfinal"),
  where = "regexp_matches(e.add261, icu('\\d-\\d'))"
)

###############matching rule 217 flsaonpaonstreet3=ADD12C5##################
drop_matched("s4_tran", "taba161")
drop_matched("s4_tran", "taba162")
rule("taba163",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon3 = "'FLAT' || ' ' || saon",
    flsaonpaon = "flsaon3 || ' ' || paon",
    flsaonpaonstreet3 = "translate(flsaonpaon || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '., ', '')"
  ),
  by = c(flsaonpaonstreet3 = "addressfinal")
)

###############matching rule 218 flsaonpaonstreet3=ADD12C1##################
drop_matched("s4_tran", "taba163")
rule("taba164",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon3 = "'FLAT' || ' ' || saon",
    flsaonpaon = "flsaon3 || ' ' || paon",
    flsaonpaonstreet3 = "translate(flsaonpaon || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./, ', '')"
  ),
  by = c(flsaonpaonstreet3 = "addressfinal")
)

###############matching rule 219 saonstreet1=ADD1C7##################
drop_matched("s4_tran", "taba164")
rule("taba165",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonstreet1 = "translate(saon || ' ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1, ', ', '')"
  ),
  by = c(saonstreet1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 220 saonpaonstreet1=add1f61f3##################
drop_matched("s4_tran", "taba165")
rule("taba166",
  tran_where = "propertytype = 'F' AND contains(paon, ' - ') AND regexp_matches(paon, icu('^\\d'))",
  tran = c(
    saonpaon1 = "trim_ws(saon || ', ' || paon)",
    saonpaonstreet1 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc_where = "contains(add, '-') AND contains(add1, 'FLAT ')",
  epc = c(
    add1df = "replace(add1, 'FLAT ', '')",
    add1f61 = "replace(beg2char(add1df, ' '), ',', '')",
    add1n = "'FLAT' || ' ' || add1f61",
    addressfinal = "replace(add1n || ', ' || add2, ' ', '')"
  ),
  by = c(saonpaonstreet1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 221 saonpaon62steet=ADDRE##################
drop_matched("s4_tran", "taba166")
rule("taba167",
  tran_where = "propertytype = 'F' AND contains(paon, ' - ') AND contains(paon, ',')",
  tran = c(
    paon62 = "char2end(paon, ',')",
    saonpaon62 = "saon || ', ' || paon62",
    saonpaon62steet = "replace(saonpaon62 || ', ' || street, ' ', '')"
  ),
  epc_where = "contains(add, '-')",
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaon62steet = "addressfinal")
)

###############matching rule 222 saonstreet2=ADD1264##################
drop_matched("s4_tran", "taba167")
rule("taba168",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonstreet2 = "translate(saon || ', ' || street, ', ', '')"
  ),
  epc_where = "regexp_matches(add, icu('^\\d'))",
  epc = c(
    add2641 = "char2end(add2, ',')",
    addressfinal = "translate(add1 || ', ' || add2641, ', ', '')"
  ),
  by = c(saonstreet2 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 223 flsaonpaon61street=ADDREC##################
drop_matched("s4_tran", "taba168")
rule("taba169",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d')) AND contains(paon, ',')",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    paon61 = "beg2char(paon, ',')",
    flsaonpaon61 = "flsaon || ' ' || paon61",
    flsaonpaon61street = "translate(flsaonpaon61 || ', ' || street, ', ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, ', ', '')"
  ),
  by = c(flsaonpaon61street = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 224 saon4paonstreet=ADD12##################
drop_matched("s4_tran", "taba169")
rule("taba170",
  tran_where = "propertytype = 'F' AND contains(saon, 'FLAT ')",
  tran = c(
    saon4 = "replace(saon, 'FLAT ', '')",
    saon4paon = "saon4 || ' ' || paon",
    saon4paonstreet = "replace(trim_ws(saon4paon || ' ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon4paonstreet = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 225 saonpaon61street1=ADD1263##################
drop_matched("s4_tran", "taba170")
rule("taba171",
  tran_where = "propertytype = 'F' AND contains(paon, ',') AND contains(paon, '-')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    saonpaon61 = "saon || ' ' || paon61",
    saonpaon61street1 = "replace(trim_ws(saonpaon61 || ', ' || street), ' ', '')"
  ),
  epc = c(
    add2641 = "char2end(add2, ',')",
    addressfinal = "replace(add1 || ', ' || add2641, ' ', '')"
  ),
  by = c(saonpaon61street1 = "addressfinal")
)

###############matching rule 226 flsaonpaon2=ADDRE##################
drop_matched("s4_tran", "taba171")
rule("taba172",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d'))",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    flsaonpaon2 = "replace(flsaon || ', ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(flsaonpaon2 = "addressfinal")
)

###############matching rule 227 saonpaon3=ADD1##################
drop_matched("s4_tran", "taba172")
fix("s4_epc", "GL52 8LJ", "add", "CHURCHFIELD", "CHURCHFIELDS")
rule("taba173",
  tran = c(
    saonpaon1 = "saon || ', ' || paon"
  ),
  by = c(saonpaon1 = "add1")
)

###############matching rule 228 saonstreet3=ADDC##################
drop_matched("s4_tran", "taba173")
rule("taba174",
  tran_where = "propertytype = 'F'",
  tran = c(
    saonstreet3 = "replace(saon || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saonstreet3 = "addressfinal")
)

###############matching rule 229  flsaonpaon3=ADD12##################
drop_matched("s4_tran", "taba174")
rule("taba175",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon3 = "'FLAT' || ' ' || saon",
    flsaonpaon3 = "replace(flsaon3 || ', ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(flsaonpaon3 = "addressfinal")
)

###############matching rule 230  flsaonpaonstreet4=ADD1263 ##################
drop_matched("s4_tran", "taba175")
rule("taba176",
  tran_where = "propertytype = 'F'",
  tran = c(
    flsaon3 = "'FLAT' || ' ' || saon",
    flsaon3paon = "flsaon3 || ', ' || paon",
    flsaonpaonstreet4 = "replace(flsaon3paon || ', ' || street, ' ', '')"
  ),
  epc = c(
    add2641 = "char2end(add2, ',')",
    addressfinal = "replace(add1 || ', ' || add2641, ' ', '')"
  ),
  by = c(flsaonpaonstreet4 = "addressfinal")
)

###############matching rule 231  saonstreet=ADD1265##################
drop_matched("s4_tran", "taba176")
rule("taba177",
  tran_where = "propertytype = 'F'
      AND NOT regexp_matches(paon, icu('^\\d'))
      AND NOT contains(paon, ',')
      AND NOT regexp_matches(paon, icu('\\d'))",
  tran = c(
    saonstreet = "replace(saon || ', ' || street, ' ', '')"
  ),
  epc = c(
    add264 = "char2end(add2, ' ')",
    addressfinal = "replace(add1 || ', ' || add264, ' ', '')"
  ),
  by = c(saonstreet = "addressfinal"),
  where = "t.postcode IN (SELECT postcode FROM s4_epc GROUP BY postcode HAVING count(DISTINCT add2) = 1)"
)

###############matching rule 232 paonsaonstreet=ADDRE##################
drop_matched("s4_tran", "taba177")
rule("taba178",
  tran = c(
    paonsaon = "paon || saon",
    paonsaonstreet = "replace(paonsaon || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(paonsaonstreet = "addressfinal")
)

###############matching rule 233  saonpaon61=ADD12##################
drop_matched("s4_tran", "taba178")
rule("taba179",
  tran_where = "propertytype = 'F' AND contains(paon, '-') AND contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    saonpaon611 = "replace(saon || ',' || paon61, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saonpaon611 = "addressfinal")
)

###############matching rule 234  saon7paon=ADD12##################
drop_matched("s4_tran", "taba179")
rule("taba180",
  tran_where = "propertytype = 'F'",
  tran = c(
    saon7 = "replace(saon, 'FLAT ', 'APARTMENT ')",
    saon7paon = "replace(saon7 || ', ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon7paon = "addressfinal")
)

###############matching rule 235 paonsaonstreet1=ADD12##################
drop_matched("s4_tran", "taba180")
rule("taba181",
  tran_where = "propertytype = 'F'",
  tran = c(
    paonsaon = "paon || ', ' || saon",
    paonsaonstreet1 = "replace(paonsaon || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(paonsaonstreet1 = "addressfinal")
)

###############matching rule 236 flsaonpaon61street1=ADD12#################
drop_matched("s4_tran", "taba181")
rule("taba182",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d')) AND contains(paon, ',')",
  tran = c(
    flsaon = "'FLAT' || ' ' || saon",
    paon61 = "beg2char(paon, ',')",
    flsaonpaon61 = "flsaon || ' ' || paon61",
    flsaonpaon61street = "flsaonpaon61 || ', ' || street",
    flsaonpaon61street1 = "replace(flsaonpaon61street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(flsaonpaon61street1 = "addressfinal")
)

###############matching rule 237  apsaonpaon=ADD12C6############
drop_matched("s4_tran", "taba182")
rule("taba183",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d'))",
  tran = c(
    apsaon = "'APARTMENT' || ' ' || saon",
    apsaonpaon = "replace(apsaon || ' ' || paon, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ' ' || add2, ' ', '')"
  ),
  by = c(apsaonpaon = "addressfinal")
)

###############matching rule 238 saon1paon62street=ADD12##################
drop_matched("s4_tran", "taba183")

# only for OX4 3PP
fix("s4_epc", "OX4 3PP", "add1", "BROADFIELD HOUSE", "")
rule("taba184",
  tran_where = "contains(saon, 'APARTMENT ') AND propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    paon62 = "char2end(paon, ',')",
    saon1paon62 = "saon1 || ', ' || paon62",
    saon1paon62street = "replace(trim_ws(saon1paon62 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add1 || ',' || add2, ' ', '')"
  ),
  by = c(saon1paon62street = "addressfinal")
)

###############matching rule 239  saonstreet=ADDC5##################
drop_matched("s4_tran", "taba184")
rule("taba185",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('\\d'))",
  tran = c(
    saonstreet = "replace(saon || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, './ ', '')"
  ),
  by = c(saonstreet = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 240 apsaonpaon62street1=ADDC8##################
drop_matched("s4_tran", "taba185")
rule("taba186",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d')) AND contains(paon, ',')",
  tran = c(
    apsaon = "'APARTMENT' || ' ' || saon",
    paon62 = "char2end(paon, ',')",
    apsaonpaon62 = "apsaon || ', ' || paon62",
    apsaonpaon62street1 = "replace(apsaonpaon62 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''. ', '')"
  ),
  by = c(apsaonpaon62street1 = "addressfinal")
)

###############matching rule 241 saonpaonstreet2=ADDRE  ##################
drop_matched("s4_tran", "taba186")
fix("s4_epc", "BH2 6LT", "add", "RICHMOND HILL GATE", "RICHMOND GATE")
fix("s4_epc", "BS8 4UJ", "add", "LEADING EDGE", "THE LEADING EDGE")
fix("s4_epc", "CV8 1FD", "add", "OAKLAND COURT,", "OAKLANDS COURT,")
fix("s4_epc", "SW1P 4DA", "add", ",WESTMINSTER GREEN,", "")
fix("s4_epc", "E3 2UT", "add", "PARK EAST BUILDING BOW QUARTER", "PARK EAST BUILDING")
fix("s4_epc", "E3 2US", "add", "PARK EAST BUILDING BOW QUARTER", "PARK EAST BUILDING")
fix("s4_epc", "E3 2UR", "add", "PARK EAST BUILDING BOW QUARTER", "PARK EAST BUILDING")
fix("s4_epc", "LS2 7EW", "add", "SPARROW WHARFE", " ")
fix("s4_epc", "SE13 7FF", "add1", "B", "FLAT ")
fix("s4_epc", "SE13 7FG", "add1", "B", "FLAT ")
fix("s4_tran", "PR9 0AU", "saon", "THE APARTMENTS", "")
# gsub("", "", ...) on tran$paon for B72 1AY leaves every value unchanged, so it is not translated
rule("taba187",
  tran = c(
    saonpaon1 = "trim_ws(saon || ' ' || paon)",
    saonpaonstreet2 = "replace(trim_ws(saonpaon1 || ', ' || street), ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(saonpaonstreet2 = "addressfinal")
)

###############matching rule 242   saon2paonstreet1=ADDC9#########################
drop_matched("s4_tran", "taba187")
fix("s4_epc", "B16 8EQ", "add", "THE PURPLE APARTMENTS BROADWAY PLAZA", "BROADWAY PLAZA")
fix("s4_epc", "B16 8SU", "add", "THE PURPLE APARTMENTS BROADWAY PLAZA", "BROADWAY PLAZA")
fix("s4_epc", "B16 8SU", "add", "THE RED APARTMENTS BROADWAY PLAZA", "BROADWAY PLAZA")
fix("s4_epc", "AL1 1BH", "add", "ALBANY GATE", "ALBENY GATE")
fix("s4_epc", "BN2 9SY", "add", "OLD COLLEGE HOUSE", "")
rule("taba188",
  tran_where = "propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saon2paon = "saon2 || ' ' || paon",
    saon2paonstreet1 = "saon2paon || ', ' || street"
  ),
  epc = c(
    addressfinal = "translate(add, '''./', '')"
  ),
  by = c(saon2paonstreet1 = "addressfinal")
)

###############matching rule 243  apsaonpaonstreet2=ADD1262cc##################
drop_matched("s4_tran", "taba188")
rule("taba189",
  tran_where = "propertytype = 'F' AND regexp_matches(saon, icu('^\\d'))",
  tran = c(
    apsaon = "'APARTMENT ' || saon",
    apsaonpaon = "apsaon || ' ' || paon",
    apsaonpaonstreet2 = "replace(apsaonpaon || ', ' || street, ' ', '')"
  ),
  epc_where = "contains(add2, ',')",
  epc = c(
    add262 = "char2end(add2, ',')",
    addressfinal = "translate(add1 || ', ' || add262, ''' ', '')"
  ),
  by = c(apsaonpaonstreet2 = "addressfinal")
)

###############matching rule 244 psaonpstreet=ADDRE##################
drop_matched("s4_tran", "taba189")
rule("taba190",
  tran_where = "propertytype = 'F' AND contains(paon, ',')",
  tran = c(
    paon61 = "beg2char(paon, ',')",
    paon6164 = "numextract(paon61)",
    # the digits removed, as in Bin Chi's original gsub("\\d+", "", paon61); the stringi version's
    # stri_replace_all_fixed() looked for the literal text \d+ and so removed nothing
    paon6163 = "regexp_replace(paon61, icu('\\d+'), '', 'g')",
    paon62 = "char2end(paon, ',')",
    paon6164saon = "paon6164 || saon",
    psaonp = "paon6164saon || ' ' || paon6163",
    psaonpp = "psaonp || ',' || paon62",
    psaonpstreet = "replace(psaonpp || ', ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "replace(add, ' ', '')"
  ),
  by = c(psaonpstreet = "addressfinal")
)

###############matching rule 245 saonpaon65street1=ADD12C#####################
drop_matched("s4_tran", "taba190")
rule("taba191",
  tran_where = "contains(paon, ',')",
  tran = c(
    paon5 = "last_token(paon)",
    saonpaon5 = "saon || ', ' || paon5",
    saonpaon5street1 = "replace(saonpaon5 || ' ' || street, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaon5street1 = "addressfinal")
)

###############matching rule 246 saon2paonstreetn3=ADDC#####################
drop_matched("s4_tran", "taba191")
rule("taba192",
  tran_where = "propertytype = 'F'",
  tran = c(
    saon2 = "replace(saon, 'APARTMENT ', '')",
    saonn2 = "replace(saon2, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon2paonn1 = "trim_ws(saonn2 || ' ' || paonn)",
    saon2paonstreetn3 = "replace(trim_ws(saon2paonn1 || ' ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon2paonstreetn3 = "addressfinal")
)

###############matching rule 247 saonpaonn=ADD12C######################
drop_matched("s4_tran", "taba192")
rule("taba193",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    saonpaonn = "replace(saonn || ', ' || paonn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add1 || ', ' || add2, '''./ ', '')"
  ),
  by = c(saonpaonn = "addressfinal")
)

###############matching rule 248  saon1paonstreetn1=ADDC###########################
drop_matched("s4_tran", "taba193")
rule("taba194",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ', ' || paonn",
    saon1paonstreetn1 = "replace(trim_ws(saon1paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '''./ ', '')"
  ),
  by = c(saon1paonstreetn1 = "addressfinal")
)

###############matching rule 249  saon4paonstreetn1=ADDC4#################
drop_matched("s4_tran", "taba194")
# epc1's own key: the trimmed address without . - / and spaces, as in Bin Chi's original. The stringi
# version assigned this key to epc$addressfinal instead, so the join used rule 248's epc1$addressfinal
rule("taba195",
  tran_where = "propertytype = 'F' AND NOT regexp_matches(paon, icu('^\\d'))",
  tran = c(
    saon4 = "replace(saon, 'FLAT ', '')",
    saonn4 = "replace(saon4, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon4paonn1 = "saonn4 || ' ' || paonn",
    saon4paonstreetn1 = "replace(trim_ws(saon4paonn1 || ', ' || streetn), ' ', '')"
  ),
  epc_where = "NOT regexp_matches(add, icu('\\d+-\\d+'))",
  epc = c(
    addressfinal = "translate(trim_ws(add), '-./ ', '')"
  ),
  by = c(saon4paonstreetn1 = "addressfinal"),
  where = "e.propertytype IN ('Flat', 'Maisonette')"
)

###############matching rule 250 saon1paonstreetn=ADDC4############################
drop_matched("s4_tran", "taba195")
rule("taba196",
  tran_where = "propertytype = 'F'",
  tran = c(
    saon1 = "replace(saon, 'APARTMENT ', 'FLAT ')",
    saonn1 = "replace(saon1, '/', '')",
    paonn = "translate(paon, '''.', '')",
    streetn = "replace(street, '''', '')",
    saon1paonn = "saonn1 || ' ' || paonn",
    saon1paonstreetn = "replace(trim_ws(saon1paonn || ', ' || streetn), ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(saon1paonstreetn = "addressfinal")
)

###############matching rule 251 saonpaonlon=ADDC4 #########################
drop_matched("s4_tran", "taba196")
rule("taba197",
  tran = c(
    saonn = "replace(saon, '/', '')",
    paonn = "translate(paon, '''.', '')",
    localityn = "translate(locality, '''.', '')",
    saonpaonn1 = "saonn || ' ' || paonn",
    saonpaonlon = "replace(saonpaonn1 || ', ' || localityn, ' ', '')"
  ),
  epc = c(
    addressfinal = "translate(add, '-./ ', '')"
  ),
  by = c(saonpaonlon = "addressfinal")
)
drop_matched("s4_tran", "taba197")

###############combine the result from stage 4 ################

end_stage("4")

# ------------------stage 5------------------------------
###############the unit signature rule, see unit_signature_rule() #############
if (use_unit_signature_rule) {
  start_stage("5")
  unit_signature_rule()
  end_stage("5")
}

# ------------------combine the results from all stages------------------------------

write_casa()

# tranbin3 and tranbin24 only hold the transactions that stages 4 and 3b start from
drop_tables("tranbin3", "tranbin24")

# ------------------data linkage ending----------------------------
# ==================end ==================

message("part 2, link: ", format(round(Sys.time() - t_part, 1)))

dbDisconnect(con, shutdown = TRUE)

# ------------------------------------------------
# Part 3, final PPD
# ------------------------------------------------
# The joins, the choice of one EPC per transaction, the data quality filters and the csv are all done
# inside DuckDB, so only the small tables behind the graphs come back to R. The output is the same as
# the retired 4. Create final PPD.R's; where several EPCs tie on every criterion, both take the lowest
# EPC id (see resolve_ties()).

t_part <- Sys.time()

# Get data; the database is only read
con <- dbConnect(duckdb::duckdb())

run_sql(sprintf("ATTACH '%s' AS db (READ_ONLY)", db_path))
run_sql("USE db")
# DuckDB would otherwise use up to 80% of memory; with 8GB this part peaks at about 9GB and is no
# slower (beyond the limit DuckDB uses temporary files)
run_sql("SET memory_limit = '8GB'")

# Join data and pre-compute absolute differences, in days as difftime(units = "days"); n_epc is the
# number of EPCs matched to the transaction
run_sql("
  CREATE TEMP TABLE tran_casa_epc AS
  SELECT
    c.transactionid, t.price, CAST(t.dateoftransfer AS DATE) AS dateoftransfer, t.postcode,
    t.county, t.district, t.propertytype, t.categorytype,
    e.id, e.tfarea, e.numberrooms, e.inspectiondate, e.lodgementdate,
    abs(e.inspectiondate - CAST(t.dateoftransfer AS DATE)) AS abs_inspection_diff,
    abs(e.lodgementdate - CAST(t.dateoftransfer AS DATE)) AS abs_lodgement_diff,
    count(*) OVER (PARTITION BY c.transactionid) AS n_epc
  FROM casa c
  JOIN transactions t USING (transactionid)
  JOIN epc e USING (id)
")

# inspectiondate before dateoftransfer, for transactions with multiple matches
run_sql(paste(
  "CREATE TEMP TABLE ties_before AS",
  resolve_ties(
    "SELECT * FROM tran_casa_epc
     WHERE n_epc > 1 AND tfarea IS NOT NULL AND tfarea > 0 AND inspectiondate <= dateoftransfer"
  )
))

# inspectiondate after dateoftransfer
run_sql(paste(
  "CREATE TEMP TABLE ties_after AS",
  resolve_ties(
    "SELECT * FROM tran_casa_epc
     WHERE n_epc > 1 AND tfarea IS NOT NULL AND tfarea > 0 AND inspectiondate > dateoftransfer
       AND transactionid NOT IN (SELECT transactionid FROM ties_before)"
  )
))

# Unique matches plus the two sets above; branch keeps the retired 4. Create final PPD.R's row order
run_sql("
  CREATE TEMP VIEW ppd AS
  SELECT *, 1 AS branch FROM tran_casa_epc WHERE n_epc = 1
  UNION ALL SELECT *, 2 FROM ties_before
  UNION ALL SELECT *, 3 FROM ties_after
")

# Match % by year and property type
tran_counts <- dbGetQuery(
  con,
  "SELECT CAST(year(CAST(dateoftransfer AS DATE)) AS INTEGER) AS year, propertytype,
          CAST(count(*) AS INTEGER) AS total
   FROM transactions GROUP BY ALL"
)

ppd_counts <- dbGetQuery(
  con,
  "SELECT CAST(year(dateoftransfer) AS INTEGER) AS year, propertytype,
          CAST(count(*) AS INTEGER) AS matched
   FROM ppd GROUP BY ALL"
)

match_pct <- tran_counts |>
  left_join(ppd_counts, by = c("year", "propertytype")) |>
  mutate(matched = replace_na(matched, 0L), pct = matched / total * 100) |>
  arrange(year, propertytype)

# Add overall match %; propertytype stays a factor, with Overall as its last level
overall <- match_pct |>
  summarise(matched = sum(matched), total = sum(total), .by = year) |>
  mutate(propertytype = factor("Overall"), pct = matched / total * 100)

match_pct <- bind_rows(match_pct, overall)

# Saved to Graphs/Main analysis at 2400 x 1736 px, as every figure in
# Main analysis.R (fig:EPC_match_percentage)
match_pct_plot <- match_pct |>
  ggplot(aes(x = year, y = pct, colour = propertytype,
             linewidth = propertytype == "Overall")) +
  geom_line() +
  geom_point(size = 1.5) +
  scale_linewidth_manual(values = c("TRUE" = 1.4, "FALSE" = 0.6), guide = "none") +
  scale_x_continuous(breaks = seq(2000, 2025, 5)) +
  scale_y_continuous(limits = c(50, 100)) +
  labs(x = NULL, y = "Match (%)", colour = "Property Type") +
  theme_minimal() +
  # Legend entries at the 11 pt of the legend title, as in Main analysis.R
  theme(legend.position = "bottom", legend.text = element_text(size = rel(1)))

print(match_pct_plot)
ggsave(
  graph_path,
  match_pct_plot,
  width = 2400 / 192,
  height = 1736 / 192,
  dpi = 192,
  bg = "white"
)

# Create additional fields; as in R, x / 0 is Inf and 0 / 0 is NaN
run_sql("
  CREATE TEMP VIEW ppd_fields AS
  SELECT *, price / tfarea AS priceper, tfarea / numberrooms AS floorper FROM ppd
")

# Pair correlations
dbGetQuery(
  con,
  "SELECT numberrooms, floorper, priceper, tfarea FROM ppd_fields USING SAMPLE 100000 ROWS"
) |>
  ggpairs()

# Data quality filters, tidy names, select only the required fields and write csv. known() is R's
# !is.na(), which is FALSE for NaN; DuckDB would otherwise count NaN as larger than any number
run_sql("CREATE TEMP MACRO known(x) AS (x IS NOT NULL AND NOT isnan(x))")

run_sql(sprintf(
  "
  COPY (
    SELECT
      dateoftransfer AS \"Date of Transfer\",
      postcode AS Postcode,
      county AS County,
      district AS District,
      price AS Price,
      tfarea,
      priceper,
      propertytype AS \"Property Type\",
      categorytype AS \"PPD Category Type\"
    FROM ppd_fields
    WHERE NOT (
      (known(numberrooms) AND numberrooms > 20) OR
      (known(tfarea) AND tfarea < 10) OR
      (known(tfarea) AND tfarea > 1000) OR
      (known(priceper) AND priceper > 60000) OR
      (known(floorper) AND floorper < 5) OR
      (known(floorper) AND floorper > 250)
    )
    ORDER BY Postcode, branch, transactionid
  ) TO '%s' (HEADER, NULLSTR 'NA')
  ",
  psqm_path
))

message("part 3, final PPD: ", format(round(Sys.time() - t_part, 1)))

dbDisconnect(con, shutdown = TRUE)

message("total: ", format(round(Sys.time() - t_start, 1)))
