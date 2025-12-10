# Land-Registry-PPD-EPC-price-per-square-metre
This is a refactor of [Bin Chi's excellent work](https://github.com/Bin-Chi/Link-LR-PPD-and-Domestic-EPCs) linking [England and Wales Land Registry Price Paid Data](https://www.gov.uk/guidance/about-the-price-paid-data) to [EPC data](https://epc.opendatacommunities.org/) to obtain a price per square metre dataset.

Overall execution time for the equivalent of scripts 3 and 4 has improved from over 18 hours to just under 65 minutes, or 18x, on an M1 MacBook Pro with 32GB of memory. I hope by speeding up this code more people can and will use Bin's approach; the rules have an excellent match rate - see the Matches by year csv file.

An overview of the scripts:

1. Combine EPC certificates. This script selects only the required fields from the individual EPC files before combining into a single file. This doesn't have an equivalent in Bin's work.

2. Add PPD and EPC to database. This script adds the PPD and EPC datasets to the duckDB database, only taking records with postcodes in common to both. This is equivalent to the Data_cleaning.sql and Read_LR_PPD.sql scripts in Bin's work but adds a few more lines to tidy up the data upfront. Note that I have kept the additional price paid category of transactions.

3. PPD_EPC_linkage fast. This is the bulk of the refactoring work.

		i) DuckDB rather than PostGIS

		ii) anti_join rather than bespoke functions

		iii) stringi library rather than base R; this library - not released until 2022 -
		is responsible for the majority of the speed gains (10x, all other changes 1.8x)

		Gagolewski M., stringi: Fast and portable character string processing in R,
		Journal of Statistical Software 103(2), 2022, 1–59, doi:10.18637/jss.v103.i02.

		iv) Major consolidation of the transaction and epc row wise operations; in some 
  		cases, stri_replace_all_regex() may be slower but easier to follow than many
		stri_replace_all_fixed() lines

		v) inner_join now includes the postcode plus the transaction and epc variables and
		subsequently selects the id variables for each rule, thereby eliminating row wise
		operations and saving memory

		vi) Tidied up the variable assignment at the end of each stage, removing duplicates
		to save memory

		vii) Filtered the epc dataframe for just the required postcodes for each stage;
		filtering after each anti_join added no benefit for stage 1 so wasn't tested for
		other stages

		viii) Removed the code to determine which EPC to use if there is a match to more
		than one (see below)

		Fixed some minor typo bugs in rules 54 and 187. Despite extensive regression
		testing, some new ones may have been introduced.

		Considered but not implemented:

		i) A common inner_join function; a missed opportunity, this would save repetition
		but it's now a lot of work for no speed benefit

		ii) Use of data.table, this was actually 7% slower for the first 3 rules (which
		return the majority of matches) and the syntax isn't as intuitive

		iii) Use of Tidyverse for the rules with execution using duckDB via collect();
		it's better to view the row wise operations for troubleshooting

		iv) Tidying up of the variable names; keeping the existing variable names allows
		for troubleshooting with comparison to the original code

4. Create final PPD. This script: uses data.table indexing to split the matches into 1:1 or 1:N transaction to EPC groups, has a new function to determine which of the N to choose, then recombines and filters for data quality. Alternate rules and filters can easily be substituted. This is equivalent to the Data_cleaning.R script in Bin's work.

There is no equivalent to Bin's Evaluation.R and Read_NSPL.sql scripts but these are trivial to replicate for those that require it.

air.toml The code has been [formatted with air](https://www.tidyverse.org/blog/2025/02/air/) with a custom line width of 105 characters to ensure the row wise operations fit on one line for ease of scanning by eye.

All Data should be put in the Data folder along with Bin's rulechi.csv which is used to correct addresses as necessary. I have not tried to add to these rules, although it may be possible to do so.
