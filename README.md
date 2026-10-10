# Land-Registry-PPD-EPC-price-per-square-metre
This is a refactor of [Bin Chi's excellent work](https://github.com/Bin-Chi/Link-LR-PPD-and-Domestic-EPCs) linking [England and Wales Land Registry Price Paid Data](https://www.gov.uk/guidance/about-the-price-paid-data) to [EPC data](https://get-energy-performance-data.communities.gov.uk/) to obtain a price per square metre dataset. I hope by speeding up this code more people can and will use Bin's approach; the rules have an excellent match rate - see my [working paper](https://github.com/chris-beech-housing/Travelling-Wave-Dynamics-in-Housing-Markets/blob/main/Travelling%20Wave%20Dynamics%20in%20Housing%20Markets.pdf).

The work in the 'Earlier code' folder was a summer 2005 manual refactoring with many helper scripts; at the time LLMs were not yet mature enough to do the translation.

'PPD EPC price per sqm' is an October 2026 refactor by Claude Code under my direction. It uses duckDB to take advantage of the database engine and its multi-core support. It is incredibly fast, completing in under 5 minutes on an M4 MacBook Air with 24GB of memory; it will take you longer to download the input files.

In addition, I challenged Claude Code to come up with an additional rule which found over 66k new matches. Run as the first rule it finds 99.95% of all matches that the 251 rules do.

All Data should be put in the Data folder along with Bin's rulechi.csv which is used to correct addresses as necessary. I have not tried to add to these rules, although it may be possible to do so.
