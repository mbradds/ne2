# Net Energy Exchange
Automated ETL for the net energy daily settlement price API, and web scraping with selenium for daily trade volumes from the net energy platform.

## Background
Net Energy is an exchange platform based in western Canada providing trading services for various crude oil grades and contracts for primarily Canadian
energy products such as Western Canadian Select Crude.

The price and volume data offered on this platform is valuable for market monitoring, and evaluating the real dollar impact of the differential between
western Canadian crude prices, and US crude prices.

Net Energy offers two key datasets to subscribers:

*Daily settlement prices: For each contract that trades on the exchange, a settlement price is generated, which reflects the average price that the contract
 traded on that day.

*Trade volumes: Each transaction through the exchange, both physical and finacial, has a volume associated with it. This could be in the form of a standard
 "contract" (1 contract = 1000 barrels) or a custom arrangement. This is important to gauge the liquidity of contracts. The next month contract typically has 
 the most volume, with contracts for delivery in later months having less barrels traded.


These two datasets can be combined to answer the question; How many barrels of western Canadian crude are exposed to steep discounts?


## Code files

* ne2.py
Connects to the net energy settlements api and adds any new data to a database. Can be scheduled each day, or any time frame, to gather all new price data. The
program automatically recognizes which days are missing, and gathers the data.

* ne2_volumes.py
Uses the selenium webdriver for chrome to automatically login to the exchange, press the navigation buttons when they appear, exit popups, and click on the 
download link. There is no api to access machine readable volumes data, so this approach is used. Can be set to run everyday to add all new trades to a database.


## Analysis

In progress. Currently figuring out how many barrels of Canadian crude produced each month is exposed to large differentials seen in the next month(s) contract
prices, vs how many barrels are trading over the counter, such as directly between a producer and refiner, without any price reporting through an exchange.













