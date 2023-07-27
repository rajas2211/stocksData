# stocksData
This program uses Alpha Vantage API to scrape stock time series data and process it as daily, weekly, monthly, quarterly, half-yearly and annual data.
Currently configured for BSE Stock data.
Run python stocksData.py [input_file.csv] [output_file.csv]
Default input_file -> symbolsT.csv => This file should contain stock symbol on each line
Default output_file -> masterData-{date}.csv => This file should contain stock symbol on each line
This program also requires Alpha Vantage's API key stored in 'apiKey.txt'
