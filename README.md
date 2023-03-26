# Financial Data Aggregation Tool
Python routine to aggregate by-the-minute open, close, high, and low data from the TD Ameritrade API into structured JSON files.

## Table of Contents
* [Purpose](https://github.com/mglush/data-aggregation/edit/main/README.md#purpose)
* [Files](https://github.com/mglush/data-aggregation/edit/main/README.md#files)
* [Use In Your Project](https://github.com/mglush/data-aggregation/edit/main/README.md#use-in-your-project)
* [Technologies](https://github.com/mglush/data-aggregation/edit/main/README.mdtechnologies)
* [Sources](https://github.com/mglush/data-aggregation/edit/main/README.md#sources)

## Purpose
In order to bypass the idea of paying for financial data, I wrote this routine to scrape the public TD Ameritrade API so that I can have by-the-minute data for use in my data science projects. I plan to rewrite the routine in MicroPython so that I can put it on my Raspberry Pi Pico.

## Files
While the files contain self-explanatory code, this section briefly summarizes the purpose of each file.
1. [data_mining/td_price_history.py](https://github.com/data-aggregation/blob/main/td_price_history.py)
* This class was written to interract with the TD Ameritrade API. It contains methods that could be helpful in pulling the price history of a single stock, a group of stocks given a list, or to pull the price history of all the stocks available on there (excluding small OTC symbols). This file should be used as an import.
2. [data_mining/parallel_gen_data.py](https://github.com/data-aggregation/blob/main/parallel_gen_data.py)
* I use this file to run the TdPriceHistory class, get the latest 10 days of data I need, and append it to the end of each tickers JSON file.

## Use In Your Project
You can import the TdPriceHistory class from the td_price_history.py file for use in your projects with the usual
```
from td_price_history import TdPriceHistory
```
command. Do insert your own API keys so that it works properly.

## Technologies
* TD Ameritrade API.
* Yahoo Finance API.
* Python (libraries include *requests*, *json*, *ijson*, *datetime*, *pandas*, *umap*, *matplotlib*, *multiprocessing*).

## Sources
* [Umap How To](https://umap-learn.readthedocs.io/en/latest/index.html)
* [Yahoo Finance How To](https://levelup.gitconnected.com/how-to-get-all-stock-symbols-a73925c16a1b)
