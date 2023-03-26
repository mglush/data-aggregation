'''
td_price_history.py
Michael Glushchenko
'''

import os
import time
import multiprocessing as mp
from datetime import datetime
from tqdm import tqdm
import ujson as json
from yahoo_fin import stock_info as si
import requests
import pandas as pd

CONSUMER_KEYS = ['UCJA7GWHIKKXO2G69GXDMEFUX24QZ0PD',
                 'NJVFPDOUVIVOCGLTOETENI5WOLKWBJ3B',
                 'I346SQV6CXH5FEQ2I8HCZNFH2Z556AH4',
                 'GQ4MAKWOMBSGGRJ91RNZEE6THVGRJAJD',
                 'HWO3NCQY5YB9TESIGPKPIICIKV0CKSYF',
                 'XWNWIEGUKAGUYBJ1AILZZTGGRIOS7RRV',
                 'WQKJUI07MWEVTAII5FDXUBLVNFPKMOAG',
                 'S2D28UJPGEOFKEN5FA9PKXLTPAQG0VEF',
                 'LMRHF8LMP3EA5N64WTOL0OFPOGDPKNVF',
                 'U3HWMIJOUAROHJFAIG4WTZUDS833TVUG',
                 'IUNOMMPZT5ESPREYGDIPUJ6TYYFGXNFZ']
KEY = CONSUMER_KEYS[0]
PROCESSES_TO_USE = len(CONSUMER_KEYS)
PROGRESS_BAR = tqdm(range(int(11000 / PROCESSES_TO_USE)))

class TdPriceHistory():
    '''
    Utility class that can be used to interract with
    the TD Ameritrade API endpoints and to pool historical price
    data for every ticker in the s&p 500, nasdaq, and dow jones, and more.
    '''
    endpoint = 'https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory'
    apikey = ''

    def __init__(self, apikey=KEY):
        '''
        Initializes the endpoint and the api key that will be used (if user wants to specify one).
        '''
        self.apikey = apikey

    def get_tickers_set(self):
        '''
        Gets all stock tickers in sp500, nasdaq, and dow indices.
        Also get all stocks in the yahoo 'other' category.

        Input: none.
        Output: a set containing ticker symbols that exist on yahoo finance.
        '''
        df1 = pd.DataFrame(si.tickers_sp500())
        df2 = pd.DataFrame(si.tickers_nasdaq())
        df3 = pd.DataFrame(si.tickers_dow())
        df4 = pd.DataFrame(si.tickers_other())

        sym1 = set( symbol for symbol in df1[0].values.tolist() )
        sym2 = set( symbol for symbol in df2[0].values.tolist() )
        sym3 = set( symbol for symbol in df3[0].values.tolist() )
        sym4 = set( symbol for symbol in df4[0].values.tolist() )

        symbols = set.union( sym1, sym2, sym3, sym4 )
        avoid_endings = ['W', 'R', 'P', 'Q']
        save = set(symbol for symbol in symbols if not (len(symbol) > 4 and symbol[-1] in avoid_endings) and symbol != '' and '$' not in symbol)

        return save

    def get_endpoint_data(self, endpoint, params):
        '''
        Performs get request on the endpoint with given parameters.

        Input: endpoint URL, dictionary of search parameters.
        Output: returns a dictionary of returned values if request successful.
                the dictionary contains data for the
                high, low, open, close, volume, and datetime columns
                returns none otherwise.
        '''
        try:
            page = requests.get(url=endpoint, params=params)
        except requests.RequestException as request_exception:
            print(request_exception)

        if page.status_code == 200:
            return json.loads(page.content)

        if page.status_code == 429:
            time.sleep(1)
            return self.get_endpoint_data(endpoint, params)

        print(f'>>>BAD REQUEST ERROR FROM {str(endpoint)}.\nPAGE STATUS = {str(page.status_code)}')
        return None

    def reformat_dates(self, content):
        '''
        Changes unix timestamp format to readable date format.

        Input: dictionary containing a 'datetime' column of unix timestamp values.
        Output: returns a modified dictionary.
        '''
        for item in content['candles']:
            item['datetime'] = datetime.utcfromtimestamp(item['datetime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

    def get_ticker(self, ticker, period_type, period, frequency_type, frequency, key=KEY):
        '''
        Uses get_endpoint_data() on a single ticker symbol.

        Input: string containing the symbol of a ticker, as well as the API
                parameters that will be used.
                period_type values: day, month, year, ytd.
                period values (by petiod type): day: 1, 2, 3, 4, 5, 10
                                                month: 1, 2, 3, 6
                                                year: 1, 2, 3, 5, 10, 15, 20
                                                ytd: 1
                frequency type values (by period type): day: minute*
                                                        month: daily, weekly
                                                        year: daily, weekly, monthly
                                                        ytd: daily, weekly
                frequency values (by frequency type): minute: 1, 5, 10, 15, 30
                                                      daily: 1
                                                      weekly: 1
                                                      monthly: 1
        Output: if request successful: returns a single dictionary containing
                the high, low, open, close, volume, and datetime columns
                for a given ticker in set returned by get_tickers_set().
                returns none otherwise.
        '''
        params = {'apikey' : key,
                        'periodType' : period_type,
                        'period' : period,
                        'frequencyType' : frequency_type,
                        'frequency' : frequency}

        result = self.get_endpoint_data(self.endpoint.format(ticker=ticker.upper()), params)

        if result is None:
            print(f'... ENCOUNTERED ERROR TRYING TO GET {ticker} DATA.')
            return None
        elif not result['empty']:
            self.reformat_dates(result)
            return result
        else:
            # print(f'... NO DATA FOR {ticker}, CHECK PARAMETERS.')
            return None

    def get_tickers(self, period_type, period, frequency_type, frequency):
        '''
        Uses get_ticker() on every ticker symbol in set returned by get_tickers_set().

        Input: api parameters for the given search.
                period_type values: day, month, year, ytd.
                period values (by petiod type): day: 1, 2, 3, 4, 5, 10
                                                month: 1, 2, 3, 6
                                                year: 1, 2, 3, 5, 10, 15, 20
                                                ytd: 1
                frequency type values (by period type): day: minute*
                                                        month: daily, weekly
                                                        year: daily, weekly, monthly
                                                        ytd: daily, weekly
                frequency values (by frequency type): minute: 1, 5, 10, 15, 30
                                                      daily: 1
                                                      weekly: 1
                                                      monthly: 1
        Output: returns a list of dictionaries, each dictionary containing
                the high, low, open, close, volume, and datetime columns
                for each given ticker in the set returned by get_tickers_set().
        '''
        results = []
        for item in self.get_tickers_set():
            result = self.get_ticker(item, period_type, period, frequency_type, frequency)
            if result is not None:
                results.append(result)

        return results

    def change_file_format(self, file, timeframe):
        '''
        Temporary use function to change json files from storing
        candle data, ticker name, and empty boolean parameter
        to simply storing candle data.
        '''
        filename = './data/' + timeframe + '/' + str(file)
        try:
            file = open(filename, encoding='utf-8', mode='r')
            data = json.load(file)
            file.close()

            out_file = open(filename, encoding='utf-8', mode='w')
            json.dump(data['candles'], out_file, indent=4)
            out_file.close()
        except OSError as file_error:
            print('oops: '+ str(file_error))

    def change_files_format(self, timeframe):
        '''
        Calls the temporary-use function change_file_format()
        on every ticker in the given timeframe folder.
        '''
        for path in os.listdir('./data/minute'):
            if os.path.isfile(os.path.join('./data/minute', path)):
                self.change_file_format(path, timeframe)
        print('>>>SUCCESSFULLY CHANGED FORMAT OF FILES.')

    def update_file(self, ticker, period_type, period, frequency_type, frequency, key=KEY):
        '''
        If file exists, updates data of existing file by appending everything
        that is not already in the file but is in the data parameter.
        Otherwise, creates the file first, then writes data to it.
        '''
        filename = './data/' + str(frequency_type) + '/' + str(ticker).lower() + '.json'

        try:
            file = open(filename, encoding='utf-8', mode='r')
            data = json.load(file)
            file.close()
            try:
                old_dates = set(item['datetime'] for item in data)
            except TypeError:
                self.change_file_format(str(ticker).lower() + '.json', str(frequency_type))
                file = open(filename, encoding='utf-8', mode='r')
                data = json.load(file)
                file.close()
                old_dates = set(item['datetime'] for item in data) if data is not None else set()
        except OSError:
            print(f'\n>>>FILE {ticker}.json DID NOT PREVIOUSLY EXIST... CREATING NEW ONE.')
            data = []
            old_dates = set()

        testing = self.get_ticker(ticker, period_type, period, frequency_type, frequency, key)

        if testing is not None:
            testing = testing['candles']
            for item in testing:
                if item is not None and item['datetime'] not in old_dates:
                    data.append(item)

        try:
            file = open(filename, encoding='utf-8', mode='w')
            data = json.dump(data, file, indent=4)
            file.close()
        except OSError:
            print('\n>>>FAILED TO WRITE TO FILE.')

        PROGRESS_BAR.update()

    def update_files(self, period_type, period, frequency_type, frequency):
        '''
        Creates/updates all files in the list of tickers returned
        by the get_tickers_set method.
        '''
        tickers = self.get_tickers_set()
        for ticker in tickers:
            self.update_file(ticker.lower(), period_type, period, frequency_type, frequency)
        print('>>>ALL FILES SUCCESSFULLY UPDATED')

    def update_files_parallel(self, tickers, period_type, period, frequency_type, frequency, key=KEY):
        '''
        Creates/updates all files in the list of tickers returned
        by the get_tickers_set method.
        '''
        while True:
            temp = tickers.get()
            if temp == 'PROC_FINISHED':
                print('>>>PROCESS FINISHED ITS WORK!!!')
                break
            self.update_file(temp, period_type, period, frequency_type, frequency, key)

    def run_parallel_routine(self, tickers, period_type, period, frequency_type, frequency):
        '''
        Creates/updates all files in the list of tickers returned
        by the get_tickers_set method using however many API keys
        are available to split work amongst the cores the user has.
        '''
        queue = mp.Queue()
        for ticker in tickers:
            queue.put(ticker)
        for i in range(PROCESSES_TO_USE):
            queue.put('PROC_FINISHED')

        processes = []
        for i in range(PROCESSES_TO_USE):
            proc = mp.Process(target=self.update_files_parallel, args=(queue, period_type, period, frequency_type, frequency, CONSUMER_KEYS[i], ))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()
        for proc in processes:
            proc.kill()

        print('>>>FINISHED RUNNING ON ALL PROCESSES!!!')
