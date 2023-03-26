'''
run this file to create and/or update the stock data file system,
using as many processes as there are API keys available
(since the API limits us to 2 requests per second per API key).
'''

import time
from td_price_history import PROCESSES_TO_USE, TdPriceHistory as Td

def time_it(func, args):
    '''
    times the execution of a given function.
    '''
    start = time.time()
    func(*args)
    return time.time() - start

if __name__ == "__main__":
    print(f'>>>STARTING UPDATING FILES IN PARALLEL USING {PROCESSES_TO_USE} PROCESSES\n')

    util = Td()
    tickerz = util.get_tickers_set()
    
    min_time = time_it(util.run_parallel_routine, (tickerz, 'day', 10, 'minute', 1))
    day_time = time_it(util.run_parallel_routine, (tickerz, 'year', 20, 'daily', 1))

    print(f'\n\n>>>DAILY ROUTINE FINISHED RUNNING IN {day_time} seconds')
    print(f'>>>MINUTE ROUTINE FINISHED RUNNING IN {min_time} seconds')
