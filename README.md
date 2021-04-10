# BitMEXDB

## Bitmex Bulk Tick Data Crawler
This program downloads tick data from public.bitmex.com

## options
#### timeframes
desired tick-resampling timeframes. use pandas's representation such as "1D", "1H", "1T"  
#### db_path
desired location for the db  
#### start_date
db crawling start date. if the db exists, it automatically detects it. if not, defaults to 20141122; 
not recommended to change  
#### reset_db
set True if you want to reset(erase) db and make a new one
#### chromedriver_loc
your selenium chrome driver location

## usage
python main.py [options]  
-> the public.bitmex.com shows up.  
-> if you see the list of csv.gz files on the webpage, click "yes"  
-> the download starts  

## query
1. the table names are formatted as: SYMBOL_TIMEFRAME
2. Available TIMEFRAME are TICK, and what you have specified in option --timeframes
3. Available SYMBOL are in table TICKERS

## tips
1. when initializing the DB, it can take long (like 10~15 hours)    
2. due to crawling restrictions, the download gets slower and slower. it's normal, but it can be faster if you just pause (shut down) and resume (rerun) the script within 30min~1 hrs.  
3. using the function load_bitmex_data, you can easliy query the desired dataset.
4. please enjoy
