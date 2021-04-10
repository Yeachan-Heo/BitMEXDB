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

## usage
python main.py [options]
