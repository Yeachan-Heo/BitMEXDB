import pandas as pd
import datetime
import sqlite3
import tqdm
import wget
import re
import os

from bs4 import BeautifulSoup
from selenium import webdriver

def create_tables(cursor, ticker, timeframes):
    cursor.execute(f"""CREATE TABLE {ticker}_TICK 
                        (timestamp text, symbol text, side text, size float, price float, tickDirection text, trdMatchID text, grossValue float, homeNotional float, foreignNotional float)""")

    for timeframe in timeframes:
        cursor.execute(f"""CREATE TABLE {ticker}_{timeframe}
                            (timestamp text, open float, high float, low float, close float, volume float, lowFirst float)""")


def low_first(x):
    if x.empty:
        return 1

    x_list = list(x)

    mx = max(x)
    mn = min(x)

    if x_list.index(mn) < x_list.index(mx):
        return 1

    return 0


def resample_tick(tick_df, timeframe):
    ohlc = tick_df["price"].resample(timeframe).ohlc().ffill()

    lowFirst = tick_df["price"].resample(timeframe).apply(low_first)
    lowFirst.name = "lowFirst"

    volume = tick_df["size"].resample(timeframe).sum().ffill()
    volume.name = "volume"

    timestamp = pd.Series([s.strftime('%Y-%m-%d %H:%M:%S') for s in ohlc.index], index=ohlc.index)

    df = pd.concat([timestamp, ohlc, volume, lowFirst], axis=1)

    df.columns = ["timestamp", "open", "high", "low", "close", "volume", "lowFirst"]

    return df


def write_to_db(cursor, table_name, df):
    for value in df.values:
        sql = f"INSERT INTO {table_name} VALUES {tuple(value)}"
        cursor.execute(sql)


def wget_retry(*args, **kwargs):
    try:
        return wget.download(*args, **kwargs)
    except:
        return wget_retry(*args, **kwargs)


def crawl(db_path, start_date="auto", timeframes=["1T", "5T", "15T", "30T", "1H", "1D"], reset_db=False, chromedriver_loc="/usr/bin/chromedriver"):
    if reset_db:
        os.system(f"rm -rf {db_path}")

    if os.path.exists(db_path):
        db = sqlite3.connect(db_path)
        cursor = db.cursor()
        tickers = [x[0] for x in cursor.execute("SELECT * FROM TICKERS").fetchall()]

        if start_date == "auto":
            last_date = load_bitmex_data(db_path, "1H", "XBTUSD")["timestamp"].iloc[-1]
            last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(1)
            start_date = last_date.strftime("%Y%m%d")
            print("autodetected start date:", start_date)
    else:
        db = sqlite3.connect(db_path)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE TICKERS(ticker text)")
        tickers = []

        if start_date == "auto":
            start_date = "20141122"
    
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(chromedriver_loc, options=chrome_options)

    if not os.path.exists("/tmp/bitmex"):
        os.makedirs("/tmp/bitmex")

    driver.get(f"https://public.bitmex.com/?prefix=data/trade/")

    while True:
        if input("type Y if webpage is open") == "Y":
            break

    y, m, d = int(start_date[:4]), int(start_date[4:6]), int(start_date[6:])
    start_date = datetime.date(y, m, d)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    for link in tqdm.tqdm(soup.findAll('a', attrs={'href': re.compile("^https://")})):
        # get link
        link = (link.get('href'))

        # check link date
        date_str = link.split("/")[-1][:8]

        if len(date_str) < 8:
            continue

        y, m, d = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])

        date = datetime.date(y, m, d)

        if not date >= start_date:
            continue

        # download

        os.system("rm -f /tmp/bitmex/raw.csv.gz")
        os.system("rm -f /tmp/bitmex/raw.csv")

        wget_retry(link, out="/tmp/bitmex/raw.csv.gz")
        os.system("gzip -d /tmp/bitmex/raw.csv.gz")

        # read
        df = pd.read_csv("/tmp/bitmex/raw.csv")

        # process
        df_grouped = dict(list(df.groupby("symbol")))
        tickers_curr_df = list(df_grouped.keys())

        # write to db
        for ticker in tickers_curr_df:
            if not ticker in tickers:
                create_tables(cursor, ticker, timeframes)
                tickers.append(ticker)
                cursor.execute(f"INSERT INTO TICKERS VALUES ('{ticker}')")

            tick_df = df_grouped[ticker]
            tick_df.index = pd.to_datetime(tick_df["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f")

            tick_df = tick_df.fillna(-1)

            write_to_db(cursor, f"{ticker}_TICK", tick_df)

            for timeframe in timeframes:
                resampled = resample_tick(tick_df, timeframe)
                write_to_db(cursor, f"{ticker}_{timeframe}", resampled)

        db.commit()

    db.close()

def load_bitmex_data(db_path, timeframe, symbol):
    db = sqlite3.connect(db_path)

    if timeframe == "TICK":
        columns = ["timestamp", "symbol", "side", "size", "price", "tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional"]
    else:
        columns = ["timestamp", "open", "high", "low", "close", "volume", "lowFirst"]
        
    df = pd.DataFrame(db.execute(f"SELECT * FROM {symbol}_{timeframe}"), columns=columns)
    df.index = pd.to_datetime(df["timestamp"])

    return df


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()

    parser.add_argument("--timeframes", type=list, default=["1T", "5T", "15T", "30T", "1H", "1D"])
    parser.add_argument("--db_path", help="path to sqlite3 database", type=str, default="/home/ych/Storage/bitmex/bitmex.db")
    parser.add_argument("--start_date", type=str, default="auto")
    parser.add_argument("--reset_db", type=bool, default=False)
    parser.add_argument("--chromedriver_loc", help="path to selenium chrome driver", default="/usr/bin/chromedriver")

    args = parser.parse_args()

    crawl(args.db_path, args.start_date, args.timeframes, args.reset_db, args.chromedriver_loc)

