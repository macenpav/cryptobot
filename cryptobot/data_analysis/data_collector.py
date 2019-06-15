import sqlite3
from dateutil.parser import parse
from datetime import timedelta
import threading
from time import sleep
from cryptobot.hitbtc.hitbtc_client import NoDataException


class SyncThread(threading.Thread):
    def __init__(self, data_collector):
        threading.Thread.__init__(self)
        self.data_collector = data_collector
        data_collector.initialize_candles()

    def run(self):
        while True:
            self.data_collector.sync_candles()
            sleep(1)


class DataCollector(object):
    SELECT_QUERY = "SELECT 1 FROM MinMax_{1} WHERE date='{0}' LIMIT 1"

    REPLACE_QUERY = "REPLACE INTO MinMax_{3} (date, min, max) VALUES ('{0}', {1}, {2})"

    UPDATE_QUERY = "UPDATE MinMax_{3} SET min=(CASE WHEN min>{1} THEN {1} ELSE min END), " \
                   "max=(CASE WHEN max>{2} THEN max ELSE {2} END) WHERE date='{0}'"

    UPDATE_CLOSING_PRICE = "UPDATE MinMax_{2} SET closing_price={1} WHERE date='{0}'"

    CREATE_QUERY = "CREATE TABLE IF NOT EXISTS MinMax_{0} " \
                   "(date DATETIME PRIMARY KEY, min FLOAT, max FLOAT, closing_price FLOAT DEFAULT NULL, " \
                   "UNIQUE(date, min, max, closing_price) ON CONFLICT REPLACE)"

    SELECT_MINMAX_QUERY = "SELECT date, min, max FROM MinMax_{0} " \
        "WHERE datetime(date) >= datetime('now', '-{1} minute') " \
        "ORDER BY date ASC"

    SELECT_CLOSING_PRICE_QUERY = "SELECT date, closing_price FROM MinMax_{0} " \
        "WHERE datetime(date) >= datetime('now', '-{1} minute') " \
        "ORDER BY date ASC LIMIT {2}"

    DB_NAME = 'my_db.db'

    HOURS_TO_MINUTES = 60

    def __init__(self, client, currency='BTCUSD'):
        self.__currency = currency
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        cursor.execute(self.CREATE_QUERY.format(currency))
        conn.commit()
        conn.close()
        self.__client = client

        self.__t_sync = None

    def __del__(self):
        if self.__t_sync is not None:
            self.__t_sync.join()

    def __initialize_candles(self, cursor, period):
        candles = self.__client.get_candles(self.__currency, limit=1000, period=period)
        for c in candles:
            date = parse(c['timestamp'])
            cursor.execute(self.REPLACE_QUERY.format(date, c['min'], c['max'], self.__currency))
        for c in candles:
            prev_date = parse(c['timestamp']) - timedelta(minutes=1)
            cursor.execute(self.UPDATE_CLOSING_PRICE.format(prev_date, c['close'], self.__currency))

    def initialize_candles(self):
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        try:
            for p in ['M1']:
                self.__initialize_candles(cursor, p)
            conn.commit()
        except NoDataException:
            pass
        conn.close()

    def start_sync(self):
        self.__t_sync = SyncThread(self)
        self.__t_sync.run()

    def sync_candles(self):
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        try:
            candles = self.__client.get_candles(self.__currency, limit=1, period='M1')

            for c in candles:
                date = parse(c['timestamp'])
                cursor.execute(self.SELECT_QUERY.format(date, self.__currency))
                if cursor.fetchone():
                    cursor.execute(self.UPDATE_QUERY.format(date, c['min'], c['max'], self.__currency))
                    print("Updating entry (date: {0}, min: {1}, max: {2})".format(date, c['min'], c['max']))
                else:
                    prev_date = date - timedelta(minutes=1)
                    cursor.execute(self.UPDATE_CLOSING_PRICE.format(prev_date, c['close'], self.__currency))
                    print("Updating closing price (date: {0}, close: {1})".format(prev_date, c['close']))
                    cursor.execute(self.REPLACE_QUERY.format(date, c['min'], c['max'], self.__currency))
                    print("Adding entry (date: {0}, min: {1}, max: {2})".format(date, c['min'], c['max']))
            conn.commit()
        except NoDataException:
            pass

        conn.close()

    def __get_minmax_over_time(self, start_in_min: int, offset_in_min: int) -> list:
        """Returns a list of min, max values for a given period of time.

        Args:
            start_in_min: start time to select min, max values for the given currency
            offset_in_min: offset prior to the start time

        Returns:
            A list of dictionaries containing date, min and max values.

        """
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()

        query = self.SELECT_MINMAX_QUERY.format(self.__currency, start_in_min + offset_in_min)
        rows = cursor.execute(query).fetchall()
        conn.close()

        result = []
        for r in rows:
            date = parse(r[0])
            result.append({'date': date, 'min': r[1], 'max': r[2]})
        return result

    def get_highlow_over_time(self, start_in_min: int, offset_in_min: int) -> list:
        """Returns a list of high, low values for a given period of time.

        Args:
            start_in_min: start time to select min, max values for the given currency
            offset_in_min: offset prior to the start time

        Returns:
            A list of dictionaries containing min and max values.

        """
        minmax = self.__get_minmax_over_time(start_in_min, offset_in_min)
        if not minmax:
            raise NoDataException
        vals = []
        for shift in range(start_in_min):
            curr_min, curr_max = None, None
            window = minmax[shift:shift + offset_in_min]

            for entry in window:
                curr_max = entry['max'] if (curr_max is None or entry['max'] > curr_max) else curr_max
                curr_min = entry['min'] if (curr_min is None or entry['min'] < curr_min) else curr_min
            vals.append({'min': curr_min, 'max': curr_max})
        if not vals:
            raise NoDataException
        return vals

    def get_closing_prices_over_time(self, period_in_min: int, offset_in_min: int) -> list:
        """Returns a list of closing prices for a given period of time with an offset in the past (all in minutes).
        E.g. if period_in_min=20 and offset_in_min=10, then it means 20 values (1 per minute) ranging from 30 minutes to
        10 minutes in the past.

        Args:
            period_in_min: number of minutes to collect data for
            offset_in_min: minute shift to the past

        Returns:
             A list of values containing closing prices.
        """
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()

        query = self.SELECT_MINMAX_QUERY.format(self.__currency, period_in_min + offset_in_min, period_in_min)
        rows = cursor.execute(query).fetchall()
        conn.close()

        return rows
