import sqlite3
from dateutil.parser import parse
import threading
from time import sleep
from cryptobot.hitbtc.hitbtc_client import NoDataException
from cryptobot.hitbtc.hitbtc_client import InvalidDataException


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

    DB_NAME = 'my_db.db'

    HOURS_TO_MINUTES = 60

    def __init__(self, client, currency='BTCUSD'):
        self.__currency = currency
        self.__client = client
        self.__t_sync = None
        self.EXISTS_QUERY = "SELECT 1 FROM MinMax_{0} WHERE date=:date LIMIT 1".format(currency)
        self.REPLACE_QUERY = "REPLACE INTO MinMax_{0} (date, min, max, closing_price) VALUES (:date, :min, :max, :cp)"\
                             .format(currency)
        self.UPDATE_QUERY = "UPDATE MinMax_{0} SET min=(CASE WHEN min>:min THEN :min ELSE min END), " \
                            "max=(CASE WHEN max>:max THEN max ELSE :max END), closing_price=:cp WHERE date=:date"\
                            .format(currency)
        self.CREATE_QUERY = "CREATE TABLE IF NOT EXISTS MinMax_{0} " \
                            "(date DATETIME PRIMARY KEY, min FLOAT, max FLOAT, closing_price FLOAT DEFAULT NULL, " \
                            "UNIQUE(date, min, max, closing_price) ON CONFLICT REPLACE)".format(currency)
        self.SELECT_MINMAX_QUERY = "SELECT date, min, max FROM MinMax_{0} " \
                                   "WHERE datetime(date) >= datetime('now', :minutes) " \
                                   "ORDER BY date ASC".format(currency)
        self.SELECT_CLOSING_QUERY = "SELECT date, closing_price FROM MinMax_{0} " \
                                    "WHERE datetime(date) >= datetime('now', :minutes) " \
                                    "ORDER BY date ASC LIMIT :limit".format(currency)

        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        cursor.execute(self.CREATE_QUERY.format(currency))
        conn.commit()
        conn.close()

    def __del__(self):
        if self.__t_sync is not None:
            self.__t_sync.join()

    def __initialize_candles(self, cursor, period):
        candles = self.__client.get_candles(self.__currency, limit=1000, period=period)
        for c in candles:
            date = parse(c['timestamp'])
            args = {"min": c['min'], "max": c['max'], "cp": c['close'], "date": date}
            cursor.execute(self.REPLACE_QUERY.format(self.__currency), args)

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
        """Returns a list of min, max values for a given period of time.
        """
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()
        try:
            candles = self.__client.get_candles(self.__currency, limit=1, period='M1')

            for c in candles:
                date = parse(c['timestamp'])
                cursor.execute(self.EXISTS_QUERY, {"date": date})
                args = {"min": c['min'], "max": c['max'], "cp": c['close'], "date": date}
                if cursor.fetchone():
                    cursor.execute(self.UPDATE_QUERY, args)
                    print("Updating entry (date: {0}, min: {1}, max: {2}, closing_price: {3})"
                          .format(date, c['min'], c['max'], c['close']))
                else:
                    cursor.execute(self.REPLACE_QUERY, args)
                    print("Adding entry (date: {0}, min: {1}, max: {2}, closing_price: {3})"
                          .format(date, c['min'], c['max'], c['close']))
            conn.commit()
        except NoDataException:
            pass

        conn.close()

    def __get_minmax_over_time(self, num_minutes: int) -> list:
        """Returns a list of min, max values for a given period of time.

        Args:
            num_minutes: length of the time period in minutes until now

        Returns:
            A list of dictionaries containing date, min and max values.

        """
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()

        args = {"minutes": "-{0} minute".format(num_minutes)}
        rows = cursor.execute(self.SELECT_MINMAX_QUERY, args).fetchall()
        conn.close()

        result = []
        for r in rows:
            result.append({'date': parse(r[0]), 'min': r[1], 'max': r[2]})
        return result

    def get_highlow_over_time(self, num_minutes: int, offset_from_now: int) -> list:
        """Returns a list of high, low values for a given period of time.

        Args:
            num_minutes: length of the time period in minutes
            offset_from_now: offset from current time in minutes

        Returns:
            A list of dictionaries containing min and max values.

        """
        minmax = self.__get_minmax_over_time(num_minutes)
        if not minmax:
            raise NoDataException
        if len(minmax) is not num_minutes:
            raise InvalidDataException("Expected: {0}, Retrieved: {1}".format(num_minutes, len(minmax)))

        vals = []
        for shift in range(num_minutes):
            curr_min, curr_max = None, None
            window = minmax[offset_from_now:offset_from_now + num_minutes]
            date = minmax[offset_from_now]
            for e in window:
                curr_max = e['max'] if (curr_max is None or e['max'] > curr_max) else curr_max
                curr_min = e['min'] if (curr_min is None or e['min'] < curr_min) else curr_min
            vals.append({'date': date, 'min': curr_min, 'max': curr_max})
        if not vals:
            raise NoDataException
        return vals

    def get_closing_prices_over_time(self, num_minutes: int, offset_from_now: int) -> list:
        """Returns a list of closing prices for a given period of time with an offset in the past (all in minutes).
        E.g. if period_in_min=20 and offset_in_min=10, then it means 20 values (1 per minute) ranging from 30 minutes to
        10 minutes in the past.

        Args:
            num_minutes: length of the time period in minutes
            offset_from_now: offset from current time in minutes

        Returns:
             A list of values containing closing prices.
        """
        conn = sqlite3.connect(self.DB_NAME)
        cursor = conn.cursor()

        args = {"minutes": "-{0} minute".format(num_minutes + offset_from_now), "limit": num_minutes}
        rows = cursor.execute(self.SELECT_CLOSING_QUERY, args).fetchall()
        conn.close()

        result = []
        for r in rows:
            result.append({'date': parse(r[0]), 'closing_price': r[1]})

        if len(result) is not num_minutes:
            raise InvalidDataException("Expected: {0}, Retrieved: {1}".format(num_minutes, len(result)))

        return rows
