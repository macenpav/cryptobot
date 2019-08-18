import sqlite3
from dateutil.parser import parse
import threading
from time import sleep
from cryptobot.hitbtc.hitbtc_client import NoDataException
from cryptobot.hitbtc.hitbtc_client import InvalidDataException
import configparser
from cryptobot.hitbtc.hitbtc_client import HitBtcClient


class SyncThread(threading.Thread):

    def __init__(self, data_collector):
        """Constructor. Fills database with initial data.

        Args:
            data_collector: DataCollector (db accessor) instance.
            refresh_rate: database update in seconds

        Returns:
            SyncThread instance.
        """
        threading.Thread.__init__(self)
        self.data_collector = data_collector
        data_collector.initialize_candles()

    def run(self, refresh_rate=1, duration=None):
        """Periodically updates database.

        Args:
            refresh_rate: database update in seconds (1 second by default)
            duration: how long to update for (infinite by default)

        Returns:
            None
        """
        if duration is None:
            while True:
                self.data_collector.sync_candles()
                sleep(refresh_rate)
        else:
            while duration > 0:
                self.data_collector.sync_candles()
                duration -= refresh_rate


class DataCollector(object):
    def __init__(self, hitbtc_client, currency='BTCUSD'):
        """Constructor.

        Args:
            hitbtc_client: HitBtcClient instance
            currency: type of currency to gather data for
            __dbname: name of the database file

        Returns:
            DataCollector instance.
        """
        self.__dbname = currency + '.db'
        self.__currency = currency
        self.__client = hitbtc_client
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
                            "WHERE datetime(date) >= datetime('now', :min_from) " \
                            "ORDER BY date ASC".format(currency)
        self.SELECT_MINMAX_RANGE_QUERY = "SELECT date, min, max FROM MinMax_{0} " \
                            "WHERE datetime(date) >= datetime('now', :min_from) " \
                            "AND datetime(date) <= datetime('now', :min_to) " \
                            "ORDER BY date ASC".format(currency)
        self.SELECT_CLOSING_QUERY = "SELECT date, closing_price FROM MinMax_{0} " \
                            "WHERE datetime(date) >= datetime('now', :minutes) " \
                            "ORDER BY date ASC LIMIT :limit".format(currency)

        conn = sqlite3.connect(self.__dbname)
        cursor = conn.cursor()
        cursor.execute(self.CREATE_QUERY.format(currency))
        conn.commit()
        conn.close()

    def __del__(self):
        """Destructor. Joins thread if created.

        Args:
            None

        Returns:
            None
        """
        if self.__t_sync is not None:
            self.__t_sync.join()

    def __initialize_candles(self, cursor, period):
        """Initial update of the database.

        Args:
            None

        Returns:
            None
        """
        candles = self.__client.get_candles(self.__currency, limit=1000, period=period)
        for c in candles:
            date = parse(c['timestamp'])
            args = {"min": c['min'], "max": c['max'], "cp": c['close'], "date": date}
            cursor.execute(self.REPLACE_QUERY.format(self.__currency), args)

    def initialize_candles(self):
        """Connects to the database and performs an initial update.

        Args:
            None

        Returns:
            None
        """
        conn = sqlite3.connect(self.__dbname)
        cursor = conn.cursor()
        try:
            for p in ['M1']:
                self.__initialize_candles(cursor, p)
            conn.commit()
        except NoDataException:
            pass
        conn.close()

    def start_sync(self, refresh_rate, duration):
        """Runs a thread to perform periodic update of the database.

        Args:
            refresh_rate: refresh rate in seconds
            duration: duration of the periodic update

        Returns:
            None
        """
        self.__t_sync = SyncThread(self)
        self.__t_sync.run(refresh_rate, duration)

    def sync_candles(self):
        """Performs an update of the database.

        Args:
            None

        Returns:
            None
        """
        conn = sqlite3.connect(self.__dbname)
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

    def get_minmax_over_time(self, min_from: int, min_to: int = 0) -> list:
        """Returns a list of min, max values for a given period of time. Returns all data, thus density of data is
        determined by how often the values are stored.

        Args:
            min_from: start of the time window in minutes
            min_to: end of the time window in minutes

        Returns:
            A list of dictionaries containing date, min and max values.

        """
        conn = sqlite3.connect(self.__dbname)
        cursor = conn.cursor()

        if min_to > min_from:
            raise InvalidDataException

        if min_to > 0:
            args = {"min_from": "-{0} minute".format(min_from),
                    "min_to": "-{0} minute".format(min_to)}
            rows = cursor.execute(self.SELECT_MINMAX_RANGE_QUERY, args).fetchall()
        else:
            args = {"min_from": "-{0} minute".format(min_from)}
            rows = cursor.execute(self.SELECT_MINMAX_QUERY, args).fetchall()
        conn.close()

        result = []
        for r in rows:
            result.append({'date': parse(r[0]), 'min': r[1], 'max': r[2]})
        return result

    def get_minmax_avg_over_time(self, min_from):
        """Returns a min/max average over a period of time.

        Args:
            min_from: minutes in the past (determines conversion line or base line)

        Returns:
            An average of min/max values over a period of time
        """
        data = self.get_minmax_over_time(min_from)
        min_val, max_val = None, None
        for i in data:
            min_val = i['min'] if (i['min'] < min_val or min_val is None) else min_val
            max_val = i['max'] if (i['max'] > max_val or max_val is None) else max_val
        return (min_val + max_val) / 2



    def get_highlow_over_time(self, num_minutes: int, offset_from_now: int) -> list:
        """Returns a list of high, low values for a given period of time.

        Args:
            num_minutes: length of the time period in minutes
            offset_from_now: offset from current time in minutes

        Returns:
            A list of dictionaries containing min and max values.

        """
        minmax = self.get_minmax_over_time(num_minutes)
        if not minmax:
            raise NoDataException

        vals = []
        for shift in range(offset_from_now):
            curr_min, curr_max = None, None
            window = minmax[shift:shift + num_minutes]
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
        conn = sqlite3.connect(self.__dbname)
        cursor = conn.cursor()

        args = {"minutes": "-{0} minute".format(num_minutes + offset_from_now), "limit": num_minutes}
        rows = cursor.execute(self.SELECT_CLOSING_QUERY, args).fetchall()
        conn.close()

        result = []
        for r in rows:
            result.append({'date': parse(r[0]), 'closing_price': r[1]})

        return rows


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('./config.ini')
    client = HitBtcClient("https://api.hitbtc.com",
                          config['hitbtc_credentials']['public_key'],
                          config['hitbtc_credentials']['secret'])

    dc = DataCollector(client)
    dc.start_sync(int(config['settings']['refresh_rate']),
                  int(config['settings']['refresh_duration']) if 'refresh_duration' in config['settings'] else None)
