import matplotlib.pyplot as plt
import numpy as np
from cryptobot.hitbtc.hitbtc_client import *
from cryptobot.data_analysis.data_collector import DataCollector
from datetime import datetime
from time import sleep
import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    client = HitBtcClient("https://api.hitbtc.com",
                          config['hitbtc_credentials']['public_key'],
                          config['hitbtc_credentials']['secret'])

    dc = DataCollector(client)

    num_of_minutes = 12 * 60

    while True:
        conversion_line, base_line, leading_span_A, leading_span_B = [], [], [], []

        period_len = 10.0 / 9.0

        short_period = int(9 * period_len)
        mid_period = int(26 * period_len)
        long_period = int(52 * period_len)

        try:
            for i in dc.get_highlow_over_time(num_of_minutes, short_period):
                conversion_line.append((i['min'] + i['max']) / 2.0)

            for i in dc.get_highlow_over_time(num_of_minutes, mid_period):
                base_line.append((i['min'] + i['max']) / 2.0)

            x = range(len(conversion_line))
            for i in x:
                leading_span_A.append((conversion_line[i] + base_line[i]) / 2.0)

            for i in dc.get_highlow_over_time(num_of_minutes, long_period):
                leading_span_B.append((i['min'] + i['max']) / 2.0)

            #lagging_span = dc.get_closing_price(num_of_minutes, mid_period)

            plt.plot(x, conversion_line)
            plt.plot(x, base_line)
            plt.plot(x, leading_span_A, x, leading_span_B)
            #plt.plot(x, lagging_span)

            a = np.array(leading_span_A)
            b = np.array(leading_span_B)

            plt.fill_between(x, leading_span_A, leading_span_B, where=a >= b, facecolor='green',
                             interpolate=True, alpha=0.25)
            plt.fill_between(x, leading_span_A, leading_span_B, where=a <= b, facecolor='red',
                             interpolate=True, alpha=0.25)
            print("Refreshing graph... current time is {0}".format(datetime.now()))

            plt.pause(30)
            plt.clf()

        except NoDataException:
            print("Failed to receive data... current time is {0}".format(datetime.now()))
            sleep(1)

    plt.show()
