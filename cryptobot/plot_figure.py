import matplotlib.pyplot as plt
import numpy as np
from cryptobot.hitbtc.hitbtc_client import *
from cryptobot.data_analysis import data_collector
from datetime import datetime
from time import sleep
import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    client = HitBtcClient("https://api.hitbtc.com",
                          config['hitbtc_credentials']['public_key'],
                          config['hitbtc_credentials']['secret'])

    dc = data_collector(client)

    num_of_minutes = 12 * 60

    while True:
        conversion_line, base_line, leading_span_A, leading_span_B = [], [], [], []

        try:
            for i in dc.get_highlow_over_time(num_of_minutes, 10):
                conversion_line.append((i['min'] + i['max']) / 2.0)

            for i in dc.get_highlow_over_time(num_of_minutes, 30):
                base_line.append((i['min'] + i['max']) / 2.0)

            x = range(len(conversion_line))
            for i in x:
                leading_span_A.append((conversion_line[i] + base_line[i]) / 2.0)

            for i in dc.get_highlow_over_time(num_of_minutes, 120):
                leading_span_B.append((i['min'] + i['max']) / 2.0)

            plt.plot(x, conversion_line)
            plt.plot(x, base_line)
            plt.plot(x, leading_span_A, x, leading_span_B)

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
