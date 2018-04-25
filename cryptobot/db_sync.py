from cryptobot.hitbtc.hitbtc_client import HitBtcClient
from cryptobot.data_analysis.data_collector import DataCollector
import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    client = HitBtcClient("https://api.hitbtc.com",
                          config['hitbtc_credentials']['public_key'],
                          config['hitbtc_credentials']['secret'])

    dc = DataCollector(client)
    dc.start_sync()
