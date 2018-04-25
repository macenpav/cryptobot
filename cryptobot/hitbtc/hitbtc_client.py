import requests


class NoDataException(Exception):
    pass


class HitBtcClient(object):
    def __init__(self, url, public_key, secret):
        self.__url = url + '/api/2'
        self.__session = requests.session()
        self.__session.auth = (public_key, secret)

    def __get_data(self, url):
        data = self.__session.get(self.__url + "/" + url).json()
        if not data:
            raise NoDataException
        return data

    def __get_balance(self, type, currency=None):
        balances = self.__get_data("{0}/balance".format(type))
        out = list()
        if currency is None:
            for b in balances:
                if float(b['available']) > 0.0 or float(b['reserved']) > 0.0:
                    out.append(b)
        else:
            for b in balances:
                if b['currency'] == currency:
                    out.append(b)
        return out

    def get_candles(self, symbol, **kwargs):
        opt = ""
        # max=1000, default=100
        if kwargs['limit']:
            opt += "?limit={0}".format(kwargs['limit'])
        # options=M1 (one minute), M3, M5, M15, M30, H1, H4, D1, D7, 1M (one month), default=M30 (30 minutes)
        if kwargs['period']:
            opt += "&" if len(opt) > 0 else "?"
            opt += "period={0}".format(kwargs['period'])
        return self.__get_data(r"public/candles/{0}{1}".format(symbol, opt))

    def get_account_balance(self, currency=None):
        return self.__get_balance('account', currency)

    def get_trading_balance(self, currency=None):
        return self.__get_balance('trading', currency)

    def get_ticker_info(self):
        return self.__get_data(r"public/ticker/BTCUSD")