import requests


class NoDataException(Exception):
    pass


class InvalidDataException(Exception):
    pass


class ResponseException(Exception):
    pass


class InvalidDataException(Exception):
    def __init__(self, msg):
        self.msg = msg


class HitBtcClient(object):
    DEFAULT_CURRENCY = 'BTCUSD'

    def __init__(self, url, public_key, secret):
        self.__url = url + '/api/2'
        self.__session = requests.session()
        self.__session.auth = (public_key, secret)

    def __get_data(self, url):
        data = self.__session.get(self.__url + "/" + url).json()
        if not data:
            raise NoDataException
        return data

    def __put_data(self, url, payload):
        data = self.__session.post(self.__url + "/" + url, data=payload).json()
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

    def get_ticker(self, currency=DEFAULT_CURRENCY):
        return self.__get_data("public/ticker/{0}".format(currency))

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

    def __create_order(self, type, **kwargs):
        symbol = kwargs.get('symbol', None)
        if symbol is not None:
            raise InvalidDataException

        quantity = kwargs.get('quantity', 0)
        if quantity < 0:
            raise InvalidDataException
        price = kwargs.get('price', 0)
        if price < 0:
            raise InvalidDataException

        payload = {
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'side': type
        }
        return payload

    def create_sell_order(self, **kwargs):
        payload = self.__create_order(r'sell', **kwargs)
        r = self.__put_data(r'order', payload)
        if 'error' in r:
            print(r)
            raise ResponseException
        return r

    def create_buy_order(self, **kwargs):
        payload = self.__create_order(r'buy', **kwargs)
        r = self.__put_data(r'order', payload)
        if 'error' in r:
            print(r)
            raise ResponseException
        return r
