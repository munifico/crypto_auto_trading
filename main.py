import datetime
import time

import config
import pyupbit
import talib

class Trader:
    open_order_dict = {}

    def __init__(self, access, secret):

        self.total_seed = 0  # get_balances 메소드 실행 시 조회 됨
        self.split = 16  # 포지션 사이즈 유지를 위해 1 split은 남겨두므로 보유하고 싶은 수량의 + 1을 입력해야 함
        self.pos_size = 10000

        self.balances = {}
        self.buy_wait_time = 270  # 정확히 5분으로 하면 다음 시그널 발생 시 주문이 늦어질 수 있으므로 5분 미만으로 설정할 것
        self.sell_wait_time = 10
        self.buy_blocker = {}

        self.open_orders = {}

        self.upbit = pyupbit.Upbit(access, secret)

        self.tickers = []

        # 최초 감시 리스트 조회
        self.get_tickers()

        while True:

            now = datetime.datetime.now()

            self.get_balances()  # 잔고 조회
            self.resize_position(0.1)  # 포지션 사이즈 재설정 (ex. 0.1 == 10%)

            # 매수, 매도 방해를 피하기 위해 특정 시간에만 ticker 리스트 갱신
            if now.minute in config.config['time_check']:
                self.get_tickers()  # 티커 갱신

            data = self.get_datas(self.tickers)  # 매수 분석용 데이터 조회

            buy_list = self.get_buy_list(data)  # 매수 티커 리스트

            data = self.get_datas(self.get_holding_tickers())  # 추가매수, 매도 분석용 보유 잔고 가격 데이터 조회

            # buy_more_list = self.get_buy_more_list(data)  # 추가매수 티커 리스트
            sell_list = self.get_sell_list(data)  # 매도 티커 리스트

            # 매수
            if buy_list:
                for ticker in buy_list:
                    self.buy(ticker[0], ticker[1]['buy_price'], ticker[1]['size'])

            # 추가매수
            # if buy_more_list:
            #     for ticker in buy_more_list:
            #         self.buy(ticker[0], ticker[1]['buy_price'], ticker[1]['size'])

            # 매도
            if sell_list:
                for ticker in sell_list:
                    self.sell(ticker[0], ticker[1]['sell_price'], ticker[1]['size'])

            # 미체결 관리
            self.remove_open_order()  # 매뉴얼로 취소됐거나 시간 초과 미체결 주문 삭제

    def get_tickers(self):
        """
        특정 필터를 거쳐 선별된 티커 리스트 생성

        :return: 티커 리스트
        """

        all_tickers = pyupbit.get_tickers(fiat="KRW")

        self.tickers = self.get_watch_list(self.get_datas(all_tickers))

    def check_tradable(self, price, limit_percent):
        """
        호가 단위 퍼센티지를 계산하여 전달받은 인자 이하일 때만 True 반환
        :param price: 현재 가격
        :param limit_percent: 호가 단위 퍼센티지 (ex. 0.5)
        :return: 거래 가능 여부(boolean)
        """
        price_unit = self.get_price_unit(price)

        price_unit_percent = price_unit / price * 100

        if price_unit_percent <= limit_percent:
            return True

        return False

    def get_price_unit(self, price):

        if price >= 2000000:
            return 1000
        elif price >= 1000000:
            return 500
        elif price >= 500000:
            return 100
        elif price >= 100000:
            return 50
        elif price >= 10000:
            return 10
        elif price >= 1000:
            return 5
        elif price >= 100:
            return 1
        elif price >= 10:
            return 0.1
        elif price >= 1:
            return 0.01
        elif price >= 0.1:
            return 0.001
        else:
            return 0.0001

    def get_datas(self, tickers, timeframe='minutes5', count=200):
        """
        티커 리스트를 전달 받아 조회된 데이터 반환

        :param timeframe: 가격 정보의 타임프레임
        :param count: 가격 정보 row 갯수
        :return: 데이터들을 담은 딕셔너리
        """
        data_dict = {}

        for ticker in tickers:

            start_time = datetime.datetime.now()

            data = pyupbit.get_ohlcv(ticker=ticker, count=count, interval=timeframe, to=None, period=0.1)

            if isinstance(data, type(None)):
                print("데이터를 수신받지 못했습니다.")
                end_time = datetime.datetime.now()
                self.regulate_time(start_time, end_time, 0.1)
                continue

            data_dict[ticker] = data

            end_time = datetime.datetime.now()

            self.regulate_time(start_time, end_time, 0.1)

        return data_dict

    def get_watch_list(self, data_dict):
        """
        매수 감시 티커 리스트 선별 메소드

        :param data_dict: 데이터들이 담긴 리스트
        :return: 특정 조건에 따라 선별된 티커 리스트 (매수감시 목적)
        """
        watch_list = []

        for data in data_dict:

            if self.check_tradable(data_dict[data]['close'][-1], 0.5) \
                    and data_dict[data]['value'][-1] >= 500000000:
                watch_list.append(data)

        return watch_list

    def get_buy_list(self, data_dict):
        """
        전달 받은 데이터들의 조건 부합 여부 체크 후 최종 매수 리스트 반환
        :param data_dict: 데이터
        :return: 매수 관련 데이터를 담은 데이터프레임 리스트
        """
        buy_list_dict = {}

        for data in data_dict:

            # 매수조건 통과 여부 체크
            res = self.check_buyable(data_dict[data])

            # 매수조건 통과, 티커 미보유, 미체결 없음 충족 시 buy_list_dict에 추가
            if res and data not in self.balances and data not in self.open_orders:
                # 모든 조건 충족 시 딕셔너리에 정렬 및 매수에 필요한 데이터 담기
                buy_list_dict[data] = {'buy_price': res[0], 'size': res[1], 'volatility': res[2]}

                self.buy_blocker[data] = data_dict[data].index[-1]

        # 매수 가능 갯수 계산: 최대 매수 제한 갯수 - 현재 잔고 갯수
        count = self.split - (len(self.balances) - 1)  # 원화도 포함되어 있으므로 잔고 갯수에서 -1

        return self.pick_final_buy(buy_list_dict, 'volatility', count)

    def get_buy_more_list(self, data_dict):

        buy_more_list_dict = {}

        for data in data_dict:
            res = self.check_more_buyable(data_dict[data])

            if data in self.buy_blocker and self.buy_blocker[data].minute == data_dict[data].index[-1].minute:
                continue

            # 추가 매수조건 통과, 티커 보유, 미체결 없음 충족 시 buy_more_list_dict에 추가
            if res \
                    and data in self.balances \
                    and data not in self.open_orders:

                buy_more_list_dict[data] = {'buy_price': res[0], 'size': res[1], 'volatility': res[2]}

        return self.pick_final_buy(buy_more_list_dict, 'volatility', 99)

    def get_sell_list(self, data_dict):

        sell_list_dict = {}

        for data in data_dict:
            res = self.check_sellable(data_dict[data], data)

            if data in self.buy_blocker and self.buy_blocker[data].minute == data_dict[data].index[-1].minute:
                continue

            # 매도조건 통과, 티커 보유, 미체결 없음 충족 시 sell_list_dict에 추가
            if res \
                    and data in self.balances \
                    and data not in self.open_orders:

                sell_list_dict[data] = {'sell_price': res[0], 'size': self.balances[data]['balance']}

        return self.pick_final_sell(sell_list_dict)

    def pick_final_buy(self, buy_list_dict, attr, count=3):
        """
        매수 조건을 통과한 티커들을 일련의 정렬 과정을 거친 후 상위 N개만 리스트로 반환

        :param buy_list_dict: 매수 조건 통과한 티커와 필요 데이터
        :param attr: 정렬 기준 속성
        :param count: 최종 리스트의 길이
        :return: 정렬 후 선별된 티커 리스트
        """
        final_buy = {}

        if len(buy_list_dict) > 0 and count > 0:
            if len(buy_list_dict) > count:
                final_buy = sorted(buy_list_dict.items(), key=lambda x: x[1][attr], reverse=True)[:count]

            elif len(buy_list_dict) <= count:
                final_buy = sorted(buy_list_dict.items(), key=lambda x: x[1][attr], reverse=True)[:len(buy_list_dict)]

        return final_buy

    def pick_final_sell(self, sell_list_dict):
        """
        매도에서는 불필요한 정렬이지만 매수와 같은 형식을 유지하기 위해 사용
        """
        final_sell = {}

        if len(sell_list_dict) > 0:
            final_sell = sorted(sell_list_dict.items(), key=lambda x: x[1]['size'], reverse=True)

        return final_sell

    def check_buyable(self, data):
        """
        :param data: 티커의 개별 데이터
        :return: 시그널 발생 시 매수에 필요한 데이터들의 리스트(매수가, 사이즈, 정렬 속성)
        """

        # 지표 리스트
        atr = talib.ATR(data['high'], data['low'], data['close'], timeperiod=20)
        volatility = atr[-2] / data['close'][-2]

        # 시그널 발생 여부 체크
        if data['value'][-2] >= 300000000 \
                and data['close'][-31] > data['close'][-2] \
                and data['close'][-11] * 0.95 >= data['close'][-2]:
            return [data['open'][-1], self.pos_size / data['open'][-1], volatility]

        return False

    def check_more_buyable(self, data):
        """
        :param data: 티커의 개별 데이터
        :return: 시그널 발생 시 매수에 필요한 데이터들의 리스트(매수가, 사이즈, 정렬 속성)
        """

        # 지표 리스트
        atr = talib.ATR(data['high'], data['low'], data['close'], timeperiod=20)
        volatility = atr[-2] / data['close'][-2]
        now = datetime.datetime.now()

        # sma = talib.SMA()
        # 시그널 발생 여부 체크
        if False:
            return [data['open'][-1], self.pos_size / data['open'][-1], volatility]

        return False

    def check_sellable(self, data, ticker):
        """
        :param data: 티커의 개별 데이터
        :return: 시그널 발생 시 매수에 필요한 데이터들의 리스트(매수가, 사이즈, 정렬 속성)
        """

        # 지표 리스트
        atr = talib.ATR(data['high'], data['low'], data['close'], timeperiod=20)
        volatility = atr[-2] / data['close'][-2]
        SMA_slow = talib.SMA(data['close'], timeperiod=200)

        # 시그널 발생 여부 체크
        if SMA_slow[-2] <= data['close'][-2]:
            return [data['close'][-1], self.balances[ticker]['balance'], volatility]

        return False

    def buy(self, ticker, price, size):

        price = pyupbit.get_tick_size(price)

        ret = self.upbit.buy_limit_order(ticker, price, size)

        if ret:
            self.record_open_order(ret, ticker)
            print('매수', ret)

    def sell(self, ticker, price, size):

        price = pyupbit.get_tick_size(price)

        ret = self.upbit.sell_limit_order(ticker, price, size)

        if ret:
            self.record_open_order(ret, ticker)
            print('매도', ret)

    def record_open_order(self, ret, ticker):

        if ticker not in self.open_orders:
            self.open_orders[ticker] = {}

        self.open_orders[ticker]['uuid'] = ret['uuid']
        self.open_orders[ticker]['side'] = ret['side']
        self.open_orders[ticker]['ord_type'] = ret['ord_type']
        self.open_orders[ticker]['price'] = ret['price']
        self.open_orders[ticker]['state'] = ret['state']
        self.open_orders[ticker]['market'] = ret['market']
        self.open_orders[ticker]['created_at'] = datetime.datetime.now()  # 대기 시간 계산 편의를 위해 datetime 모듈로 대체
        self.open_orders[ticker]['volume'] = ret['volume']
        self.open_orders[ticker]['remaining_volume'] = ret['remaining_volume']
        self.open_orders[ticker]['reserved_fee'] = ret['reserved_fee']
        self.open_orders[ticker]['remaining_fee'] = ret['remaining_fee']
        self.open_orders[ticker]['paid_fee'] = ret['paid_fee']
        self.open_orders[ticker]['locked'] = ret['locked']
        self.open_orders[ticker]['executed_volume'] = ret['executed_volume']

    def remove_open_order(self):

        to_be_deleted = []

        for ticker in self.open_orders:
            start_time = datetime.datetime.now()
            open_order = self.upbit.get_order(ticker)
            end_time = datetime.datetime.now()

            self.regulate_time(start_time, end_time, 0.15)

            # 수동으로 주문 취소 시 삭제될 수 있게끔 주문 조회
            if len(open_order) <= 0:
                to_be_deleted.append(ticker)

            side = self.open_orders[ticker]['side']
            uuid = self.open_orders[ticker]['uuid']
            now = datetime.datetime.now()
            diff = now - self.open_orders[ticker]['created_at']

            if side == 'bid':

                if diff.seconds >= self.buy_wait_time:

                    start_time = datetime.datetime.now()
                    ret = self.upbit.cancel_order(uuid)
                    end_time = datetime.datetime.now()

                    self.regulate_time(start_time, end_time, 0.15)

                    if ret:
                        to_be_deleted.append(ticker)

            if side == 'ask':

                if diff.seconds >= self.sell_wait_time:

                    start_time = datetime.datetime.now()
                    ret = self.upbit.cancel_order(uuid)
                    end_time = datetime.datetime.now()

                    self.regulate_time(start_time, end_time, 0.15)

                    if ret:
                        to_be_deleted.append(ticker)

        for ticker in to_be_deleted:
            del self.open_orders[ticker]

    def regulate_time(self, start_time, end_time, threshold):
        """
        요청 제한 횟수 맞추기
        시작과 종료 시간간의 차가 기준 시간보다 적을 시 적은 만큼 sleep
        :param start_time: 시작 시간
        :param end_time:  종료 시간
        :param threshold: 기준 시간
        :return: -
        """
        diff = end_time - start_time

        if diff.seconds < threshold:
            time.sleep(threshold - diff.seconds)

    #################################################################

    def get_balances(self):

        self.balances = {}

        balances = self.upbit.get_balances()

        self.total_seed = int(float(balances[0]['balance']))

        for idx, balance in enumerate(balances):
            ticker = "KRW" + '-' + balances[idx]['currency']

            if ticker not in self.balances:
                self.balances[ticker] = {}

            self.balances[ticker]['balance'] = balances[idx]['balance']

    def get_holding_tickers(self):

        holding_tickers = []

        for ticker in self.balances:

            if ticker == 'KRW-KRW':
                continue

            holding_tickers.append(ticker)

        return holding_tickers

    def resize_position(self, percent):
        """
        토탈 시드머니 증가량을 체크해서 포지션 사이즈를 재설정함
        :param percent: 증가량 
        :return: -
        """

        if self.total_seed / self.pos_size >= self.split + percent:

            before_pos_size = self.pos_size

            self.pos_size = int(self.total_seed / self.split)

            print(f'포지션 사이즈가 재설정 됩니다. {format(before_pos_size, ",")}원 => {format(self.pos_size, ",")}원')


trader = Trader(access=config.config['access'], secret=config.config['secret'])
