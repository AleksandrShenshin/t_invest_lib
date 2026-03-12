import asyncio
from decouple import config
from t_tech.invest import Client, InstrumentIdType
from t_tech.invest.exceptions import RequestError
from t_tech.invest.utils import quotation_to_decimal
from t_tech.invest import (
    AsyncClient,
    CandleInstrument,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)

async def get_param_instrument(ticker_instr):
    ticker_param = {'ticker': '', 'figi': '', 'precision': ''}
    with Client(config('T_TOKEN')) as client:
        for ticker in [ticker_instr, ticker_instr.upper()]:
            for class_code in ["SPBFUT", "TQBR"]:
                try:
                    instrument_response = client.instruments.get_instrument_by(
                        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                        id=ticker,
                        class_code=class_code
                    )
                    break
                except RequestError as e:
                    continue
            else:
                continue
            break
        else:
            return -1, ticker_param, f"ERROR: get_param_instrument(): get_instrument_by({ticker_instr})"

        ticker_param['ticker'] = instrument_response.instrument.ticker
        ticker_param['figi'] = instrument_response.instrument.figi
        ticker_param['precision'] = quotation_to_decimal(instrument_response.instrument.min_price_increment)
        return 0, ticker_param, None


async def stream_ticker_one_minute(lock, shared_tasks, ticker):
    should_unsubscribe = False
    async with lock:
        if ticker not in shared_tasks:
            print(f"ERROR stream_ticker_one_minute({ticker}): START: {ticker} not found in shared_tasks = {shared_tasks}", flush=True)
            return
        figi = shared_tasks[ticker]['figi']

    async def request_iterator():
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[
                    CandleInstrument(
                        figi=figi,
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                    )
                ],
            )
        )

        elapsed = 0
        while True:
            if should_unsubscribe:
                # Отмена подписки
                yield MarketDataRequest(
                    subscribe_candles_request=SubscribeCandlesRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE,
                        instruments=[
                            CandleInstrument(
                                figi=figi,
                                interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                            )
                        ],
                    )
                )
                break

            await asyncio.sleep(1)
            elapsed += 1

            # Heartbeat: не чаще 1 раза в 30–60 секунд
            if elapsed >= 35:
                # Пустой запрос, поддерживающий стрим "живым"
                yield MarketDataRequest()
                elapsed = 0

    try:
        async with AsyncClient(config('T_TOKEN')) as client:
            async for marketdata in client.market_data_stream.market_data_stream(
                    request_iterator()
            ):
                if marketdata.candle:
                    # Данные — свечи
                    async with lock:
                        candle = marketdata.candle
                        if ticker in shared_tasks:
                            try:
                                shared_tasks[ticker]['high'] = float(quotation_to_decimal(candle.high))
                                shared_tasks[ticker]['low'] = float(quotation_to_decimal(candle.low))
                                shared_tasks[ticker]['volume'] = int(candle.volume)
                                shared_tasks[ticker]['time_received'] = candle.last_trade_ts
                            except ValueError:
                                print(f"ERROR stream_ticker_one_minute({ticker}): except ValueError: candle = {candle}", flush=True)

                            if not shared_tasks[ticker]['depends']:
                                # Все задачи с ticker завершены - завершаем опрос данного ticker
                                should_unsubscribe = True
                                shared_tasks.pop(ticker, None)
                                await asyncio.sleep(2)
                                break
                        else:
                            print(f"ERROR stream_ticker_one_minute({ticker}): {ticker} not found in shared_tasks = {shared_tasks}", flush=True)
    except Exception as e:
        print(f"ERROR critical stream_ticker_one_minute({ticker}): {e}", flush=True)


async def stream_list_figi_five_minute(lock_data_long5, data_tasks_long5, market):
    should_unsubscribe = False
    async with lock_data_long5:
        list_figi = list(data_tasks_long5[market]['tickers'])

    async def request_iterator():
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[
                    CandleInstrument(
                        figi=figi,
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
                    )
                    for figi in list_figi
                ],
            )
        )

        elapsed = 0
        while True:
            if should_unsubscribe:
                # Отмена подписки
                yield MarketDataRequest(
                    subscribe_candles_request=SubscribeCandlesRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE,
                        instruments=[
                            CandleInstrument(
                                figi=figi,
                                interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
                            )
                            for figi in list_figi
                        ],
                    )
                )
                break

            await asyncio.sleep(1)
            elapsed += 1

            # Heartbeat: не чаще 1 раза в 30–60 секунд
            if elapsed >= 35:
                # Пустой запрос, поддерживающий стрим "живым"
                yield MarketDataRequest()
                elapsed = 0

    try:
        async with AsyncClient(config('T_TOKEN')) as client:
            async for marketdata in client.market_data_stream.market_data_stream(
                    request_iterator()
            ):
                if marketdata.candle:
                    # Данные — свечи
                    async with lock_data_long5:
                        candle = marketdata.candle
                        if data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['time_received'] != None:
                            # Проверка перехода между 5 мин
                            if data_tasks_long5[market]['tickers'][candle.figi]['prev_bin'] != (candle.last_trade_ts.minute // 5) and \
                               data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['time_received'] < candle.last_trade_ts:
                                # произошёл переход 5 мин

                                # обновляем массив atr
                                data_tasks_long5[market]['tickers'][candle.figi]['atr'].append(
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high'] -
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low']
                                )
                                if len(data_tasks_long5[market]['tickers'][candle.figi]['atr']) > 5:
                                    del data_tasks_long5[market]['tickers'][candle.figi]['atr'][0]

                                # инициализируем cur_atr для новой 5 мин
                                try:
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high'] = float(quotation_to_decimal(candle.high))
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low'] = float(quotation_to_decimal(candle.low))
                                    data_tasks_long5[market]['tickers'][candle.figi]['prev_bin'] = (candle.last_trade_ts.minute // 5)
                                except ValueError:
                                    print(f"ERROR stream_list_figi_five_minute({market}): except ValueError: candle = {candle}", flush=True)
                            else:
                                if float(quotation_to_decimal(candle.high)) > data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high']:
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high'] = float(quotation_to_decimal(candle.high))
                                if float(quotation_to_decimal(candle.low)) < data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low']:
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low'] = float(quotation_to_decimal(candle.low))
                            data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['time_received'] = candle.last_trade_ts
                        else:
                            try:
                                # first initialization
                                data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high'] = float(quotation_to_decimal(candle.high))
                                data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low'] = float(quotation_to_decimal(candle.low))
                                data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['time_received'] = candle.last_trade_ts
                                data_tasks_long5[market]['tickers'][candle.figi]['prev_bin'] = (candle.last_trade_ts.minute // 5)
                            except ValueError:
                                print(f"ERROR stream_list_figi_five_minute({market}): except ValueError: candle = {candle}", flush=True)

                        if not data_tasks_long5[market]['depends']:
                            # Задача по market завершена - завершаем опрос
                            should_unsubscribe = True
                            data_tasks_long5.pop(market, None)
                            await asyncio.sleep(2)              # ожидание завершения should_unsubscribe
                            break
    except Exception as e:
        print(f"ERROR critical stream_list_figi_five_minute({market}): {e}", flush=True)
