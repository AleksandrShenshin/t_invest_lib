import asyncio
import logging
from decouple import config
from datetime import datetime, timedelta, timezone
from t_tech.invest import Client, InstrumentIdType, CandleInterval
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

logger = logging.getLogger(__name__)


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
                    if e.details == '40003':
                        return -1, ticker_param, f"ERROR: get_param_instrument(): get_instrument_by({ticker_instr}) - T_TOKEN {e.code}"
                    else:
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
            logger.error(f"ERROR stream_ticker_one_minute({ticker}): START: {ticker} not found in shared_tasks = {shared_tasks}")
            return
        figi = shared_tasks[ticker]['figi']
    logger.warning(f"START stream_ticker_one_minute({ticker})")

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
                                logger.error(f"ERROR stream_ticker_one_minute({ticker}): except ValueError: candle = {candle}")

                            if not shared_tasks[ticker]['depends']:
                                # Все задачи с ticker завершены - завершаем опрос данного ticker
                                should_unsubscribe = True
                                shared_tasks.pop(ticker, None)
                                logger.warning(f"stream_ticker_one_minute(): no task for {ticker} - FINISH")
                                await asyncio.sleep(2)
                                break
                        else:
                            logger.error(f"ERROR stream_ticker_one_minute({ticker}): {ticker} not found in shared_tasks = {shared_tasks}")
    except Exception as e:
        logger.error(f"ERROR critical stream_ticker_one_minute({ticker}): {e}")
    finally:
        logger.warning(f"Finished stream_ticker_one_minute({ticker})")


async def stream_list_figi_five_minute(lock_data_long5, data_tasks_long5, market):
    should_unsubscribe = False
    async with lock_data_long5:
        list_figi = list(data_tasks_long5[market]['tickers'])
    logger.warning(f"START stream_list_figi_five_minute({market}) -- {list_figi}")

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
                                if len(data_tasks_long5[market]['tickers'][candle.figi]['atr']) > 120:
                                    del data_tasks_long5[market]['tickers'][candle.figi]['atr'][0]

                                # инициализируем cur_atr для новой 5 мин
                                try:
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['high'] = float(quotation_to_decimal(candle.high))
                                    data_tasks_long5[market]['tickers'][candle.figi]['cur_atr']['low'] = float(quotation_to_decimal(candle.low))
                                    data_tasks_long5[market]['tickers'][candle.figi]['prev_bin'] = (candle.last_trade_ts.minute // 5)
                                except ValueError:
                                    logger.error(f"ERROR stream_list_figi_five_minute({market}): except ValueError: candle = {candle}")
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
                                logger.error(f"ERROR stream_list_figi_five_minute({market}): except ValueError: candle = {candle}")

                        if not data_tasks_long5[market]['depends']:
                            # Задача по market завершена - завершаем опрос
                            should_unsubscribe = True
                            data_tasks_long5.pop(market, None)
                            logger.warning(f"stream_list_figi_five_minute({market}): no task - FINISH")
                            await asyncio.sleep(2)              # ожидание завершения should_unsubscribe
                            break
    except Exception as e:
        logger.error(f"ERROR critical stream_list_figi_five_minute({market}): {e}")
    finally:
        logger.warning(f"Finished stream_list_figi_five_minute({market})")


async def stream_get_last_5sec_candle(lock_data_throws, data_tasks_throws, market):
    async with lock_data_throws:
        list_figi = list(data_tasks_throws[market]['tickers'])
    logger.warning(f"START stream_get_last_5sec_candle({market}) -- {list_figi}")

    try:
        with Client(config('T_TOKEN')) as client:
            while True:
                # sleep until next 5sec
                now = datetime.now(timezone.utc)
                sec = now.second + now.microsecond / 1_000_000
                # offset to next 0,5,10,...55
                delta = 5 - (sec % 5)
                await asyncio.sleep(delta)

                async with lock_data_throws:
                    now = datetime.now(timezone.utc)
                    for figi in list_figi:
                        candles = client.market_data.get_candles(
                            instrument_id=figi,
                            from_=now - timedelta(seconds=10),
                            to=now,
                            interval=CandleInterval.CANDLE_INTERVAL_5_SEC,
                        ).candles
                        if candles:
                            data_tasks_throws[market]['tickers'][figi]['candle']['high'] = float(quotation_to_decimal(candles[-1].high))
                            data_tasks_throws[market]['tickers'][figi]['candle']['low'] = float(quotation_to_decimal(candles[-1].low))
                            data_tasks_throws[market]['tickers'][figi]['candle']['open'] = float(quotation_to_decimal(candles[-1].open))
                            data_tasks_throws[market]['tickers'][figi]['candle']['close'] = float(quotation_to_decimal(candles[-1].close))
                            data_tasks_throws[market]['tickers'][figi]['candle']['time_received'] = candles[-1].time
                        else:
                            # в данный интервал не было сделок
                            pass

                    if not data_tasks_throws[market]['depends']:
                        # Задача по market завершена - завершаем опрос
                        data_tasks_throws.pop(market, None)
                        logger.warning(f"FINISH stream_get_last_5sec_candle({market})")
                        break
    except Exception as e:
        logger.error(f"ERROR critical: Finish stream_get_last_5sec_candle({market}): {e}")
    finally:
        logger.warning(f"Finished stream_get_last_5sec_candle({market})")
