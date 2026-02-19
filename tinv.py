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
    async with lock:
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
        while True:
            await asyncio.sleep(1)

    async with AsyncClient(config('T_TOKEN')) as client:
        async for marketdata in client.market_data_stream.market_data_stream(
                request_iterator()
        ):
            if marketdata.candle:
                # Данные — свечи
                async with lock:
                    candle = marketdata.candle
                    try:
                        shared_tasks[ticker]['high'] = float(quotation_to_decimal(candle.high))
                        shared_tasks[ticker]['low'] = float(quotation_to_decimal(candle.low))
                        shared_tasks[ticker]['volume'] = int(candle.volume)
                        shared_tasks[ticker]['time_received'] = candle.last_trade_ts
                    except ValueError:
                        pass

                    if not shared_tasks[ticker]['depends']:
                        # Все задачи с ticker завершены - завершаем опрос данного ticker
                        shared_tasks.pop(ticker, None)
                        break

                await asyncio.sleep(3)
