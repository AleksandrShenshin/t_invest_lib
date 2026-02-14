import asyncio
from decouple import config
from t_tech.invest import Client, InstrumentIdType
from t_tech.invest.exceptions import RequestError
from t_tech.invest.utils import quotation_to_decimal

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
