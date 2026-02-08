import asyncio
from decouple import config
from t_tech.invest import Client, InstrumentIdType

async def get_figi_instrument(ticker, class_code):
    with Client(config('T_TOKEN')) as client:
        try:
            instrument_response = client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                id=ticker,
                class_code=class_code,
            )
        except:
            return -1, None, f"ERROR: get_figi_instrument(): get_instrument_by({ticker}, {class_code})"
        else:
            return 0, instrument_response.instrument.figi, None
