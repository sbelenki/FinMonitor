import pandas as pd
from datetime import datetime
from database import Manager as DBM
import logging

def init_log(logger):
    formatter = logging.Formatter('[%(asctime)s/%(levelname)s][%(filename)s:%(funcName)s:%(lineno)s]: %(message)s')
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    init_log(logger)
    dbm = DBM(logger)
    dbm.open("./FMDB.s3db")

    dbm.add_symbol_yahoo("NYSEARCA", "SPY", "SPY", "SPDR S&P 500 ETF Trust", datetime(1993, 1, 1))
    dbm.add_symbol_yahoo("TSE", "XIU", "XIU.TO", "iShares S&P/TSX 60 Index Fund", datetime(1999, 1, 1))
    dbm.add_symbol_yahoo("NYSEARCA", "GLD", "GLD", "SPDR Gold Trust", datetime(2004, 1, 1))

    #dbm.upd_symbol_yahoo("SPY")
    #dbm.add_symbol_yahoo("NASDAQ", "GOOG", "GOOG", 'Google', datetime(2016, 10, 1))
    #dbm.del_symbol_yahoo("MSFT", vacuum = True)
    #dbm.del_symbol_yahoo("SPY")
    #dbm.del_symbol_yahoo("XEG.TO")

    dbm.upd_all()
    #dbm.del_all()
    dbm.close()
