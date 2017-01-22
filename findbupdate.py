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
    dbm.upd_all()
    dbm.close()

