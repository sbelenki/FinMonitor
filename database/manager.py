import sqlite3
import pandas as pd
from pandas_datareader import data as web
from time import strftime
from datetime import datetime
from datetime import timedelta
from datetime import date
import logging

class Manager:
    conn = None
    max_sec_id = 0

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def open(self, dbname):
        self.conn = sqlite3.connect(dbname)
        cur = self.conn.cursor()
        cur.execute("SELECT IFNULL(MAX(sec_id), 0) FROM fm_symbol")
        self.max_sec_id = cur.fetchone()[0]
        cur.close()

    def close(self):
        self.conn.close()

    def add_symbol_yahoo(self, market, symbol, yahoo_symbol, full_name, start_date = None):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fm_symbol WHERE yahoo_symbol = '" + yahoo_symbol + "'")
        if cur.fetchone()[0] == 0:
            cur2 = self.conn.cursor()
            cur2.execute("INSERT INTO fm_symbol (sec_id, mkt_code, symbol, yahoo_symbol, symbol_full_name) VALUES ('" + \
            str(self.max_sec_id + 1) + "', '" + market + "', '" + symbol + "', '" + yahoo_symbol + "', '" + full_name + "')")
            if start_date is None:
                start_date = datetime(2000, 1, 1)
            data = web.DataReader(yahoo_symbol, "yahoo", start=start_date)
            data["sec_id"] = self.max_sec_id + 1
            data.to_sql("fm_hist_trade", self.conn, if_exists="append")
            self.conn.commit()
            cur2.close()
            self.max_sec_id += 1
            self.logger.info("DBM: " + yahoo_symbol + " symbol added to the database")
        else:
            self.logger.warning("DBM: " + yahoo_symbol + " symbol already exists in the database")
        cur.close()

    def upd_symbol_yahoo(self, yahoo_symbol):
        cur = self.conn.cursor()
        cur.execute("SELECT sec_id FROM fm_symbol WHERE yahoo_symbol = '" + yahoo_symbol + "'")
        sec_id = cur.fetchone()
        cur.close()
        if sec_id:
            cur2 = self.conn.cursor()
            cur2.execute("SELECT date(MAX(date)) FROM fm_hist_trade WHERE sec_id = " + str(sec_id[0]))
            date_val = cur2.fetchone()
            cur2.close()
            if date_val:
                year, month, day = [int(x) for x in date_val[0].split('-') if x]
                self.logger.debug("DBM: " + yahoo_symbol + " symbol historical date " + date_val[0])
                start_date = datetime(year, month, day) + timedelta(days=1)
                if start_date < datetime.now():
                    try:
                        data = web.DataReader(yahoo_symbol, "yahoo", start=start_date)
                        data["sec_id"] = sec_id[0]
                        data.to_sql("fm_hist_trade", self.conn, if_exists="append")
                        self.conn.commit()
                        self.logger.info("DBM: " + yahoo_symbol + " symbol updated in the database")
                    except IOError:
                        self.logger.error("DBM: " + yahoo_symbol + " symbol update IOException", exc_info=True)
                else:
                    self.logger.info("DBM: " + yahoo_symbol + " symbol already up to date in the database")
            else:
                self.logger.error("DBM: " + yahoo_symbol + " symbol start date for update not found")
        else:
            self.logger.warning("DBM: " + yahoo_symbol + " symbol to update doesn't exist")

    def del_symbol_yahoo(self, yahoo_symbol):
        cur = self.conn.cursor()
        cur.execute("SELECT sec_id FROM fm_symbol WHERE yahoo_symbol = '" + yahoo_symbol + "'")
        ret_val = cur.fetchone()
        if ret_val:
            cur2 = self.conn.cursor()
            cur2.execute("DELETE FROM fm_hist_trade WHERE sec_id = '" + \
                str(ret_val[0]) + "'")
            cur2.execute("DELETE from fm_symbol WHERE sec_id = '" + \
                str(ret_val[0]) + "'")
            self.conn.commit()
            cur2.execute("SELECT IFNULL(MAX(sec_id), 0) FROM fm_symbol")
            self.max_sec_id = cur2.fetchone()[0]
            cur2.close()
            self.conn.execute("VACUUM")
            self.logger.info("DBM: " + yahoo_symbol + " symbol deleted from the database")
        else:
            self.logger.warning("DBM: " + yahoo_symbol + " symbol to delete doesn't exist")
        cur.close()

    def upd_all(self, source = "yahoo"):
        if source == "yahoo":
            cur = self.conn.cursor()
            cur.execute('SELECT sec_id, yahoo_symbol FROM fm_symbol ORDER BY sec_id')
            rows = cur.fetchall()
            cur.close()
            for row in rows:
                self.upd_symbol_yahoo(row[1])
            for row in rows:
                self.upd_stats(str(row[0]))
            self.conn.execute("VACUUM")
        else:
            self.logger.error("DBM: " + source + " source for update not implemented")

    def del_all(self):
        cur = self.conn.cursor()
        cur.execute('DELETE FROM fm_stat')
        cur.execute('DELETE FROM fm_hist_trade')
        cur.execute('DELETE FROM fm_symbol')
        self.conn.commit()
        cur.close()
        self.conn.execute("VACUUM")
        self.logger.info("DBM: all symbols deleted from the database")

    def upd_stats(self, sec_id):
        df = pd.read_sql("SELECT sec_id, Date, Close " \
        "FROM fm_hist_trade WHERE sec_id = " + sec_id + " ORDER BY Date", self.conn)
        if not df.empty:
            df["Std"] = df['Close'].rolling(window=251).std()
            df["Mean"] = df['Close'].rolling(window=251).mean()
            del df["Close"]
            cur = self.conn.cursor()
            cur.execute('DELETE from fm_stat WHERE sec_id = ' + str(df['sec_id'].iloc[0]))
            df.to_sql("fm_stat", self.conn, if_exists="append", index=False)
            self.conn.commit()
        else:
            self.logger.warning("DBM: " + mkt_code + ":" + symbol + " symbol for stats doesn't exist")

