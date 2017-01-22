import sys
import ConfigParser
from pattern.web import URL, extension, cache, plaintext, URLTimeout, URLError, HTTP404NotFound, HTTP400BadRequest
import simplejson as json
import ast
import pandas as pd
from random import randint
from time import sleep, strftime
import logging
from notifier import FCM

error_flag = 0
logger = logging.getLogger(__name__)

# Config Definitions
CFG_EXCHANGE          = 'Exchange'
CFG_SYMBOL            = 'Symbol'
CFG_NOTIF_STOP_UP     = 'NotifStopUp'
CFG_NOTIF_STOP_DOWN   = 'NotifStopDown'
CFG_NOTIF_SIGMA_DAILY = 'NotifSigmaDaily'
CFG_NOTIF_SIGMA       = 'NotifSigma'

def check_config(cfg_file):
    config = ConfigParser.ConfigParser()
    config.optionxform = str # make options case-aware
    if len(config.read(cfg_file)) == 0:
        raise ValueError, "Failed to open config files: " + cfg_file
    logger.info("Loaded checks:\n" + "\n".join(config.sections()))
    return config

def build_url_list(config):
    pref = "http://finance.google.com/finance/info?client=ig&q="
    urls = []
    one_url = pref # TODO: implement url splitting for request more than 10
    count = 0
    for step in config.sections():
        if count > 0:
            one_url += ","
        one_url += config.get(step, CFG_EXCHANGE) + "%3a" + config.get(step, CFG_SYMBOL)
        count += 1
    urls.append(one_url)
    return urls

# Google Finance Definitions
GF_CHANGE          = 'c'        # Change (normal trading session)
GF_FIX             = 'c_fix'    # Change (normal trading session)
GF_CCOL            = 'ccol'     # ??
GF_CHANGE_PC       = 'cp'       # Change (percent)
GF_CHANGE_PC_FIX   = 'cp_fix'   # ??
GF_DIVIS           = 'div'      # Dividend
GF_EXCHANGE        = 'e'        # Exchange
GF_EXT_TRADE       = 'el'       # Ext Hrs Last Trade Price
GF_EXT_TRADE_CUR   = 'el_cur'   # Ext Hrs Last Trade with currency
GF_EXT_TRADE_TIME  = 'elt'      # Ext Hrs Last Trade DateTime Long
GF_EXT_CHANGE      = 'ec'       # Ext Hrs Change
GF_EXT_CHANGE_PC   = 'ecp'      # Ext Hrs Change Percent
GF_ID              = 'id'       # Google ID
GF_LAST            = 'l'        # Last (not one)
GF_LAST_CUR        = 'l_cur'    # Last with currency
GF_LAST_FIX        = 'l_fix'    # last?
GF_LAST_TRADE_TIME = 'ltt'      # Last trade timestamp
GF_LAST_TRADE_DTS  = 'lt_dts'   # Last trade DTS time
GF_LAST_TRADE_DT   = 'lt'       # Last trade date/time
GF_PREV_CLOSE      = 'pcls_fix' # Previous close
GF_SIZE            = 's'        # LastTradeSize
GF_SYMBOL          = 't'        # Symbol
GF_YIELD           = 'yld'      # Yield

def fetch_data(in_url, webdump = None):
    url = URL(in_url)
    for i in range(4):
        try:
            url_data = url.download(timeout = 5, cached=False)
            try:
                new_dict = ast.literal_eval(url_data.replace("/", "").strip()) # list of dictionaries - new_dict[0] etc
                json_raw_data = json.loads(json.dumps(new_dict)) # json_raw_data now contains proper json with ""
                if webdump != None: dump_to_file(webdump, in_url, url_data)
                return pd.Series(json_raw_data)
            except SyntaxError:
                logger.warning("SyntaxError, retrying again")
                sleep(randint(1,5))
            except Exception, e:
                logger.error("Exception {0}: ".format(e))
                break;
        except URLTimeout:
            logger.warning("Timeout, retrying again")
            sleep(randint(1,5))
        except (URLError, HTTP404NotFound, HTTP400BadRequest):
            logger.error("Website not found")
            break;
    return pd.Series() # return empty series

def dump_to_file(webdump, url, data):
    f = open(str(webdump),'a')
    f.write('==============================================\n')
    f.write("URL: " + str(url) + "\n");
    f.write("DATA:\n")
    f.write(str(data) + "\n")
    f.write('==============================================\n')
    f.close()

def check_item(config, item):
    for step in config.sections():
        if ((item[GF_EXCHANGE] == config.get(step, CFG_EXCHANGE)) and
                (item[GF_SYMBOL] == config.get(step, CFG_SYMBOL))):
            logger.info("==> Config item for [" + step + "] " + item[GF_EXCHANGE]
                  + ":" + item[GF_SYMBOL] + " current: " + item[GF_LAST_CUR])
            for name in config.options(step):
                if name == CFG_NOTIF_STOP_UP:
                    check_notif_up(config.get(step, CFG_NOTIF_STOP_UP), item)
                if name == CFG_NOTIF_STOP_DOWN:
                    check_notif_down(config.get(step, CFG_NOTIF_STOP_DOWN), item)
                if name == CFG_NOTIF_SIGMA_DAILY:
                    check_notif_sigma_daily(config.get(step, CFG_NOTIF_SIGMA_DAILY), item)
                if name == CFG_NOTIF_SIGMA:
                    check_notif_sigma(config.get(step, CFG_NOTIF_SIGMA), item)

def check_notif_up(stop_up, item):
    if stop_up == "":
        return
    logger.info("Checking stop up: " + str(stop_up))
    if float(item[GF_LAST]) >= float(stop_up):
        notify("Stop up reached", stop_up, item[GF_LAST], item)

def check_notif_down(stop_down, item):
    if stop_down == "":
        return
    logger.info("Checking stop down: " + str(stop_down))
    if float(item[GF_LAST]) <= float(stop_down):
        notify("Stop down reached", stop_down, item[GF_LAST], item)

def check_notif_sigma_daily(sigma_daily, item):
    if sigma_daily == "":
        return
    logger.info("Checking sigma daily: " + str(sigma_daily))

def check_notif_sigma(sigma, item):
    if sigma == "":
        return
    logger.info("Checking sigma historical: " + str(sigma))

def notify(msg, limit, val, item):
    fcm = FCM(logger)
    fcm.send("/topics/news", "" + item[GF_EXCHANGE] + ":" + item[GF_SYMBOL] +
             " " + msg + " limit: " + limit + " actual: " + val);

def init_log():
    formatter = logging.Formatter('[%(asctime)s/%(levelname)s][%(filename)s:%(funcName)s:%(lineno)s]: %(message)s')
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

if __name__ == "__main__":
    webdump = None
    init_log()
    logger.info("Started to run")

    if len(sys.argv) < 2:
        logger.error('No list file provided\n')
        sys.exit(1)
    else:
        logger.info('Running with the list file ' + sys.argv[1])
        if (len(sys.argv)) > 2:
            logger.info('Running with the web dump file ' + sys.argv[2])
            webdump = sys.argv[2]
            f = open(str(webdump),'w') # delete the old webdump
            f.close()

    try:
        cfg = check_config(sys.argv[1])
        urls = build_url_list(cfg)
        print(urls)
        for url in urls:
            series = fetch_data(url, webdump)
            if len(series) > 0:
                for item in series:
                    check_item(cfg, item)
            else: logger.error("No data loaded for " + url)
    except Exception, e:
        logger.error("Exception encountered {0}: ".format(e))
        sys.exit(1)

    logger.info("Finished to run")
