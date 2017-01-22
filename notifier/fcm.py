import requests
from time import strftime
import logging

class FCM:
    url = 'https://fcm.googleapis.com/fcm/send'
    headers = {'Authorization': 'key=<FCM_KEY>', 'Content-Type': 'application/json'}
    timeout = 5

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def send(self, topic, msg):
        self.logger.info("FCM send topic: " + topic)
        self.logger.info("FCM send msg: " + msg)
        data= "{\"to\":\"" + topic + "\",\"data\":{\"message\":\"" + msg + "\"}}"
        r = requests.post(self.url, data=data, headers=self.headers, timeout=self.timeout)
        self.logger.info("FCM response: " + str(r))
        self.logger.info("FCM reply: " + r.text)

