import sys
from time import strftime
from notifier import FCM

if __name__ == "__main__":
    fcm = FCM()
    fcm.send("/topics/news", "test message at " + strftime("%Y-%m-%d %H:%M:%S"))
