
import json
import sys
import requests
class SlackClient():
    def __init__(self, url):
        self.url = url
    def send_message(self, text, channel, title):
        message = (text)
        title = (title)

        slack_data = {
            "username": "NotificationBot",
            "icon_emoji": ":satellite:",
            # By default it is sent to #trading
            "channel" : channel,
            "attachments": [
                {
                    "color": "#9733EE",
                    "fields": [
                        {
                            "title": title,
                            "value": message,
                            "short": "false",
                        }
                    ]
                }
            ]
        }
        byte_length = str(sys.getsizeof(slack_data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        try:
            response = requests.post(self.url, data=json.dumps(slack_data), headers=headers)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)
        except:
            print("Could not send anything to slack")