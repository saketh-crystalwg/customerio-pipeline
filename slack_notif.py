import requests
import json
import os
import sys

def send_notification(title, message, color):
    url = "https://hooks.slack.com/services/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"
    if not url:
        return  # silently skip if not configured

    slack_data = {
        "username": "NotificationBot",
        "icon_emoji": ":rocket:",
        "attachments": [
            {
                "color": color,
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": False,
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
