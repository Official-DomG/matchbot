import os
import requests
from datetime import datetime
import pytz

LONDON = pytz.timezone("Europe/London")

def send_telegram_message(text: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("Telegram not configured")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True
    }

    r = requests.post(url, json=payload, timeout=20)
    print("Telegram response:", r.status_code, r.text)

def main():
    now = datetime.now(LONDON)
    message = (
        "MatchBot sanity check ✅\n\n"
        f"Time (London): {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "Pipeline is alive."
    )

    print("MatchBot starting:", now)
    send_telegram_message(message)

if __name__ == "__main__":
    main()
