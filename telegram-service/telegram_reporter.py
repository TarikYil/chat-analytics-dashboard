import json
import logging
import threading
import time
from collections import deque
from typing import Dict, Any, List
from kafka import KafkaConsumer
from telegram import Bot
import os
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram-reporter")

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPICS = ['raw-comments', 'sentiment_analytics', 'category_analytics', 'realtime_analytics']

bot = Bot(token="8140408145:AAEw60OyXf6YNJcEnozHn10VtOt_JSA1Ns0")

def get_dashboard_summary():
    try:
        redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
        # Latest comments
        last_positive = redis_client.lindex("last_positive_comments", 0) or "No comments"
        last_negative = redis_client.lindex("last_negative_comments", 0) or "No comments"
        last_neutral = redis_client.lindex("last_neutral_comments", 0) or "No comments"
        # Counts and ratios
        positive_count = int(redis_client.get('positive_comments') or 0)
        negative_count = int(redis_client.get('negative_comments') or 0)
        neutral_count = int(redis_client.get('neutral_comments') or 0)
        total_comments = int(redis_client.get('total_comments') or 0)
        positive_ratio = float(redis_client.get('positive_ratio') or 0)
        negative_ratio = float(redis_client.get('negative_ratio') or 0)
        neutral_ratio = float(redis_client.get('neutral_ratio') or 0)
        comments_per_minute = float(redis_client.get('comments_per_minute') or 0)
        # Message format
        msg = (
            "📝 *Latest Comments*\n\n"
            f"*Positive:*\n{last_positive}\n\n"
            f"*Negative:*\n{last_negative}\n\n"
            f"*Neutral:*\n{last_neutral}\n\n"
            "✅ *Real-time sentiment data from Redis*\n\n"
            f"*Positive*: {positive_ratio:.1f}% ({positive_count} comments)\n"
            f"*Negative*: {negative_ratio:.1f}% ({negative_count} comments)\n"
            f"*Neutral*: {neutral_ratio:.1f}% ({neutral_count} comments)\n"
            f"*Total*: {total_comments}\n"
            f"*{comments_per_minute:.1f}/min*"
        )
        return msg
    except Exception as e:
        return f"Could not retrieve Redis data: {e}"

class TelegramReporter:
    def __init__(self, interval: int = 300):
        self.interval = interval
        self.thread = threading.Thread(target=self._report_loop, daemon=True)
        self.is_running = False

    def start(self):
        self.is_running = True
        self.thread.start()
        logger.info("Telegram dashboard reporting started")

    def stop(self):
        self.is_running = False
        self.thread.join(timeout=5)
        logger.info("Telegram dashboard reporting stopped")

    def _report_loop(self):
        while self.is_running:
            time.sleep(self.interval)
            dashboard_summary = get_dashboard_summary()
            try:
                bot.send_message(chat_id="7933901900", text=dashboard_summary, parse_mode="Markdown")
                logger.info("Dashboard summary sent to Telegram.")
            except Exception as e:
                logger.error(f"Could not send message to Telegram: {e}")

if __name__ == "__main__":
    reporter = TelegramReporter(interval=300)  # 5 minutes
    reporter.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        reporter.stop() 