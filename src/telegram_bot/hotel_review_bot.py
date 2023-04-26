#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Simple Telegram bot that receives hotel reviews from users
and sends them to Apache Kafka
"""

import os
import socket
import telebot
from confluent_kafka import Producer

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

bot = telebot.TeleBot(API_TOKEN)

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
producer = Producer(conf)
topic = "hotel-reviews"

# Handle '/start' and '/help'
@bot.message_handler(commands=['help', 'start'])
def send_welcome(message):
    bot.reply_to(message, """\
Hello there, I am Hotel review bot.
Submit your hotel review here!\
""")


# Handle all other messages with content_type 'text' (content_types defaults to ['text'])
@bot.message_handler(func=lambda message: True)
def echo_message(message):
    producer.produce(topic, value='{"review": "' + message.text +' "}')
    producer.flush()
    bot.reply_to(message, "Thank you for the review!")


bot.infinity_polling()