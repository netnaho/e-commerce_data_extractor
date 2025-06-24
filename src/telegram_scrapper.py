import asyncio
import csv
import re
import unicodedata
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# Selected Telegram channels
channels = ['ZemenExpress', 'nevacomputer', 'ethio_brand_collection', 'Leyueqa', 'MerttEka']

# Initialize Telegram client
client = TelegramClient('session_name', api_id, api_hash)

# Normalize Amharic text (remove extra spaces, normalize fidel characters)
def normalize_amharic_text(text):
    if not text or not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFC', text)
    text = re.sub(r'[^\u1200-\u137F\s0-9a-zA-Z.,@]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Preprocess message data
def preprocess_message(message):
    data = {
        'channel': message.chat.username if message.chat and hasattr(message.chat, 'username') else '',
        'sender': message.sender.username if message.sender and hasattr(message.sender, 'username') else '',
        'timestamp': message.date.isoformat(),
        'text': normalize_amharic_text(message.text) if message.text else '',
        'media_type': '',
        'media_path': ''
    }
    if message.media:
        if hasattr(message.media, 'photo'):
            data['media_type'] = 'image'
            data['media_path'] = f"media/photos/{message.id}.jpg"
        elif hasattr(message.media, 'document'):
            ext = message.media.document.mime_type.split('/')[-1]
            data['media_type'] = 'document'
            data['media_path'] = f"media/documents/{message.id}.{ext}"
    return data

# Save data to CSV file in ./data/ folder
def save_to_csv(data, filename='data/preprocessed_data.csv'):
    headers = ['channel', 'sender', 'timestamp', 'text', 'media_type', 'media_path']
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_exists = os.path.exists(filename)
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

# Async main
async def main():
    await client.start(phone)
    print("Client connected successfully")

    for channel in channels:
        try:
            entity = await client.get_entity(channel)
            async for message in client.iter_messages(entity, limit=100):
                if message.text or message.media:
                    processed_data = preprocess_message(message)
                    save_to_csv(processed_data)
                    print(f"Processed message from {channel}: {processed_data['text'][:50]}...")
        except Exception as e:
            print(f"Error processing channel {channel}: {str(e)}")

    print("Data ingestion and preprocessing complete")

# Run
if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())