import asyncio
import aiohttp
from faststream.kafka import KafkaBroker

URLS = [
    "https://api.coinbase.com/v2/prices/BTC-USD/spot",
    "https://api.coinbase.com/v2/prices/ETH-USD/spot",
]

async def fetch_price(session, url):
    async with session.get(url) as response:
        data = await response.json()
        return {
            "crypto_currency": data["data"]["base"],
            "price": float(data["data"]["amount"]),
        }

async def publish_prices(broker: KafkaBroker):
    async with aiohttp.ClientSession() as session:
        while True:
            for url in URLS:
                try:
                    msg = await fetch_price(session, url)
                    await broker.publish(
                        msg,
                        topic="new_crypto_price",
                        key=msg["crypto_currency"].encode("utf-8")  # partition key
                    )
                    print(f"✅ Sent: {msg}")
                except Exception as e:
                    print(f"❌ Error fetching or publishing: {e}")
            await asyncio.sleep(2)