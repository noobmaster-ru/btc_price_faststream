from faststream import FastStream
from crypto_producer import publish_prices
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

@app.after_startup
async def start_producer():
    await publish_prices(broker)
    