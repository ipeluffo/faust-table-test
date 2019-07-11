from app import app, events_topic
from random import randint
from faust.cli import option

@app.command(option('--max-events', type=int, required=True))
async def send_events(self, max_events:int):
    print(f'Sending {max_events} events...')
    half_events = max_events // 2
    for i in range(max_events):
        await events_topic.send(key=str(randint(1, 1500000)), value=randint(0, 1000))
        if i == half_events:
            print(f'Half amount of events sent: {i}')
