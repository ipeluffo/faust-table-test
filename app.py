from faust import app

app = app.App(
    id='faust-table-test',
    broker='kafka://localhost:9092',
    # autodiscover=True,
    store='rocksdb://',
)

events_topic = app.topic('events_topic', internal=True, key_type=str)

events_table = app.Table(name='events_table', default=int)

@app.agent(events_topic)
async def events_agent(stream):
    async for key, value in stream.items():
        print(f'Processing [{key},{value}]')
        events_table[key] = value


@app.page('/testing')
async def testing_view(web , request):
    import ipdb; ipdb.set_trace()
    return web.text('testing')


@app.page('/value/{key}')
@app.table_route(table=events_table, match_info='key')
async def key_value(web, request, key: str):
    return web.json({
        'key': key,
        'value': events_table[key],
    })
