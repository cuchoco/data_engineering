import faust
from taxi_rides import TaxiRide
from faust import current_event

app = faust.App('App.stream.v2', broker='kafka://localhost:9092', consumer_auto_offset_reset='earliest')
topic = app.topic('yellow_taxi_ride.json', value_type=TaxiRide)

high_amount_rides = app.topic('yellow_taxi_ride.high_amount')
low_amount_rides = app.topic('yellow_taxi_ride.low_amount')

@app.agent(topic)
async def process(stream):
    async for event in stream:
        if event.total_amount >= 40.0:
            await current_event().forward(high_amount_rides)
        else:
            await current_event().forward(low_amount_rides)

if __name__ == '__main__':
    app.main()