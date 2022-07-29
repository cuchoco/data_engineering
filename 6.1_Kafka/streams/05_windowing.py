from datetime import timedelta
import faust
from taxi_rides import TaxiRide


app = faust.App('App.stream.v1', broker="kafka://localhost:9092")
topic = app.topic('yellow_taxi_ride.json', value_type=TaxiRide)


vendor_rides = app.Table('vendor_rides', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1)
)

# windowing 1 minute > 1분이 지나면 새로 집계됨
@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.vendorId):
        vendor_rides[event.vendorId] += 1


if __name__=="__main__":
    app.main()