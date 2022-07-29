import faust
from taxi_rides import TaxiRide


app = faust.App('App.stream.v1', broker="kafka://localhost:9092")
topic = app.topic('yellow_taxi_ride.json', value_type=TaxiRide)

vendor_rides = app.Table('vendor_rides', default=int)

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.vendorId):
        vendor_rides[event.vendorId] += 1

# vendor id 1,2 가 각각 몇개인지 더해서 보여줌
if __name__=="__main__":
    app.main()
