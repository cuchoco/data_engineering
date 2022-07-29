import faust
from taxi_rides import TaxiRide


app = faust.App('App.stream.v1', broker="kafka://localhost:9092")
topic = app.topic('yellow_taxi_ride.json', value_type=TaxiRide)



@app.agent(topic)
async def start_reading(records):
    async for record in records:
        print(record)

# python stream.py worker
if __name__=="__main__":
    app.main()
