import faust
from marvel_movie import MarvelMovie
from marvel_movie import MarvelRate


app = faust.App('stream.rate.week6', broker='kafka://localhost:9092')
MarvelMovieTopic = app.topic('homework.marvelmovie.json', value_type=MarvelMovie)
MarvelRateTopic = app.topic('homework.marvelrate.json', value_type=MarvelRate)



@app.agent(MarvelRateTopic)
async def start_reading(records):
    async for value in (records):
        print(value)
        yield value


@app.agent(MarvelMovieTopic)
async def start_reading(records):
    async for value in (records):
        print(value)
        yield value


if __name__ == '__main__':
    app.main()