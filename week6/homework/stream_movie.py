import faust
from marvel_movie import MarvelMovie


app = faust.App('stream.homework.week6', broker='kafka://localhost:9092')
MarvelMovieTopic = app.topic('homework.marvelmovie.json', value_type=MarvelMovie)


@app.agent(MarvelMovieTopic)
async def start_reading(records):
    async for value in (records):
        print(value)
        yield value


if __name__ == '__main__':
    app.main()