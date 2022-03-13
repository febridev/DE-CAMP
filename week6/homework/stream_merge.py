import faust
from marvel_movie import MarvelMovie
from marvel_movie import MarvelRate


app = faust.App('stream.homework.week6', broker='kafka://localhost:9092')
# MarvelMovieTopic = app.stream('homework.marvelmovie.json', value_type=MarvelMovie)
# MarvelRateTopic = app.stream('homework.marvelrate.json', value_type=MarvelRate)

MarvelMovieTopic = app.stream('homework.marvelmovie.json')
MarvelRateTopic = app.stream('homework.marvelrate.json')


@app.agent(app.stream('MarvelMovieTopic', 'MarvelRateTopic'))
async def topic_merger(stream1):
    async for value in (stream1 & MarvelRateTopic):
        print(value)
        yield value


if __name__ == '__main__':
    app.main()