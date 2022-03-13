import faust


class MarvelMovie(faust.Record, validation=True):
    Title: str
    distributor: str
    Budget: float


class MarvelRate(faust.Record, validation=True):
    Film: str
    Metacritic: float