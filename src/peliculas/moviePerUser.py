from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMoviePerUser(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices_and_movie_per_user,
                   reducer=self.reducer_get_movies_and_mean_rating),
        ]

    def mapper_get_prices_and_movie_per_user(self, _, line : str): 
        columns : list[str] = ['user', 'movie', 'rating', 'genre', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['user'], (data_row['movie'], int(data_row['rating'])))

    def reducer_get_movies_and_mean_rating(self, user : str, movie_ratings : Generator[tuple[tuple[str, int]], None, None]):
        list_movie_ratings = list(movie_ratings)
        movies : dict[str, bool] = {}
        ratings = 0
        for movie, rating in list_movie_ratings:
            movies[movie] = True
            ratings += rating
        ratings /= len(list_movie_ratings)
        yield (user, (len(movies), ratings))


if __name__ == '__main__':
    MRMoviePerUser.run()



