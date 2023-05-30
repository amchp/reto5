from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRBestAndWorstRatedMovieByGenre(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings_and_movie_per_genre,
                   reducer=self.reducer_get_max_and_min_mean_ratings_per_genre)
        ]

    def mapper_get_ratings_and_movie_per_genre(self, _, line : str): 
        columns : list[str] = ['user', 'movie', 'rating', 'genre', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['genre'], (int(data_row['rating']), data_row['movie']))

    def reducer_get_max_and_min_mean_ratings_per_genre(self, genre : str, ratings_and_movie : Generator[tuple[tuple[int, str]], None, None]):
        movies_sum_ratings : dict[str, list[int]] = {}
        num_ratings : dict[str, int] = {}
        for rating, movie in ratings_and_movie:
            if movie in movies_sum_ratings:
                movies_sum_ratings[movie] += rating
                num_ratings[movie] += 1
            else:
                movies_sum_ratings[movie] = rating
                num_ratings[movie] = 1
        max_rating = 0
        min_rating = float('inf')
        for movie in movies_sum_ratings.keys():
            mean_rating = movies_sum_ratings[movie] / num_ratings[movie]
            max_rating = max(mean_rating, max_rating)
            min_rating = min(mean_rating, min_rating)
        yield (genre, (min_rating, max_rating))


if __name__ == '__main__':
    MRBestAndWorstRatedMovieByGenre.run()

