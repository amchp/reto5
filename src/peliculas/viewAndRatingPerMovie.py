from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRViewAndRatingPerMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_views_and_ratings_per_movie,
                   reducer=self.reducer_add_views_and_mean_ratings)
        ]

    def mapper_get_views_and_ratings_per_movie(self, _, line): 
        columns = ['user', 'movie', 'rating', 'genre', 'date']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['movie'], (1, int(data_row['rating'])))

    def reducer_add_views_and_mean_ratings(self, movie, views_and_ratings):
        list_views_and_ratings = list(views_and_ratings)
        views = 0
        ratings = 0
        for view, rating in list_views_and_ratings:
            views += view
            ratings +=  rating
        ratings /= len(list_views_and_ratings)
        yield (movie, (views, ratings))


if __name__ == '__main__':
    MRViewAndRatingPerMovie.run()

