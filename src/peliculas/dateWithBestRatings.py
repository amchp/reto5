from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRDateBestRatings(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings_per_date,
                   reducer=self.reducer_mean_ratings_and_date),
            MRStep(reducer=self.reducer_get_best_rated_dates)
        ]

    def mapper_get_ratings_per_date(self, _, line): 
        columns = ['user', 'movie', 'rating', 'genre', 'date']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['date'], int(data_row['rating']))

    def reducer_mean_ratings_and_date(self, dia, ratings):
        list_ratings = list(ratings)
        yield (None, (sum(list_ratings) / len(list_ratings), dia))
        
    def reducer_get_best_rated_dates(self, _, ratings_date):
        list_ratings_date = list(ratings_date)
        max_rating = max(list_ratings_date)[0]
        for rating, date in list_ratings_date:
            if rating == max_rating:
                yield (date, rating)


if __name__ == '__main__':
    MRDateBestRatings.run()

