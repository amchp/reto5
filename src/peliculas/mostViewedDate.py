from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostViewedDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies_per_date,
                   reducer=self.reducer_add_movies_per_date),
            MRStep(reducer=self.reducer_get_most_viewed_date)
        ]

    def mapper_movies_per_date(self, _, line): 
        columns = ['user', 'movie', 'rating', 'genre', 'date']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['date'], 1)

    def reducer_add_movies_per_date(self, dia, movies):
        yield (None, (sum(movies), dia))
        
    def reducer_get_most_viewed_date(self, _, movies_date):
        list_movies_date = list(movies_date)
        most_viewed = max(list_movies_date)[0]
        for views, date in list_movies_date:
            if views == most_viewed:
                yield (date, views)


if __name__ == '__main__':
    MRMostViewedDate.run()

