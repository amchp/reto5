from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRLeastViewedDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies_per_date,
                   reducer=self.reducer_add_movies_per_date),
            MRStep(reducer=self.reducer_get_least_viewed_dates)
        ]

    def mapper_get_movies_per_date(self, _, line : str): 
        columns : list[str] = ['user', 'movie', 'rating', 'genre', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['date'], 1)

    def reducer_add_movies_per_date(self, dia : str, movies : Generator[tuple[int], None, None]):
        yield (None, (sum(movies), dia))
        
    def reducer_get_least_viewed_dates(self, _, movies_date : Generator[tuple[tuple[int, str]], None, None]):
        list_movies_date = list(movies_date)
        least_viewed = min(list_movies_date)[0]
        for views, date in list_movies_date:
            if views == least_viewed:
                yield (date, views)


if __name__ == '__main__':
    MRLeastViewedDate.run()

