from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostViewedDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies_per_date,
                   reducer=self.reducer_add_movies_per_date),
            MRStep(reducer=self.reducer_get_most_viewed_date)
        ]

    def mapper_movies_per_date(self, _, line : str): 
        columns : list[str] = ['user', 'movie', 'rating', 'genre', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['date'], 1)

    def reducer_add_movies_per_date(self, dia : str, movies : tuple[int]):
        yield (None, (sum(movies), dia))
        
    def reducer_get_most_viewed_date(self, _, movies_date : tuple[tuple[int, str]]):
        yield max(movies_date)


if __name__ == '__main__':
    MRMostViewedDate.run()

