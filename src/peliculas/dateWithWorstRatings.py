from mrjob.job import MRJob
from mrjob.step import MRStep

class MRDateWorstRatings(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings_per_date,
                   reducer=self.reducer_mean_ratings_and_date),
            MRStep(reducer=self.reducer_get_worst_rated_date)
        ]

    def mapper_get_ratings_per_date(self, _, line : str): 
        columns : list[str] = ['user', 'movie', 'rating', 'genre', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['date'], int(data_row['rating']))

    def reducer_mean_ratings_and_date(self, dia : str, ratings : tuple[int]):
        yield (None, (sum(ratings) / len(ratings), dia))
        
    def reducer_get_worst_rated_date(self, _, ratings_date : tuple[tuple[int, str]]):
        yield min(ratings_date)


if __name__ == '__main__':
    MRDateWorstRatings.run()

