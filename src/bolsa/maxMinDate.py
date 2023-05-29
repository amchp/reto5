from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMaxMinDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices_per_company,
                   reducer=self.reducer_get_min_max_price),
        ]

    def mapper_get_prices_per_company(self, _, line : str): 
        columns : list[str] = ['company', 'price', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['company'], int(data_row['price']))

    def reducer_get_min_max_price(self, company : str, prices : tuple[int]):
        yield (company, (min(prices),max(prices)))

if __name__ == '__main__':
    MRMaxMinDate.run()

