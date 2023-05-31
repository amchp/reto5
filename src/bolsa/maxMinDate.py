from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMaxMinDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices_per_company,
                   reducer=self.reducer_get_min_max_price),
        ]

    def mapper_get_prices_per_company(self, _, line): 
        columns = ['company', 'price', 'date']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['company'], float(data_row['price']))

    def reducer_get_min_max_price(self, company, prices):
        list_prices = list(prices)
        yield (company, (min(list_prices),max(list_prices)))

if __name__ == '__main__':
    MRMaxMinDate.run()

