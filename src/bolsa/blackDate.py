from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRBlackDate(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices_and_dates_per_company,
                   reducer=self.reducer_get_dates_with_min_price),
            MRStep(reducer=self.reducer_sum_dates_with_min_price),
            MRStep(reducer=self.reducer_get_date_with_most_min_price)
        ]

    def mapper_get_prices_and_dates_per_company(self, _, line): 
        columns = ['company', 'price', 'date']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['company'], (float(data_row['price']), data_row['date']))

    def reducer_get_dates_with_min_price(self, company, price_dates):
        list_price_dates = list(price_dates)
        min_price = min(list_price_dates)[0]
        for price, date in list_price_dates:
            if min_price == price:
                yield (date, 1)

    def reducer_sum_dates_with_min_price(self, date, companies):
        yield (None, (sum(companies), date))

    def reducer_get_date_with_most_min_price(self, _, sum_dates):
        list_sum_dates = list(sum_dates)
        min_stocks = min(list_sum_dates)[0]
        for num_stocks, date in list_sum_dates:
            if num_stocks == min_stocks:
                yield (date, num_stocks)

if __name__ == '__main__':
    MRBlackDate.run()


