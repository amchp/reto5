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

    def mapper_get_prices_and_dates_per_company(self, _, line : str): 
        columns : list[str] = ['company', 'price', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['company'], (int(data_row['price']), data_row['date']))

    def reducer_get_dates_with_min_price(self, company : str, price_dates : tuple[tuple[int, str]]):
        list_price_dates = list(price_dates)
        list_price_dates.sort()
        min_price = price_dates[0][0]
        for price, date in list_price_dates:
            if min_price < price:
                break
            yield (date, 1)

    def reducer_sum_dates_with_min_price(self, date: str, companies: tuple[int]):
        yield (None, (sum(companies), date))

    def reducer_get_date_with_most_min_price(self, _, sum_dates: tuple[int, str]):
        yield max(sum_dates)

if __name__ == '__main__':
    MRBlackDate.run()


