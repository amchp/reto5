from mrjob.job import MRJob
from mrjob.step import MRStep

class MRStockGrowOrStable(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_dates_and_prices_per_company,
                   reducer=self.reducer_get_stable_companies),
        ]

    def mapper_get_dates_and_prices_per_company(self, _, line: str): 
        columns : list[str] = ['company', 'price', 'date']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['company'], (data_row['date'], int(data_row['price'])))

    def reducer_get_stable_companies(self, company: str, date_prices: tuple[tuple[str,int]]): 
        list_date_prices : list[tuple[str,int]] = list(date_prices)
        list_date_prices.sort()
        cur_price : int = -1
        for _, price in list_date_prices:
            if price < cur_price:
                return 
            cur_price = price 
        yield (company, 1)

if __name__ == '__main__':
    MRStockGrowOrStable.run()


