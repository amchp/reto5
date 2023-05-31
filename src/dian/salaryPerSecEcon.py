from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSalaryPerSecEcon(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salaries_per_sec_econ,
                   reducer=self.reducer_sum_salaries),
        ]

    def mapper_get_salaries_per_sec_econ(self, _, line): 
        columns : list[str] = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['sec_econ'], float(data_row['salary']))

    def reducer_sum_salaries(self, sec_econ, salaries):
        list_salaries = list(salaries)
        yield (sec_econ, sum(list_salaries) / len(list_salaries)) if len(list_salaries) > 0 else 0

if __name__ == '__main__':
    MRSalaryPerSecEcon.run()
