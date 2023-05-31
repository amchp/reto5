from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSalaryPerEmployee(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salaries,
                   reducer=self.reducer_sum_salaries),
        ]

    def mapper_get_salaries(self, _, line): 
        columns = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['id_emp'], float(data_row['salary']))

    def reducer_sum_salaries(self, id_emp, salaries):
        list_salaries = list(salaries)
        yield (id_emp, sum(list_salaries) / len(list_salaries)) if len(list_salaries) > 0 else 0

if __name__ == '__main__':
    MRSalaryPerEmployee.run()
