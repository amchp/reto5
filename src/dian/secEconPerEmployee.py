from collections import Counter
from typing import Generator
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSecEconPerEmployee(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sec_econ_per_employee,
                   reducer=self.reducer_get_sec_econs),
        ]

    def mapper_get_sec_econ_per_employee(self, _, line): 
        columns = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row = dict(zip(columns, line.split(',')))
        yield (data_row['id_emp'], data_row['sec_econ'])

    def reducer_get_sec_econs(self, id_emp, sec_econs):
        yield (id_emp, len(Counter(list(sec_econs)).keys()))

if __name__ == '__main__':
    MRSecEconPerEmployee.run()

