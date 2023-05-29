from collections import Counter
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSecEconPerEmployee(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sec_econ_per_employee,
                   reducer=self.reducer_get_sec_econs),
        ]

    def mapper_get_sec_econ_per_employee(self, _, line : str): 
        columns : list[str] = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['id_emp'], data_row['sec_econ'])

    def reducer_get_sec_econs(self, id_emp : str, sec_econs : tuple[str]):
        yield (id_emp, len(Counter(sec_econs).keys()))

if __name__ == '__main__':
    MRSecEconPerEmployee.run()

