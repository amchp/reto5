from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSalaryPerEmployee(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salaries,
                   reducer=self.reducer_sum_salaries),
        ]

    def mapper_get_salaries(self, _, line : str): 
        columns : list[str] = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['id_emp'], int(data_row['salary']))

    def reducer_sum_salaries(self, id_emp : str, salaries : tuple[int]):
        yield (id_emp, sum(salaries))

if __name__ == '__main__':
    MRSalaryPerEmployee.run()
