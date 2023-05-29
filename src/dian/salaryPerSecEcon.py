from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSalaryPerSecEcon(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salaries_per_sec_econ,
                   reducer=self.reducer_sum_salaries),
        ]

    def mapper_get_salaries_per_sec_econ(self, _, line : str): 
        columns : list[str] = ['id_emp', 'sec_econ', 'salary', 'year']
        data_row : dict[str, str] = dict(zip(columns, line.split(',')))
        yield (data_row['sec_econ'], int(data_row['salary']))

    def reducer_sum_salaries(self, sec_econ : str, salaries : tuple[int]):
        yield (sec_econ, sum(salaries))

if __name__ == '__main__':
    MRSalaryPerSecEcon.run()
