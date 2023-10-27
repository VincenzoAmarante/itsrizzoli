from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (ticket,opentime,type,size,item,priceopen,sl,tp,closetime,priceclose,commission,taxes,swap,profit) = line.split(',')
        yield item, 1
		
    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted_output(self, count, items):
        for item in items:
            yield item, count

if __name__ == '__main__':
    RatingsBreakdown.run()
