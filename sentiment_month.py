from mrjob.job import MRJob
import re

DATE_RE = re.compile(r"[a-zA-Z]* [0-9]{4}")

class SentimentMonth(MRJob):

    def mapper(self, _, line):
        val = line.split(',')
        (review_posted_date, review_score_cleaned) = (val[1].strip(), float(val[3]))
        if (re.match(DATE_RE, review_posted_date) and (-1 <= review_score_cleaned <= 1)):
            month = review_posted_date.split()[0]
            yield month, review_score_cleaned

    def reducer(self, key, values):
        total = 0.0
        count = 0
        max = -1
        min = 1
        for value in values:
            total += value
            count += 1
            if(value > max):
                max = value
            if(value < min):
                min = value
        yield key, {"average":(total / count), "max":max, "min":min}

if __name__ == '__main__':
    SentimentMonth.run()
