from mrjob.job import MRJob

class SentimentListing(MRJob):

    def mapper(self, _,line):
        val=line.split(',')
        listing_id = int(val[0])
        rsc = float(val[3])
        if rsc <= 1 and rsc >= -1:
            yield listing_id, rsc

    def reducer(self, key, values):
        count =0
        total =0
        max =-1
        min = 1

        for t in values:
            count = count + 1
            total += t
            if(t>max):
                max = t
            if(t<min):
                min = t
        yield key , {"average":(total /count), "max":max, "min":min}


if __name__ == '__main__':
    SentimentListing.run()
