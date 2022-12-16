from mrjob.job import MRJob

class SentimentWords(MRJob):

    def mapper(self, _, line):
        val = line.split(',')
        (clean_text, review_score_cleaned) = (val[2].strip(), float(val[3]))
        if ((len(clean_text) > 0) and (-1 <= review_score_cleaned <= 1)):
            frequency = {}
            words = clean_text.split()
            for word in words:
                if len(word) > 1:
                    if word in frequency.keys():
                        frequency[word] += 1
                    else:
                        frequency[word] = 1
            sentiment = "positive"
            if review_score_cleaned <= -0.05:
                sentiment = "negative"
            elif review_score_cleaned < 0.05:
                sentiment = "neutral"
            frequency = dict(sorted(frequency.items(),
                                key=lambda item: item[1], reverse=True))
            yield sentiment, frequency

    def reducer(self, key, values):
        total_frequency = {}
        for frequency in values:
            for word in frequency:
                if word in total_frequency.keys():
                    total_frequency[word] += frequency[word]
                else:
                    total_frequency[word] = frequency[word]
        total_frequency = dict(filter(lambda item: item[1] > 1, total_frequency.items()))
        total_frequency = dict(sorted(total_frequency.items(),
                                key=lambda item: item[1], reverse=True))
        yield key, total_frequency

if __name__ == '__main__':
    SentimentWords.run()
