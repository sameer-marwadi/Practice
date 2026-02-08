from mrjob.job import MRJob

class MRFraudTransactionCount(MRJob):
    def mapper(self, _, line):
        # Example transaction line format: ID, Amount, IsFraud
        data = line.split(',')
        transaction_id = data[0]
        is_fraud = data[2]  # Assuming 1 for fraud, 0 for legitimate
        
        # Yield fraudulent transactions
        if is_fraud == '1':
            yield ('fraud', 1)

    def reducer(self, key, values):
        # Sum up all fraudulent transactions
        yield (key, sum(values))
