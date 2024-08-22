import logging
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

def filter_transactions(transaction, min_amount, min_year):
    logger.info(f"Filtering transaction: min_amount={min_amount}, min_year={min_year}")
    
    try:
        amount = float(transaction['amount'])
        date = datetime.strptime(transaction['date'], '%Y-%m-%d')
        
        if amount >= min_amount and date.year >= min_year:
            return transaction
        else:
            logger.debug(f"Filtered out transaction: {transaction}")
            return None
    except (KeyError, ValueError) as e:
        logger.error(f"Error processing transaction {transaction}: {str(e)}")
        return None

def aggregate_transactions(transactions):
    logger.info("Aggregating transactions")
    aggregated = defaultdict(float)
    
    for transaction in transactions:
        date = transaction['date']
        amount = float(transaction['amount'])
        aggregated[date] += amount
    
    result = [{'date': date, 'total_amount': total} for date, total in aggregated.items()]
    logger.info(f"Aggregated {len(transactions)} transactions into {len(result)} daily totals")
    return result

def generate_filtered_transactions(transactions, min_amount, min_year):
    logger.info(f"Generating filtered transactions: min_amount={min_amount}, min_year={min_year}")
    for transaction in transactions:
        filtered = filter_transactions(transaction, min_amount, min_year)
        if filtered:
            yield filtered