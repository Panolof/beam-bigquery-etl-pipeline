import logging
from datetime import datetime

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
    # Implement aggregation logic here
    pass