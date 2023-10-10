import datetime
import logging
import random
import azure.functions as func
import os
from azure.eventhub import EventHubProducerClient, EventData

# Retrieve environment variables
EVENTHUB_CONNECTION_STRING = os.environ["EventHubConnectionString"]
EVENTHUB_NAME = os.environ["EventHubName"]

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('Python timer function is running late!')

    transaction = generate_transaction()
    logging.info(f"Generated transaction at {utc_timestamp}: {transaction}")

    # Send the transaction to the Event Hub
    send_eventhub_message(transaction)

def generate_transaction():
    merchants = [f'Merchant{i}' for i in range(1, 101)]  # Generating 100 merchant names

    # Note: The following are NOT real credit card numbers but adhere to the formats used by credit card companies for testing purposes.
    prefixes_visa = ["4539", "4556", "4916", "4532", "4929", "40240071", "4485"]
    prefixes_mastercard = ["51", "52", "53", "54", "55"]
    prefixes_amex = ["34", "37"]

    card_number = random.choice([
        generate_credit_card_number(random.choice(prefixes_visa), 16),
        generate_credit_card_number(random.choice(prefixes_mastercard), 16),
        generate_credit_card_number(random.choice(prefixes_amex), 15),
    ])
    
    transaction = {
        "card": card_number,
        "merchant": random.choice(merchants),
        "amount": round(random.uniform(5, 500), 2),
        "timestamp": datetime.datetime.now().isoformat()
    }
    return transaction


def generate_credit_card_number(prefix, length):
    """
    Generates a credit card number with a given prefix and total length.
    
    Args:
        prefix (str): The starting digits of the card number.
        length (int): The total length of the card number.
        
    Returns:
        str: A generated credit card number as a string.
    """
    # The length of the random number to be appended to the prefix
    random_number_length = length - len(prefix)
    
    # Generate the random number segment of the card number
    random_number = ''.join(random.choice("0123456789") for _ in range(random_number_length))
    
    # Combine the prefix and random_number to create the full card number
    card_number = prefix + random_number
    
    return card_number

def send_eventhub_message(transaction):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING, 
        eventhub_name=EVENTHUB_NAME
    )
    
    try:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(str(transaction)))
        producer.send_batch(event_data_batch)
        logging.info("Message sent!")
    except Exception as ex:
        logging.error(f"Exception: {str(ex)}")
    finally:
        producer.close()