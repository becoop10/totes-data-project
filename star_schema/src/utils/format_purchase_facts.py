from star_schema.src.utils.helpers import time_splitter


def format_purchase(raw_purchase_data):
    formatted_purchases = [time_splitter(purchase)
                           for purchase in raw_purchase_data]

    return formatted_purchases
