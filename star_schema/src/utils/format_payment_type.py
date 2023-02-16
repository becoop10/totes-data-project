from star_schema.src.utils.helpers import remove_keys

def format_payment_type(raw_data):
    formatted_data = remove_keys(raw_data)
    return formatted_data
