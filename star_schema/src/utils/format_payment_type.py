def format_payment_type(raw_data):
    formatted_data = []
    for payment_type in raw_data:
        new_details = {}
        new_details['payment_type_id'] = payment_type['payment_type_id']
        new_details['payment_type_name'] = payment_type['payment_type_name']
        formatted_data.append(new_details)
    return formatted_data
