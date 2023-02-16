from star_schema.src.utils.helpers import time_splitter, remove_keys


def format_payments(raw_data):
    time_formatted_data = [time_splitter(payment) for payment in raw_data]
    formatted_data = remove_keys(time_formatted_data, remove=[
                                 'company_ac_number', 'counterparty_ac_number'])
    return formatted_data
