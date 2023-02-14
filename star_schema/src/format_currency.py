def format_currency(raw_currency):
    currency_names = {'GBP': 'Great British Pound',
                      'USD': 'United States Dollar', 'EUR': 'Euro'}
    formatted_currency = []
    for currency_data in raw_currency:
        new_details = {}
        new_details['currency_id'] = currency_data['currency_id']
        new_details['currency_code'] = currency_data['currency_code']
        new_details['currency_name'] = currency_names[currency_data['currency_code']]
        formatted_currency.append(new_details)
    return formatted_currency