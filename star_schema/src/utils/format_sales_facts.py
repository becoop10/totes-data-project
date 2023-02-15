def format_sales_facts(raw_sales_data):
    formatted_sales = []

    for sale in raw_sales_data:
        new_details = {}
        new_details['sales_record_order_id'] = sale['sales_order_id']
        new_details['created_date'] = sale['created_at'].split(' ')[0]
        new_details['created_time'] = sale['created_at'].split(' ')[1]
        new_details['last_updated_date'] = sale['last_updated'].split(' ')[0]
        new_details['last_updated_time'] = sale['last_updated'].split(' ')[1]
        new_details['sales_staff_id'] = sale['staff_id']
        new_details['counterparty_id'] = sale['counterparty_id']
        new_details['units_sold'] = sale['units_sold']
        new_details['unit_price'] = sale['unit_price']
        new_details['currency_id'] = sale['currency_id']
        new_details['design_id'] = sale['design_id']
        new_details['agreed_delivery_date'] = sale['agreed_delivery_date']
        new_details['agreed_payment_date'] = sale['agreed_payment_date']
        new_details['agreed_delivery_location_id'] = sale['agreed_delivery_location_id']
        formatted_sales.append(new_details)

    return formatted_sales
