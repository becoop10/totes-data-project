
def format_sales_facts(raw_sales_data):
    formatted_sales = []

    for sale in raw_sales_data:

        new_details = {}
        for key in sale:

            if key == "staff_id":
                new_details["sales_staff_id"] = sale[key]
            elif key == "created_at":
                new_details['created_date'] = sale['created_at'].split(' ')[0]
                new_details['created_time'] = sale['created_at'].split(' ')[1]
            elif key == "last_updated":
                new_details['last_updated_date'] = sale['last_updated'].split(' ')[
                    0]
                new_details['last_updated_time'] = sale['last_updated'].split(' ')[
                    1]
            else:
                new_details[key] = sale[key]

        formatted_sales.append(new_details)

    return formatted_sales
