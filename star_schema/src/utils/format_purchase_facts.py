def format_purchase(raw_purchase):
    formatted_purchase = []

    for purchase in raw_purchase:
        new_details = {}
        for key in purchase:
            if key == "created_at":
                new_details['created_date'] = purchase['created_at'].split(' ')[0]
                new_details['created_time'] = purchase['created_at'].split(' ')[1]
            elif key == "last_updated":
                new_details['last_updated_date'] = purchase['last_updated'].split(' ')[0]
                new_details['last_updated_time'] = purchase['last_updated'].split(' ')[1]
            else:
                new_details[key]=purchase[key]
        formatted_purchase.append(new_details)
        

    return formatted_purchase













