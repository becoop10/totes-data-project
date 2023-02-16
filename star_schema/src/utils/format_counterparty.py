from star_schema.src.utils.helpers import find_match
import logging
logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)


def format_counterparty(raw_counter, raw_address):
    formattedList = []
    for counter in raw_counter:
        new_details = {}
        try:
            match = find_match(
                "legal_address_id",
                "address_id",
                counter,
                raw_address)
            prefix = "counterparty_legal_"
            new_details["counterparty_id"] = counter["counterparty_id"]
            new_details[f"{prefix}name"] = counter[f"{prefix}name"]
            new_details[f"{prefix}address_line_1"] = match["address_line_1"]
            new_details[f"{prefix}address_line_2"] = match["address_line_2"]
            new_details[f"{prefix}district"] = match["district"]
            new_details[f"{prefix}city"] = match["city"]
            new_details[f"{prefix}postal_code"] = match["postal_code"]
            new_details[f"{prefix}country"] = match["country"]
            new_details[f"{prefix}phone_number"] = match["phone"]
            formattedList.append(new_details)
        except IndexError:
            logger.error(f'{counter} no matching table found')
            
            
            continue

    return formattedList
