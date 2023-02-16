from star_schema.src.utils.helpers import find_match
import logging
logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)



def format_staff(unformatted_staff, unformatted_depts):
    formatted_staff = []
    for staff in unformatted_staff:
        new_details = {}
        try:
            new_details['staff_id'] = staff['staff_id']
            new_details['first_name'] = staff['first_name']
            new_details['last_name'] = staff['last_name']
            match = find_match(
                "department_id",
                "department_id",
                staff,
                unformatted_depts)
            new_details['department_name'] = match['department_name']
            new_details['location'] = match['location']
            new_details['email_address'] = staff['email_address']
            formatted_staff.append(new_details)
        except IndexError:
            logger.error(f'{staff} no matching table found')

            continue

    return formatted_staff
