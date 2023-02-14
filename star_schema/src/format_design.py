def format_design(raw_design):
    formatted_designs = []
    for design in raw_design:
        print(design)
        new_details = {}
        new_details['design_id'] = design['design_id']
        new_details['design_name'] = design['design_name']
        new_details['file_location'] = design['file_location']
        new_details['file_name'] = design['file_name']
        formatted_designs.append(new_details)
    return formatted_designs
