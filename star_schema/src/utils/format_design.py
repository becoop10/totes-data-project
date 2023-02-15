def format_design(raw_design):
    

    remove = ["created_at", "last_updated"]

    formatted = [{key: design[key] for key in design.keys() if key not in remove}
                 for design in raw_design]

    return (formatted)

