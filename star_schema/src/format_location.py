def format_location(unformatted):
    remove = ["created_at", "last_updated"]

    formatted = [{key: location[key] for key in location.keys() if key not in remove}
                 for location in unformatted]

    return (formatted)
