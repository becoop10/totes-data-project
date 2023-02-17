
def find_match(primarykey, secondarykey, target, search_list):
    result = [match for match in search_list if match[secondarykey]
              == target[primarykey]][0]
    return result


def remove_keys(list,remove=["created_at","last_updated"]):
    return [{key:dictionary[key] for key in dictionary if key not in remove} for dictionary in list]


def time_splitter(dictionary):
    
        new_details = {}
        for key in dictionary:
            if key == "created_at":
                new_details['created_date'] = dictionary['created_at'].split(' ')[0]
                new_details['created_time'] = dictionary['created_at'].split(' ')[1]
            elif key == "last_updated":
                new_details['last_updated_date'] = dictionary['last_updated'].split(' ')[0]
                new_details['last_updated_time'] = dictionary['last_updated'].split(' ')[1]
            else:
                new_details[key]=dictionary[key]
        return new_details
        
