
def find_match(primarykey,secondarykey, target, search_list):
    result = [match for match in search_list if match[secondarykey]==target[primarykey]][0]
    return result