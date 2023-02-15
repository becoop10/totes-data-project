
def find_match(primarykey, secondarykey, target, search_list):
    result = [match for match in search_list if match[secondarykey]
              == target[primarykey]][0]
    return result


def remove_keys(list,remove=["created_at","last_updated"]):
    return [{key:dictionary[key] for key in dictionary if key not in remove} for dictionary in list]


def time_splitter(dictionary):
    pass
    
#     listkeys=list(dictionary.keys())
#     print(dictionary)
#     for index,key in enumerate(listkeys):
        
#         if key == "created_at":
            
#             listkeys[index]=[[f"created_date",dictionary["created_at"].split()[0]],[f"created_time",dictionary["created_at"].split()[1]]]
#         if key == "last_updated":   
#             listkeys[index]=[[f"last_updated_date",dictionary["last_updated"].split()[0]],[f"last_updated_time",dictionary["last_updated"].split()[1]]]
            
#         else:
#             listkeys[index]=["hello","jello"]
#     print(listkeys)
#     returndict={listkey[0]:listkey[1] for listkey in listkeys}
#     print(returndict)
#     pass
            



