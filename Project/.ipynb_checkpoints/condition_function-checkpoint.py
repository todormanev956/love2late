import pandas as pd

Conditions = pd.read_json('conditions_dataframe.json')

def lists_to_compare(descr1,interest1,descr2,interest2, inverse):
    lists = []
    if inverse:
        lists.append([descr1,interest2])
        lists.append([descr2,interest1])
    
    else:
        lists.append([descr1 + interest1, descr2 + interest2])
    
    return lists


def conditions_met(descr1, interest1, descr2, interest2):

    tags = set(descr1,interest1,descr2,interest2)

    for tag in tags:
        inverse = Conditions.loc[tag, 'Inverse']
        obligatory = Conditions.loc[tag, 'Obligatory']
        opposite = Conditions.loc[tag, 'Opposite']


        lists_compared = lists_to_compare(descr1,interest1,descr2,interest2, inverse)

        for compare in lists_compared:
            list1 = compare[0]
            list2 = compare[1]

            if obligatory:
                if tag in list1 and tag not in list2:
                    return False
                if tag in list2 and tag not in list1:
                    return False
            
            if opposite != '':
                if tag in list1 and opposite in list2:
                    return False
                if tag in list2 and opposite in list1:
                    return False
    
    return True

