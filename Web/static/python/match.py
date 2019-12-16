import numpy as np
import pandas as pd
from static.python import journal_image as photo
from datetime import datetime

# df = pd.read_json('./static/python/goodmatch.json')

def match(df, dic):

    # Sex
    copy = df[df.Sex == dic['sex']]
    # Age
    copy = copy[copy.Info.apply(lambda x : x['age'] >= dic['age'][0] and x['age'] <= dic['age'][1])]
    # Score calculation
    copy['Score_web'] = copy.Info.map(lambda x : compute_score(x['self_traits'],dic['tags']))
    max_score = copy.Score_web.max()
    # Chosing the best matches
    copy = copy[copy.Score_web == max_score]
    
    

    print('-----------------------------------------------------\n', dic)
    print('MAX SCORE:      ', max_score)
    print('NUMBER OF MATCHES:      ', copy.shape[0])
    print(copy.head())


    if copy.shape[0]:
        return copy.sample(1).index[0] # Chose randomly from the best
    else :
        return None


def compute_score(traits,list_):
    score = 0
    for trait in traits:
        if trait in list_:
            score+=1        
    return score

def get_info(df, index):
    if index == None:
        return None
    else:
        match = {
            'journal': df.loc[index, 'Titre'],
            'date': str(datetime.utcfromtimestamp(df.loc[index, 'Date']/1000).day) + \
                '/' + str(datetime.utcfromtimestamp(df.loc[index, 'Date']/1000).month) + \
                    '/' + str(datetime.utcfromtimestamp(df.loc[index, 'Date']/1000).year),
            'page_nb': df.loc[index, 'Page'],
            'cover': photo.cover(df, index),
            'page': photo.page(df, index),
            'paragraph': photo.paragraph(df, index),
            'soulmate': False,
            'competition': None
            }
        if df.SoulMate.loc[index] != None:
            match['soulmate'] = True
            match['competition'] = photo.paragraph(df, df.SoulMate.loc[index])
        return match

