import pandas as pd
import numpy as np

Conditions = pd.read_json('fconditions.json')
Points = pd.read_json('fbonus.json')


# On ajoute l'indice du match de chaque annonce au dataframe
def update_matches(dataframe,maximise_n_match):
    
    love_matrix = get_matches_matrix(dataframe[dataframe.Sex == 'Man'],dataframe[dataframe.Sex == 'Woman'])
    
    couples,left_alone = create_love(love_matrix,maximise_n_match)
    print('{0} hommes, {1} femmes et {2} matches!'.format(dataframe[dataframe.Sex == 'Man'].shape[0],\
          dataframe[dataframe.Sex == 'Woman'].shape[0],len(couples)))
    for couple in couples:
        dataframe.loc[couple[0],'SoulMate'] = couple[1]
        dataframe.loc[couple[0],'Score']=couple[2]
        dataframe.loc[couple[1],'SoulMate'] = couple[0]
        dataframe.loc[couple[1],'Score']=couple[2]
        dataframe = dataframe.sort_values('Score', ascending=False)
    return dataframe.drop(columns = 'Score'),left_alone


#retourne True si les deux annonces associés au tags en entrée peuvent matcher 
def conditions_met(tags1, tags2):
    
    descr1 = tags1['self_traits']
    interest1 = tags1['other_traits']
    descr2 = tags2['self_traits']
    interest2 = tags2['other_traits']
    
    social_status1=tags1['self_social_stat']
    social_status2=tags2['self_social_stat']

    age1=tags1['age']
    age2=tags2['age']
    
    age_other1=tags1['other_age']
    age_other2=tags2['other_age']
    
    #Prise en compte de situation en rapport et âge en rapport
    for rapport in tags1['rapport']:
        if rapport == 'âge':
            if age2 > age1+5 or age2 < age1-5:
                return False
        if rapport == 'situation':
            if social_status1!=social_status2:
                return False
    for rapport in tags2['rapport']:
        if rapport == 'âge':
            if age1 > age2+5 or age1 < age2-5:
                return False
        if rapport == 'situation':
            if social_status1!=social_status2:
                return False
            
    #condition sur l'age
    if age_other1 == 'None':
        if age2>age1+10 or age2<age1-10: # si quelqu'un n'a pas précisé de tranche d'age, on fixe un ecart de max 10 ans.
            return False 
    else:
        if age2<age_other1[0] or age2>age_other1[1]:
            return False
    if age_other2 == 'None':
        if age1>age2+10 or age1<age2-10:
            return False 
    else:
        if age1<age_other2[0] or age1>age_other2[1]:
            return False
    
    # on s'attaque ensuite au condition plus spécifique (tiré de fconditions.json)
    cond1 = [ cond for cond in descr1 if cond in Conditions.index]
    cond1 = [ cond for cond in cond1 if not Conditions.loc[cond,'inverse']] # On garde les conditions taguées 'inverse' que si elles font parti de la description
    cond1.extend([ cond for cond in interest1 if cond in Conditions.index]) 
    
    cond2 = [ cond for cond in descr2 if cond in Conditions.index]
    cond2 = [ cond for cond in cond2 if not Conditions.loc[cond,'inverse']]
    cond2.extend([ cond for cond in interest2 if cond in Conditions.index])
    
    
    cond1 = list(set(cond1))
    cond2 = list(set(cond2))
    
    for tag in cond1:
        inverse = Conditions.loc[tag, 'inverse']
        opposite = Conditions.loc[tag, 'opposite']
        
        if inverse : # si inverse True, on garde regarde si l'opposé du mot en question est dans l'autre annonce
            for opp in opposite:
                if opp in descr2 or opp in interest2 :
                    return False
        else: # Sinon, il faut que les deux annonces aient les mêmes conditions 
            if tag not in descr2 and tag not in interest2:
                    return False 
                
    for tag in cond2:
        inverse = Conditions.loc[tag, 'inverse']
        opposite = Conditions.loc[tag, 'opposite']
        
        if inverse :
            for opp in opposite:
                if opp in descr1 or opp in interest1 :
                    return False
        else:
            if tag not in descr1 and tag not in interest1:
                    return False

    return True


#Calcule un score parfaitement arbitraire entre deux annonces, plus il est
#élevé, plus on est sûre qu'ils s'aimeront.                
def compute_score(tags1, tags2): 
    score1 = 0
    score2 = 0
    
    descr1 = list(set([ bonus for bonus in tags1['self_traits'] if bonus in Points.index]))
    interest1 = list(set([ bonus for bonus in tags1['other_traits'] if bonus in Points.index]))
    descr2 = list(set([ bonus for bonus in tags2['self_traits'] if bonus in Points.index]))
    interest2 = list(set([ bonus for bonus in tags2['other_traits']  if bonus in Points.index]))
    
    
    if ('mutilé' in descr1 and 'beauté' in interest2) or ('mutilé' in descr2 and 'beauté' in interest1): #mutilé == pas beau
        return None
    
    for tag in descr1:
        if tag in interest2:
            score1+=Points.loc[tag,'points']
        
    for tag in descr2:
        if tag in interest1:
            score2+=Points.loc[tag,'points']   
    
    if tags1['self_job'] in tags2['other_job']:
        score1+=10
    if tags2['self_job'] in tags1['other_job']:
        score2+=10
        
        
    if tags1['self_social_stat'] in tags2['other_social_stat'] and tags1['self_social_stat'] != 'None' :
        score1+=5
    if tags2['self_social_stat'] in tags1['other_social_stat'] and tags2['self_social_stat'] != 'None' :
        score1+=5
        
    
    if score1+score2: # On garde que les scores non nuls 
        return score1+score2
    else :
        return None

#retourne une matrice avec les indices de sex1 en rangée et les indices de sex2
# en colonne. Les valeurs sont le score entre les deux indices 
def get_matches_matrix(sex1,sex2):
    
    matrix = pd.DataFrame()
    for ind1,row1 in sex1.iterrows():
        for ind2,row2 in sex2.iterrows():
            if conditions_met(row1.Info, row2.Info):
                matrix.at[ind1,ind2] = compute_score(row1.Info, row2.Info)
            else:
                matrix.at[ind1,ind2] = None
    return matrix


#Deux algorithmes en fonction de maximise_match:
#maximise_match == True : On cherche à minimiser le nombres d'annonces laissées
#seules en commencant par trouver un match à ceux qui ont le moins de candidat possible
#parmis le sexe limitant
#maximise_match == False : On cherche à maximisé les scores entres les annonces qui
# match en extrayant en priorité les matchs avec le plus haut score
def create_love(matrix,maximise_n_match):
    copy = matrix.copy()
    score = 1
    couples = []
    if maximise_n_match: # on veut faire le plus grand nombre de match possible
        while min(copy.shape):
            copy=copy.dropna(axis=0,how='all')
            copy=copy.dropna(axis=1,how='all')
            coord=['','']
            smallest_axis = np.argmin(copy.shape)
            biggest_axis=1-smallest_axis
            sm_ind = copy.isna().sum(axis=biggest_axis).idxmax() #on récupère l'indice ayant le moins de match possible dans le plus petit axe de la matrice 
            if sm_ind in copy.index:
                bg_ind = copy.loc[sm_ind].idxmax() #on maximise le score du match
            elif sm_ind in copy.columns:
                bg_ind = copy[sm_ind].idxmax() #on maximise le score du match
            else:
                break
            if sm_ind==sm_ind and bg_ind==bg_ind:
                coord[biggest_axis] = bg_ind
                coord[smallest_axis] = sm_ind
                score = copy.at[coord[0],coord[1]]
                copy = copy.drop(index = coord[0],columns = coord[1])
                if  score :
                    coord.append(score)
                    couples.append(coord)
            else:
                break
    else: # on veut les matchs de meilleurs qualité en priorité 
        while min(copy.shape) and score:
            max_col=copy.max().idxmax()
            if type(max_col)==float:
                break
            max_index=copy[max_col].idxmax()
            if type(max_index)==float:
                break
            score = copy.loc[max_index,max_col]
            if not score :
                break
            copy = copy.drop(index = max_index,columns = max_col)
            couples.append([max_index,max_col,score])
        
    return couples,copy






