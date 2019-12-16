import pandas as pd
import os
from pathlib import Path 
from bs4 import BeautifulSoup
import string
from Levenshtein import distance

Tags = pd.read_json('ftags.json') # list des tags
Jobs= pd.read_json('fjobs.json') # list des professions

#Fonctions principales du pipeline

#extraction des données et métadonnées des OCR de Transckribus
def OCR_to_DataFrame(folder): 
                               
    ocr_folder = Path(folder+'/ocr')
    metadata_folder = Path(folder+'/metadata')
    files = sorted(os.listdir(ocr_folder))
    meta_files = sorted(os.listdir(metadata_folder))
            
    frame = []
    meta_frame=[]
    
    for meta_file in meta_files:
        meta_xml = open(metadata_folder/meta_file,encoding="utf8")
        meta_xml_parsed = BeautifulSoup(meta_xml,'xml')
        
        date =str(meta_xml_parsed.find('dc:date')).replace('/','')\
                                                  .replace('<dc:date>','')
        titre =str(meta_xml_parsed.find('dc:title')).replace('/','')\
                                                    .replace('<dc:title>','')
                       
        meta_ark = meta_file.split('_')[0]
        meta_frame.append({'Ark': meta_ark,'Date': date, 'Titre':titre})
    
    meta_dataframe=pd.DataFrame.from_dict(meta_frame)
    meta_dataframe['Folder'] = folder
  
    
    for file in files:
        xml = open(ocr_folder/file,encoding="utf8")
        xml_parsed = BeautifulSoup(xml,'xml')
        
        name = file.strip('.xml')
        ark = name.split('_')[0]
        page = name.split('_')[1]
        image_name = name + '.jpg'
        
        
        paragraphs = xml_parsed.find_all(('TextRegion'))
        for paragraph in paragraphs:
            paragraph_id = paragraph['id']
            r_str = paragraph.Coords['points'].split(' ')
            region = [r_str[0].split(',')[0], r_str[0].split(',')[1],\
                      r_str[1].split(',')[0], r_str[2].split(',')[1]]
            region = [int(i) for i in region]
            text = paragraph.find_all('TextEquiv')[-1].Unicode.text
            frame.append({'Ark': ark,
                   'Page': page,
                   'Paragraph_id': paragraph_id,
                   'Image_file': image_name,
                   'Text': text,
                   'Coordinates': [region]})
                  
    
    dataframe = pd.DataFrame.from_dict(frame)
    
    
    
    return pd.merge(dataframe, meta_dataframe)


    
#Retourne une copie du dataframe sans les textes qui ne présentent pas la 
#structure type d'une annonce et avec de nouvelles annonces apportant des
# infos complémentaires. 
def update_frame(dataframe):
    
    periodic = dataframe.Folder.iloc[0]
    
    dataframe = update_shape(dataframe,periodic)

    dataframe.Text = dataframe.Text.map(lambda text:clean_text(text,dataframe.Titre.iloc[0])) # on clean le text

    dataframe = update_slice(dataframe)
    dataframe = update_sex(dataframe)
    dataframe = update_age(dataframe)
    dataframe = update_duplicates(dataframe)
    dataframe = update_tags(dataframe)
    dataframe.index = dataframe['Ark'] + dataframe['Page'].apply(str) + dataframe['Paragraph_id']
    dataframe=dataframe.drop_duplicates(['Text'])
    print('Il nous reste {0} annonces'.format(dataframe.shape[0]))
    return dataframe.drop_duplicates(['Text'])


def update_shape(dataframe,periodic):
    
    dataframe.loc[:,'Beginning'] = dataframe['Text'].map(lambda text: start_asanad(text,periodic))
    dataframe.loc[:,'End'] = dataframe['Text'].map(lambda text: end_asanad(text,periodic))
    dataframe=has_anad_shape(dataframe)
    
    print('A première vu, il y aurait {0} annonces'.format(dataframe[dataframe.Beginning & dataframe['Adwidth']].shape[0]))
    
    len_before = dataframe[dataframe.Beginning & dataframe.End].shape[0]
    for ind,row in dataframe.iterrows():
         if row.Beginning and (not row.End) and dataframe.index.shape[0]> ind+5:
            #la partie ci-dessous permet d'inclure les annonces dont le point de fin a été mal OCRisé mais dont on est sur 
             #que c'est une annonce car elles sont suivis par une autre annonce dans la page.
             if dataframe['Beginning'].loc[ind+1]:
                 dataframe.at[ind,'End'] = True
             #Ici on tente d'inclure le reste d'une annonce coupé en deux si les annonces qui suivents dans le tableau on la bonne forme
             elif any([dataframe['Adwidth'].loc[ind+i]  for i in range(1,4)]):
                 for i in range(1,4):
                     if dataframe['Beginning'].loc[ind+i]:
                         break 
                     elif dataframe['Adwidth'].loc[ind+i] and dataframe['End'].loc[ind+i] and dataframe.Adthickness.loc[ind+i] :
                         dataframe.at[ind,'Text'] += dataframe.Text.loc[ind+i]
                         dataframe.at[ind,'Coordinates'] = [dataframe.at[ind,'Coordinates'][0],dataframe.Coordinates.loc[ind+i][0]]
                         dataframe.at[ind,'End'] = True
                         dataframe.at[ind,'Saved'] = True
                         break
    saved = dataframe[dataframe.Saved == True].shape[0]
    print("{0} annonces ont pu être sauvé alors que transckribus ".format(saved)+\
          "les avaient scindés en deux paragraphes")
          
    
    len_before = dataframe[dataframe.Beginning & dataframe.End].shape[0]
    dataframe = dataframe[dataframe.Adthickness & dataframe.Adwidth]
    dataframe = dataframe[dataframe.Beginning & dataframe.End]
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car elles ne presentaient pas la structure type".format(lost))
    
    return dataframe.drop(columns =['Beginning','End','Adthickness','Adwidth','Saved'])

#Sépare les annonces en deux parties, l'une parlant de l'annonceur, l'autre de ce qu'il recherche.
#Si la séparation n'est pas possible, on retire l'annonce.
def update_slice(dataframe):

    Separation_Words = [' désire ',' épouserait ',' contracterait ','désirerait'\
                    ' recherche ',' épouser ',' marierait ',' dévouerait ',\
                    ' demande ', ' correspondrait ',' accepterait '," s'unirait "]
    len_before = dataframe.shape[0]
    dataframe=dataframe[dataframe.Text.apply(lambda text : any([word in text.lower() for word in Separation_Words]))]
    dataframe.loc[:,'Slices'] = dataframe.Text.map(lambda text : separate_in_two(text,Separation_Words))
    dataframe.loc[:,'Self'] = dataframe['Slices'].map(lambda slices : slices[0])
    dataframe.loc[:,'Other'] = dataframe['Slices'].map(lambda slices : slices[1])
    dataframe=dataframe[dataframe.Self.apply(lambda text : text != '')]
    lost =len_before-dataframe.shape[0]
    print('{0} annonces ont été perdu faute de mot de séparation'.format(lost))
    return dataframe.drop(columns='Slices')

def update_sex(dataframe):
    len_before = dataframe.shape[0]
    dataframe.loc[:,'Sex'] = (dataframe.Self +'----'+ dataframe.Other).map(lambda text : extract_sex(text))
    dataframe = dataframe[dataframe.Sex != 'None']
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car le sexe n'a pas pu être extrait".format(lost))
    return dataframe

#Retire les annonces dont ont ne peut extraire l'âge
def update_age(dataframe):   
    dataframe.loc[:,'Age'] = dataframe.Self.map(lambda text: extract_age(text))#on prends les 2 digits avant le mot "ans"
    len_before = dataframe.shape[0]
    dataframe = dataframe[dataframe['Age'] != 0 ]
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car l'age n'a pas pu être extrait".format(lost))
    return dataframe.drop(columns='Age')

#Ajoute une colonne avec les tags de l'annonce qui seront utilisé pour le matching 
def update_tags(dataframe):
    dataframe.loc[:,'Info'] = ''
    for ind,row in dataframe.iterrows():
        self_  = row.Self
        other_ = row.Other
        dataframe.at[ind,'Info'] = {'age':extract_age(self_),'other_age':extract_other_age(other_),'self_traits':extract_traits(self_),\
                                    'other_traits':extract_traits(other_),'self_social_stat':extract_statsoc(self_),\
                                    'other_social_stat':extract_statsoc(other_),'self_job':extract_job(self_,True),\
                                    'other_job':extract_job(other_,False),'rapport':extract_rapport(other_)}
    
    return dataframe.drop(columns=['Self','Other'])

def update_duplicates(dataframe):
    len_initial =dataframe.shape[0]
    dataframe=optimized_levenshtein(dataframe,10)
    lost = len_initial - dataframe.shape[0]
    print("{0} annonces ont été perdu car elles étaient des doublons".format(lost))
    return dataframe


#FONCTION SECONDAIRE
    
def clean_text(text,periodic):
    #enlève les charactères qui sont de trop
    text = text.lower()
    expr_to_replace_by_space = ['\n',' i ','.—','.-','. -','. —','—']
    expr_to_remove = ['¬\n','¬', '*','■','•','!','?','/','<','>','^',]
    for char in expr_to_remove:
        text = text.replace(char,'')
    for char in expr_to_replace_by_space:
        text = text.replace(char,' ')
    
    #enleve le format typique de début d'annonce:
    for char in text: 
        if char.isdigit():
            text=text.replace(char,'',1)
        else : 
            break 
            
    #liste de mot mal ORCisé et leur correction
    spell_error = [[e[0],e[1]] for e in pd.read_json('err').values]
    followed_char = ' .,'
    text = text+' '
    for char in followed_char:
        for error in spell_error:
            err = error[0] + char
            correction = error[1]+char
            if err in text:
                text = text.replace(err,correction)
    text = ' '.join(text.split())
 
    return text

  
def extract_age(text):
    segment = ''
    age = ''
    if 'ans' in text :
        segment = text.lower().split(' ans')[0][-5:] #on s'interesse au 5 charactère précédant le mot 'ans'
    
    for char in segment[::-1]:
        if char.isdigit() and len(age)<2:
            age = char + age 
    
            
    if len(age) == 2:
        intage = int(age)
        if intage>17 and intage<79:
            return intage
        else :
            return 0
    else :
        return 0
 
#On récupère la tranche d'age recherchée par l'auteur si elle est donnée  
def extract_other_age(text):
    segment = ''
    age_max = ''
    age_min = ''
    if 'ans' in text :
        segment = text.lower().split(' ans')[0][-8:] #on s'interesse aux 8 charactères précédant le mot 'ans'
    
    for char in segment[::-1]:
        if char.isdigit() and len(age_max)<2:
            age_max = char + age_max
        elif char.isdigit() and len(age_min)<2:
            age_min = char + age_min
            
    ages = []
    if len(age_min) == 2:
        intage_min = int(age_min)
        if (intage_min>15 and intage_min<79):
            ages.append(intage_min)
    if len(age_max) == 2:
        intage_max = int(age_max)
        if (intage_max>15 and intage_max<79):
            ages.append(intage_max)
    if len(ages)==1 and 'environ' in text:
        ages = [ages[0]-5,ages[0]+5]
    indifferent = ['âge indifférent','âge peu importe','age indifférent','importe quel âge'] #indique les personnes pour lesquelles l'age importe peu
    if len(ages) ==2:
        return ages
    elif any([word in text for word in indifferent]):
        return [0,100]
    else :
        return 'None' # dans ce cas, à mettre en relation avec personne de la même tranche d'age
    
#Compare le nombre de mot typiquement au feminin par rapport au nombre de mot au masculin pour définir le sex de l'auteur de l'annonce donnée
def extract_sex(text):

    fem_words = ['demoiselle','veuve','dot','élevée','âgée','orpheline','institutrice',\
                 'dame','femme','ouvrière','fille','divorcée','employée','rentière','distinguée',\
                 'instruite','gentille','intelligente','sérieuse','travailleuse','affectueuse',\
                 'forte','compagne','brune','blonde','aimante']#Liste de mot utilisé principalement par ou pour des femmes dans les annonces
    masc_words =['veuf','âgé','officier','retraite','retraité','établi','garçon','employé','mécanicien',\
                 'agriculteur','monsieur','rentier','commerçant','notaire','ouvrier','distingué',\
                 'élevé','déformé','mutilé','amputé','instruit','homme','sérieux','professeur',\
                 'travailleur','blessé','doux','orphelin','gai','affectueux','brun','sobre',\
                 'divorcé','blond','intelligent'] #Liste de mot utilisé principalement par ou pour des hommes dans les annonces
    Self,Other = text.split('----')
    masculinity = list_score(Self,masc_words)+list_score(Other,fem_words)
    feminity = list_score(Self,fem_words)+list_score(Other,masc_words)
    if masculinity == feminity:
        return 'None'
    elif masculinity > feminity:
        return 'Man'
    else:
        return 'Woman'
    
#compte le nombre de fois qu'un element de la liste donnée apparait dans un texte donné
def list_score(text,word_list):
    for p in string.punctuation:
        text = text.replace(p,' ')
    words = [ word for word in text.lower().split() if len(word) > 2 ]
    score = 0
    for word in words:
        if word in word_list:
            score +=1
    return score
    
#Sépare l'annonce en deux entre auto-description et personne recherché
def separate_in_two(text,Separation_Words):
    
    t = text
    for punc in string.punctuation:
        t= t.replace(punc,' ')
    t = ' '.join(t.split())
    
    if any([word in t for word in Separation_Words]):
        
        Separation_Word = ''
        ind=10000
        for word in Separation_Words:
            if word in t:
                if ind > t.split().index(word.replace(' ','')):
                    Separation_Word = word.replace(' ','')
                    ind = t.split().index(word.replace(' ',''))
        return text.split(Separation_Word, 1)            
    else:
        return ['','']
    
    
#Défini le text donné apporte des indication sur la classe scoial (riche ou pauvre) de l'auteur ou de la personne recherché
def extract_statsoc(text):
    pauvre = [' sans fortune',' pauvre',' sans avoir',' sans argent',' sans dot',' pas de fortune','sans propriété']
    m_pauvre = [' quelques économies',' petite propriété',' avec ou sans dot',' petites économies',\
                ' situation d’avenir',' modeste avoir',' présentant bien',' petit avoir',\
                ' petite fortune','situation moyenne','famille honorable','petite dot']
    aise = [ ' aisé',' possédant mobilier','appointement',' trousseau',' dot',' propriété',\
             ' ayant situation',' propriétaire distingué',' parents aisés',' famille aisée',' rentière',\
             ' bel avenir',' situation assurée',' petite fortune',' avec fortune',' bonne situation',' bijoux',\
             ' ayant fortune',' économie',' belle installation',' ayant avoir',' belle situation',' argenterie',\
             ' beau mobilier',' bonne dot',' gentil avoir',' situation sûre',' propriétaire']
    riche = [' industrie prospère',' propriétés',' riche mobilier',' très aisée',' belle situation et fortune',\
             ' fortuné',' situation de fortune',' titre',' noble',' rente',' renti']
    dic={}
    for elem in pauvre:
        dic[elem]=-2
    for elem in m_pauvre:
        dic[elem]=-1
    for elem in aise:
        dic[elem]=0.5
    for elem in riche:
        dic[elem]= 2
        
    dic= { k:dic[k] for k in sorted(dic,key=len,reverse=True)}
        
    score = 0 #score pour évalué la classe sociale
    param = 0
    t=text
    for punct in string.punctuation:
        t = t.replace(punct,'')
    segments_text = t.split('fr')
    moneys = []
    for segment in segments_text:
            segment = segment.replace(' ','')
            money=''
            while len(segment)>0 and segment[-1].isdigit():
                money=segment[-1] +money
                segment = segment[:-1]
            if len(money)>0:
                if int(money)>0:
                        moneys.append(int(money))
    if len(moneys)>0:
        if max(moneys) >100000: # si la personne parle de plus de 100 000 francs dans son annonce, elle est considérée riche
            return 'riche' #riche
        elif max(moneys)>10000:
            score +=1
            param +=1
            
   
    t = text    
    for elem in dic:
        if elem in t:
            score += dic[elem]
            param += 1
            t = t.replace(elem,' ')

    if param == 0:
        return 'None'
    else:
        mean = score/param
        
    if mean>0:
        return 'riche' 
    else:
        return 'pauvre' 
    
#retourne les informations essentielles au matching (défini dans le fichier .json)
def extract_traits(text):
    
    t = text
    text_tags=[]
    for itag in Tags.index:
        if itag in text:
            text_tags.append(Tags.loc[itag,'tag'])
            t = t.replace(itag,' ')
    return list(set(text_tags))

#retourne la profession de l'auteur ou les professions recherché par l'auteur si self_ == True
def extract_job(text,self_):
    
    list_ =[]
    for job in Jobs.index:
        if self_:
            for segments in text.split(','):
                if job in segments:
                    return Jobs.loc[job].Equivalence
        else:
            if job in text:
                list_.append(Jobs.loc[job].Equivalence)
    if self_:
        return ''
    return list_

#retourne les choses que l'auteurs veut avoir en rapport avec son match
def extract_rapport(text):
    rapport = []
    for segment in text.split(','):
        if 'rapport' in segment or 'analogue' in segment:
            if 'âge' in segment:
                rapport.append('âge')
            if 'situation' in segment or 'd’avoir' in segment:
                rapport.append('situation')
            return rapport
    return rapport

#Retourne True si le début du texte présente la structure type d'une annonce sauf pour 'RV' ou l'on considère la fin de l'annonce
def start_asanad(text, periodic):
    if periodic == 'RV':
        if ('. —' in text[-13:] or '. -' in text[-13:] or '.-' in text[-13:] or '.—' in text[-13:])\
           and (any(c.isdigit() for c in text[-6:]) or any(c=='N' for c in text[-9:])):
            return True
        else:
            return False
    else: 
        #return true if the the 20 firsts letters of text have '. —' in it and if the first 4 letters contain a digit 
        t = text[:20]
        if any(c.isdigit() for c in t[:4]):
            typical_start = ['—','-']
            if  any([elem in t for elem in typical_start]):
                return True 
            else:
                return False
        else : return False
#Retourne True si la fin du texte présente la structure type d'une annonce sauf pour 'RV' ou l'on considère le début de l'annonce
def end_asanad(text,periodic):
    
    if periodic == 'RV':
        if any(c.isupper() for c in text[:2]):
            return True
        else:
            return False
    else: 
        if any(c=='.' for c in text[-5:]):
            return True 
        else:
            return False
        
#retourne le dataframe avec uniquement les annonces ayant la dimension type d'une annonce
def has_anad_shape(dataframe):
    periodic = dataframe.Folder.iloc[0]
    #boundary of ad width and thickness in pixel for each periodic
    LLDM = [1080,1190,45,330]
    LI = [835,920,45,736]
    RV = [870,950,50,300]
    LTDU = [1025,1055,200,661]
    
    if periodic == 'LLDM':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[0][2]-coord[0][0] >LLDM[0] and coord[0][2]-coord[0][0] < LLDM[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[0][3]-coord[0][1] >LLDM[2] and coord[0][3]-coord[0][1] < LLDM[3] )
    elif periodic == 'LI':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[0][2]-coord[0][0] >LI[0] and coord[0][2]-coord[0][0] < LI[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[0][3]-coord[0][1] >LI[2] and coord[0][3]-coord[0][1] < LI[3] )
    elif periodic == 'LTDU':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[0][2]-coord[0][0] >LTDU[0] and coord[0][2]-coord[0][0] < LTDU[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[0][3]-coord[0][1] >LTDU[2] and coord[0][3]-coord[0][1] < LTDU[3] )
    elif periodic == 'RV':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[0][2]-coord[0][0] >RV[0] and coord[0][2]-coord[0][0] < RV[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[0][3]-coord[0][1] >RV[2] and coord[0][3]-coord[0][1] < RV[3] )
        
    
    return dataframe 


# on accélère le calcule en découpant le dataframe en n_slices, chaque tranche 
#contenant des textes de longueurs similaires, basé sur un intervalle légèrement
# plus grand qu'un quantiles
def optimized_levenshtein(dataframe,n_slices):
    quantiles = [dataframe.Text.map(lambda text : len(text)).quantile(i/n_slices) for i in range(0,n_slices+1)]
    df_sliced = [dataframe[dataframe.Text.apply(lambda text : len(text)>quantiles[i]-quantiles[1]/20 and len(text)<quantiles[i+1]+quantiles[1]/20)] for i in range(0,n_slices)]
    df_sliced = [compute_levenshtein(df) for df in df_sliced ]
    return pd.concat(df_sliced).drop_duplicates(subset = 'Text')
    

#On évalue la distance de levenshtein entre chaque annonce et quand deux annonces
# ont une distance plus petite que 5% de la somme de leur longueur on en retire une
# des deux, ainsi on diminue drastiquement le nombre de doublons.
def compute_levenshtein(dataframe):
    copy = dataframe.copy()
    iterated=[]
    for ind1, row1 in copy.iterrows():
        for ind2,row2 in copy.drop(index = ind1).iterrows():
            if ind2 not in iterated:
                t1=row1.Text
                t2=row2.Text
                d = distance(t1,t2)
                threshold = (len(t1)+len(t2))/20 
                
                if d < threshold:
                    iterated.append(ind1)
                    dataframe = dataframe.drop(index = ind1)
                    break
        iterated.append(ind1)
    return dataframe
            
        
