
import pandas as pd
import numpy as np
import os
from pathlib import Path #Good pathmanagment across different systems (Mac/Windows...)
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from requests import get
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm
import string
import cv2
import PIL
from IPython.display import display
import spacy
import fr_core_news_md
nlp = fr_core_news_md.load()


#Fonction pour la Pipeline

def OCR_to_DataFrame(folder): #retourne un dataframe à partir des fichiers xmls dans les sous dossiers 'metadata' et 'ocr' de 'folder'
    ocr_folder = Path(folder+'/ocr')
    metadata_folder = Path(folder+'/metadata')
    files = sorted(os.listdir(ocr_folder))
    meta_files = sorted(os.listdir(metadata_folder))
            
    frame = []
    meta_frame=[]
    
    for meta_file in meta_files:
        meta_xml = open(metadata_folder/meta_file,encoding="utf8")
        meta_xml_parsed = BeautifulSoup(meta_xml,'xml')
        
        date =str(meta_xml_parsed.find('dc:date')).replace('/','').replace('<dc:date>','')
        titre =str(meta_xml_parsed.find('dc:title')).replace('/','').replace('<dc:title>','')
        publisher =str(meta_xml_parsed.find('dc:publisher')).replace('/','').replace('<dc:publisher>','')
        meta_ark = meta_file.split('_')[0]
        meta_frame.append({'Ark': meta_ark,'Date': date, 'Titre':titre,'Publisher':publisher})
    meta_dataframe=pd.DataFrame.from_dict(meta_frame)
    meta_dataframe['Folder'] = folder
# =============================================================================
#     meta_dataframe.Date = meta_dataframe.Date.map(lambda d : d.contents[0])
#     meta_dataframe.Titre = meta_dataframe.Titre.map(lambda t : t.contents[0])
#     meta_dataframe.Publisher = meta_dataframe.Publisher.map(lambda p : p.contents[0])
# =============================================================================
    
    
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
            region = [r_str[0].split(',')[0], r_str[0].split(',')[1], r_str[1].split(',')[0], r_str[2].split(',')[1]]
            region = [int(i) for i in region]
# =============================================================================
#             words = paragraph.find_all('Word')
#             bold = []
#             for word in words:
#                 try:
#                     if word.TextStyle['bold'] == 'true':
#                         bold.append(word.TextEquiv.Unicode.text)
#                 except:
#                     continue
# =============================================================================
            text = paragraph.find_all('TextEquiv')[-1].Unicode.text
            frame.append({'Ark': ark,
                   'Page': page,
                   'Paragraph_id': paragraph_id,
                   'Image_file': image_name,
                   'Text': text,
                   'Coordinates': region})
                   #'Bold_words': bold})
    dataframe = pd.DataFrame.from_dict(frame)
    
    
    
    return pd.merge(dataframe, meta_dataframe)


    
#Retourne une copie du dataframe sans tous les  textes qui ne présente pas la structure type d'une annonce et avec une nouvelle colonne 
#précisant, si possible, le sexe de la personne ayant ecrit l'annonce. 
def update_frame(dataframe):
    periodic = dataframe.Folder.iloc[0]
    dataframe.loc[:,'Beginning'] = dataframe['Text'].map(lambda text: start_asanad(text,periodic))
    dataframe.loc[:,'End'] = dataframe['Text'].map(lambda text: end_asanad(text,periodic))
    
    
    
# =============================================================================
#     #la partie ci-dessous permet d'inclure les annonces dont le point de fin a été mal OCRisé mais dont on est sur 
#     #que c'est une annonce car elles sont suivis par une autre annonce dans la page.
#     for ind,row in dataframe.iterrows():
#         if row.Beginning and (not row.End) and dataframe.index.shape[0]> ind+5:
#             #la partie ci-dessous permet d'inclure les annonces dont le point de fin a été mal OCRisé mais dont on est sur 
#             #que c'est une annonce car elles sont suivis par une autre annonce dans la page.
#             if dataframe['Beginning'].loc[ind+1]:
#                 row.End = True
#             #Ici on tente d'inclure le reste d'une annonce coupé en deux ou trois si les annonces qui suivents dans le tableau on la bonne forme
#             elif any([dataframe['Adwidth'].loc[ind+i]  for i in range(1,4)]):
#                 for i in range(1,4):
#                     if dataframe['Beginning'].loc[ind+i]:
#                         break 
#                     elif dataframe['Adwidth'].loc[ind+i] and dataframe['Adthickness'].loc[ind+i]:
#                         row.Text += dataframe.Text.loc[ind+i]
#                         row.End = True 
# =============================================================================
                        
    
    
    len_before = dataframe.shape[0]
    dataframe=has_anad_shape(dataframe)
    dataframe = dataframe[dataframe.Adthickness & dataframe.Adwidth]
    dataframe = dataframe[dataframe.Beginning & dataframe.End]
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car elles ne presentaient pas la structure type".format(lost))
    dataframe.Text = dataframe.Text.map(lambda text:clean_text(text,dataframe.Titre.iloc[0]))
    dataframe= Slice_Ad(dataframe)
    dataframe = update_sex(dataframe)
    dataframe = update_age(dataframe)
    
    return dataframe.drop(columns =['Beginning','End','Adthickness','Adwidth','Date','Titre','Publisher']).drop_duplicates(['Text'])
# =============================================================================
# 
# def update_sex(dataframe):
#     dataframe.loc[:,'Sex'] = ''
#     sex = ''
#     countm=0
#     countw=0
#     if dataframe.Folder.iloc[0] == 'LI':
#         for ind,row in dataframe.iterrows():
#             if 'pour les messieurs' in remove_punc(row['Text']).lower():
#                 sex = 'Woman' 
#                 countm +=1
#             elif 'pour les dames' in remove_punc(row['Text']).lower():
#                 sex = 'Man'
#                 countw +=1
#             dataframe.loc[ind,'Sex'] = sex
#         print('m ={0}'.format(countm))
#         print('w ={0}'.format(countw))
#     elif dataframe.Folder.iloc[0] == 'LTDU':
#         for ind,row in dataframe.iterrows():
#             if 'offres aux dames' in remove_punc(row['Text']).lower():
#                 sex = 'Man' 
#                 countm +=1
#             elif 'offres aux messieurs' in remove_punc(row['Text']).lower():
#                 sex = 'Woman'
#                 countw +=1
#             dataframe.loc[ind,'Sex'] = sex
#         print('m ={0}'.format(countm))
#         print('w ={0}'.format(countw))
#     elif dataframe.Folder.iloc[0] == 'LLDM':
#         for ind,row in dataframe.iterrows():
#             if 'demoiselles et veuves' in remove_punc(row['Text']).lower() or 'offres en mariages' in remove_punc(row['Text']).lower():
#                 sex = 'Woman' 
#                 countm +=1
#             elif 'garçons et veufs' in remove_punc(row['Text']).lower() or 'offres en mariages' in remove_punc(row['Text']).lower():
#                 sex = 'Man'
#                 countw +=1
#             dataframe.loc[ind,'Sex'] = sex
#         print('m ={0}'.format(countm))
#         print('w ={0}'.format(countw))
#     elif dataframe.Folder.iloc[0] == 'RV':
#         sex=''
#     return dataframe
# =============================================================================

def update_sex(dataframe):
    len_before = dataframe.shape[0]
    dataframe.loc[:,'Sex'] = (dataframe.Self +'----'+ dataframe.Other).map(lambda text : extract_sex(text))
    dataframe = dataframe[dataframe.Sex != 'C och']
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car le sexe n'a pas pu être extrait".format(lost))
    return dataframe

#crée une colonne pour l'age
def update_age(dataframe):   
    dataframe.loc[:,'Age'] = dataframe.Self.map(lambda text: extract_age(text))#on prends les 2 digit avant le mot "ans"
    len_before = dataframe.shape[0]
    dataframe = dataframe[dataframe['Age'] != 0 ]
    lost = len_before-dataframe.shape[0]
    print("{0} annonces ont été perdu car l'age n'a pas pu être extrait".format(lost))
    return dataframe

#Convertire la collone 'text', qui est en string, en doc spacy
def Text_to_Spacy(dataframe):    
    dataframe.loc[:,'SpacyDoc'] = dataframe.Text.map(lambda x : nlp(x))
    return dataframe 
    
def Slice_Ad(dataframe):
    Separation_Words = ['désire','épouserait','contracterait',\
                    'recherche','épouser','marierait','dévouerait',\
                    'mariage', 'correspondrait','accepterait',"s'unirait"]
    #on enlève les adds qui ne contienne pas les mots de séparation
    len_before = dataframe.shape[0]
    dataframe=dataframe[dataframe.Text.apply(lambda text : True in [word in text.lower() for word in Separation_Words])]
    dataframe=dataframe
    dataframe.loc[:,'Slices'] = dataframe.Text.map(lambda text : separate_in_two(text))
    dataframe.loc[:,'Self'] = dataframe['Slices'].map(lambda slices : slices[0])
    dataframe.loc[:,'Other'] = dataframe['Slices'].map(lambda slices : slices[1])
    dataframe[dataframe.Self.apply(lambda text : text != '')]
    lost =len_before-dataframe.shape[0]
    print('{0} annonces ont été perdu faute de mot de séparation'.format(lost))
    return dataframe.drop(columns=['Slices'])

#FONCTION UTILITAIRE
    
#retourne la portion d'image correspondante à la page et au coordonnée donné
def paragraph_image(path,coord):
    image = PIL.Image.open(path)
    paragraph_image = image.crop(coord)
    return paragraph_image
#pour un index donné, retourne l'image du texte
def show_image(index,dataframe):
    display(paragraph_image(dataframe.Folder.loc[index]+'/image/'+dataframe.Image_file.loc[index],dataframe.Coordinates.loc[index]))
    
#enleve la ponctuation d'un string  
def remove_punc(text):
    poncs ='!@#$%^&*_+={}[]:;"\|<>,.?/~`'
    for ponc in poncs:
        text=text.replace(ponc,'')
    return text    
    
def clean_text(text,periodic):
    #enlève les charactères qui sont de trop
    text = text.lower()
    expr_to_replace_by_space = ['\n',' i ','.—','.-','. -','. —','—']
    expr_to_remove = ['¬\n','¬', '*','■','•','!','?','/','<','>','^',] #j'ai check, tous les '!' et '?' sont des erreurs d'OCR (intéressant)
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
            
    #liste de mot mal ocrisé à enelevé des textes
    spell_error = list(pd.read_csv('Homemade/err',header=None)[0].map(lambda x : x.replace('[','').replace(']','').replace("'",'').replace('"','').split(', ')))
    followed_char = ' .,'
    for char in followed_char:
        for error in spell_error:
            err = error[0] + char
            correction = error[1]+char
            if err in text:
                text = text.replace(err,correction)
    text = ' '.join(text.split())
    
    if periodic == 'RV':
        text = 'N'.join(text.split('N')[:-1])
        
    
    return text

def remove_digit (text):
    new_text = ''
    for char in text:
        if not char.isdigit():
            new_text += char 
    if new_text == '':
        return None 
    else: 
        return new_text
    
def tokenizeponct(text):
    #renvoie une liste des groupes de mot séparé par la ponctuation du text
   
    ponc = '.,!?'
    if ~(text[-1] in ponc):
        text += '.' #permet à l'algo de considérer le dernier groupe de mots
    words_group =[]
    pos_group=[]
    for pos,char in enumerate(text):
        if char not in ponc:
            pos_group.append(pos)
        elif  pos_group:
            if pos_group[0] < pos:
                words_group.append(text[pos_group[0]:pos])
                pos_group=[]
    words_group = [remove_digit(words_group[0])] + words_group[1:]
    words_group = [x for x in words_group if (x != [] and x != [None] and x != None)]
        
    return words_group

def tokenizeword(text):
    return [remove_punc(word) for word in text.lower().split()]
    


def extract_number(text):
    number = ''
    for char in text:
        if char.isdigit():
            number+=char
        if len(number) >1: #a priori on a pas de centenaire dans les annonces... 
            break
    if number == '':
        return None
    else: 
        return int(number)
    
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
        if intage>15 and intage<79:
            return intage
        else :
            return 0
    else :
        return 0
 
#Compare le nombre de mot au feminin par rapport au nombre de mot au masculin pour définir le sex de l'annonce
def extract_sex(text):
# =============================================================================
#     text = text.split('----')
#     doc=nlp(text[0])# on étudie le sex apparant dans la description de soi
#     
#     neutral_words = ['fortune','famille','argent','mobilier','commerce','an','enfant',\
#                      'célibataire','économie','obligation','immeuble','bureau','institution',\
#                      'fond','âge','rapport','franc','industrie','propriété','religion',\
#                      'espérance','santé','situation','honorable','goûts', 'honnête', 'taille',\
#                      'moyenne', 'bonne', 'distinction', 'foyer', 'relation', 'ville'] # list de mot dont le genre n'indique par le genre de l'annonceur
#     doc = [tok for tok in doc if not tok.is_stop and tok.is_alpha and not tok.is_oov and tok.text not in neutral_words]
#     for i,word in enumerate(neutral_words):
#         neutral_words[i]= word+'s' #on oublie pas les pluriels
#         if word[-3:] == 'eau':
#             neutral_words[i]= word+'x'
#     doc = [tok for tok in doc if tok.text not in neutral_words ] #on eneleve les pluriels 
#     mselfcounter = 0 # counter pour le nombre de mot masculin
#     fselfcounter = 0 # counter pour le nombe de mot feminin
#     for tok in doc:
#             if 'Gender=Fem' in tok.tag_:
#                 fselfcounter +=1
#             elif 'Gender=Masc' in tok.tag_:
#                 mselfcounter +=1
#     if fselfcounter>0 or mselfcounter >0:
#         self_testosterone=mselfcounter/(mselfcounter+fselfcounter )
#     else :
#         self_testosterone=0.5
#         
#         
#     doc=nlp(text[1])# on étudie le sex apparant dans la description de la personne recherché
#     doc = [tok for tok in doc if not tok.is_stop and tok.is_alpha and not tok.is_oov and tok.text not in neutral_words]
#     neutral_words = ['fortune','famille','argent','mobilier','commerce','an','enfant',\
#                      'célibataire','économie','obligation','immeuble','bureau','institution',\
#                      'fond','âge','rapport','franc','industrie','propriété','religion',\
#                      'espérance','santé','situation','honorable','goûts', 'honnête', 'taille',\
#                      'moyenne', 'bonne', 'distinction', 'foyer', 'relation', 'ville']
#     doc = [tok for tok in doc if tok.text not in neutral_words ]
#     mselfcounter = 0 # counter pour le nombre de mot masculin
#     fselfcounter = 0 # counter pour le nombe de mot feminin
#     for tok in doc: # la présence de mot masculin dans la partie recherche indique que l'auteur est une femme et vice-versa
#             if 'Gender=Fem' in tok.tag_:
#                 mselfcounter +=1
#             elif 'Gender=Masc' in tok.tag_:
#                 fselfcounter +=1
#     if fselfcounter>0 or mselfcounter >0:
#         other_testosterone=mselfcounter/(mselfcounter+fselfcounter )   
#     else :
#         other_testosterone=0.5
#     del neutral_words
#             
#     if self_testosterone+other_testosterone > 1:
#         return 'Man'
#     else:
#         return 'Woman'
# =============================================================================
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
    masculinity = sex_score(Self,masc_words)+sex_score(Other,fem_words)
    feminity = sex_score(Self,fem_words)+sex_score(Other,masc_words)
    if masculinity == feminity:
        return 'C och'
    elif masculinity > feminity:
        return 'Man'
    else:
        return 'Woman'
    
    
def sex_score(text,word_list):
    for p in string.punctuation:
        text = text.replace(p,' ')
    words = [ word for word in text.lower().split() if len(word) > 2 ]
    score = 0
    for word in words:
        if word in word_list:
            score +=1
    return score
    
        

def separate_in_two(text):
    Separation_Words = [' désire ',' épouserait ',' contracterait ','désirerait'\
                    ' recherche ',' épouser ',' marierait ',' dévouerait ',\
                    ' demande ', ' correspondrait ',' accepterait '," s'unirait ",]
    #TO DO, faire la liste des mots de séparation apparaissant dans le texte, garder celui qui apparait le premier et splitter autour de ce mot
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


def Full_Token_List(dataframe):
    Token_Serie = dataframe.SpacyDoc.explode().copy()
    Pos_Serie=Token_Serie.map(lambda tok :tok.pos_)
    Token_df =pd.DataFrame()
    Token_df.loc[:,'Token']=Token_Serie
    return Token_df

#Retourne True si le début du texte présente la structure type d'une annonce sauf pour 'RV' ou l'on considère la fin de l'annonce
def start_asanad(text, periodic):
    if periodic == 'RV':
        if ('. —' in text[-13:] or '. -' in text[-13:] or '.-' in text[-13:] or '.—' in text[-13:])\
           and (any(c.isdigit() for c in text[-6:]) or any(c=='N' for c in text[-9:])):
            return True
        else:
            return False
    else: 
        #return true if the the 10 firsts letters of text have '. —' in it and if the first 4 letters contain a digit 
        if any(c.isdigit() for c in text[:4]):
            if '. —' in text[:10] or '. -' in text[:10] or '.-' in text[:10] or '.—' in text[:10]:
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
        if any(c=='.' for c in text[-3:]):
            return True 
        else:
            return False
#retourne le dataframe avec uniquement les annonces ayant la dimension d'une annonce
def has_anad_shape(dataframe):
    periodic = dataframe.Folder.iloc[0]
    #boundary of ad width and thickness in pixel for each periodic
    LLDM = [1080,1190,45,330]
    LI = [835,920,45,736]
    RV = [870,950,50,300]
    LTDU = [1025,1055,200,661]
    
    if periodic == 'LLDM':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LLDM[0] and coord[2]-coord[0] < LLDM[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LLDM[2] and coord[3]-coord[1] < LLDM[3] )
    elif periodic == 'LI':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LI[0] and coord[2]-coord[0] < LI[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LI[2] and coord[3]-coord[1] < LI[3] )
    elif periodic == 'LTDU':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LTDU[0] and coord[2]-coord[0] < LTDU[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LTDU[2] and coord[3]-coord[1] < LTDU[3] )
    elif periodic == 'RV':
        dataframe.loc[:,'Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >RV[0] and coord[2]-coord[0] < RV[1] )
        dataframe.loc[:,'Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >RV[2] and coord[3]-coord[1] < RV[3] )
        
    
    return dataframe 
            
        
def store_to_folder(dataframe, file_name = 'dataframe'):
    dataframe.to_csv(dataframe.Folder.iloc[0]+'/' +file_name,index=False)
    
def read_from_folder(folder,file_name = 'dataframefull'):
    dataframe = pd.read_csv(folder + '/' + file_name)
    dataframe.Coordinates = dataframe.Coordinates.map(lambda coord : eval(coord))
    return dataframe

def Ngrams(dataframe, column, N):
    Ngrams_List= []
    Word_List=[]
    for ind,row in dataframe.iterrows():
        lemmas = []
        for word in row[column]:
            if not word.is_stop and word.is_alpha and not word.is_oov:
                lemmas.append(word.lemma_.lower())
        
        for ind,lemma in enumerate(lemmas):
            Nword=[]
            if ind + N < len(lemmas):
                for i in range(N):
                    Nword.append(lemmas[ind+i])
                Ngrams_List.append(' '.join(Nword))
            if len(lemma) > 2 :
                Word_List.append(lemma)
            
    n_ngram = len(Ngrams_List)
    n_word = len(Word_List)
    Ngrams = pd.Series(Ngrams_List).value_counts()
    
    Word = pd.Series(Word_List).value_counts()
   
    return pd.concat([Ngrams.map(lambda x : float(x/n_word)) , Word.map(lambda x : float(x/n_word))]).sort_values(ascending=False)

#def Knn_Ad_segment(SpacySeries):
    
        
# =============================================================================
# for index, row in wordandcorr.iterrows():
#     
#     
#     #display(wordandcorr.loc[index, 'text'])
#     if not row['done']:
#         clear_output()
#         print(index)
#         print(row['text'])
#         action = input('What to do?\n c : copy, l : lower and copy, r : rewrite in list, i : ignore, else : leave \n')
#         if action == 'c':
#             wordandcorr.list.loc[index] = row['text']
#         elif action == 'l':
#             wordandcorr.list.loc[index] = row['text'].lower()
#         elif action == 'r':
#             List = input("separate list by ', '") 
#             wordandcorr.list.loc[index] = List
#             if len(List.split(', ')) != len(List.split(',')):
#                 print('format error')
#                 break
#             val = input('Error?\n y : yes, n : no')
#             if val == 'n':
#                 print('ok nice')
#             elif val == 'e':
#                 break
#             else:
#                 err = input(" error format :  err1, corr1, err2, corr2")
#                 if err == 'e':
#                     break
#                 wordandcorr.err.loc[index] = err 
#         elif action == 'i':
#             print('ok')
#         else :
#             break
#             
#             
#         wordandcorr.done.loc[index] = True
#         wordandcorr.to_csv('yes',index=False)
# 
# =============================================================================
    