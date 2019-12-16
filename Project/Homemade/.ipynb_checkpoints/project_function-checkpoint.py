
import pandas as pd
import numpy as np
import os
from pathlib import Path #Good pathmanagment across different systems (Mac/Windows...)
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from requests import get
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm
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
        
        date =meta_xml_parsed.find('dc:date')
        titre =meta_xml_parsed.find('dc:title')
        publisher = meta_xml_parsed.find('dc:publisher')
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
    dataframe['Beginning'] = dataframe['Text'].map(lambda text: start_asanad(text,periodic))
    dataframe['End'] = dataframe['Text'].map(lambda text: end_asanad(text,periodic))

    dataframe=has_anad_shape(dataframe)
    #la partie ci-dessous permet d'inclure les annonces dont le point de fin a été mal OCRisé mais dont on est sur 
    #que c'est une annonce car elles sont suivis par une autre annonce dans la page.
    for ind,row in dataframe.iterrows():
        if row.Beginning and (not row.End) and dataframe.index.shape[0]> ind+5:
            #la partie ci-dessous permet d'inclure les annonces dont le point de fin a été mal OCRisé mais dont on est sur 
            #que c'est une annonce car elles sont suivis par une autre annonce dans la page.
            if dataframe['Beginning'][ind+1]:
                row.End = True
            #Ici on tente d'inclure le reste d'une annonce coupé en deux ou trois si les annonces qui suivents dans le tableau on la bonne forme
            elif any([dataframe['Adwidth'][ind+i]  for i in range(1,4)]):
                for i in range(1,4):
                    if dataframe['Beginning'][ind+i]:
                        break 
                    elif dataframe['Adwidth'][ind+i] and dataframe['Adthickness'][ind+i]:
                        row.Text += dataframe.Text[ind+i]
                        row.End = True 
            
    print(dataframe[dataframe['Beginning']].shape) #avant de drop les duplicates
    dataframe = update_sex(dataframe)
    dataframe1 = dataframe[dataframe.Beginning].copy()
    dataframe1 = dataframe1[dataframe1.End]
    dataframe1.Text = dataframe1.Text.map(lambda text:clean_text(text))
    return dataframe1.drop(columns =['Beginning','End','Adthickness','Adwidth']).drop_duplicates(['Text'])

def update_sex(dataframe):
    dataframe['Sex'] = ''
    sex = ''
    countm=0
    countw=0
    if dataframe.Folder.iloc[0] == 'LI':
        for ind,row in dataframe.iterrows():
            if 'pour les messieurs' in remove_punc(row['Text']).lower():
                sex = 'Woman' 
                countm +=1
            elif 'pour les dames' in remove_punc(row['Text']).lower():
                sex = 'Man'
                countw +=1
            dataframe.loc[ind,'Sex'] = sex
        print('m ={0}'.format(countm))
        print('w ={0}'.format(countw))
    elif dataframe.Folder.iloc[0] == 'LTDU':
        for ind,row in dataframe.iterrows():
            if 'offres aux dames' in remove_punc(row['Text']).lower():
                sex = 'Man' 
                countm +=1
            elif 'offres aux messieurs' in remove_punc(row['Text']).lower():
                sex = 'Woman'
                countw +=1
            dataframe.loc[ind,'Sex'] = sex
        print('m ={0}'.format(countm))
        print('w ={0}'.format(countw))
    elif dataframe.Folder.iloc[0] == 'LLDM':
        for ind,row in dataframe.iterrows():
            if 'demoiselles et veuves' in remove_punc(row['Text']).lower() or 'offres en mariages' in remove_punc(row['Text']).lower():
                sex = 'Woman' 
                countm +=1
            elif 'garçons et veufs' in remove_punc(row['Text']).lower() or 'offres en mariages' in remove_punc(row['Text']).lower():
                sex = 'Man'
                countw +=1
            dataframe.loc[ind,'Sex'] = sex
        print('m ={0}'.format(countm))
        print('w ={0}'.format(countw))
    elif dataframe.Folder.iloc[0] == 'RV':
        sex=''
    return dataframe

#crée une colonne pour l'age
def update_age(dataframe):
    dataframe['Wordgroup']= dataframe.Text.map(lambda text: tokenizeponct(text))            
    dataframe['Age'] = dataframe['Wordgroup'].map(lambda groups: extract_age(groups[:3]))#on prends les digit des 2 premiers groupes de mot pour definir l'age
    #dataframe['Situation'] = dataframe['Wordgroup'].map(lambda groups: groups[0]) # retourne le mot ou groupe de mot mis en évidence au début d'ad
    return dataframe.drop(columns=['Wordgroup'])

#Convertire la collone 'text', qui est en string, en doc spacy
def Text_to_Spacy(dataframe):    
    dataframe['SpacyDoc'] = dataframe.Text.map(lambda x : nlp(x))
    return dataframe 
    
def Slice_Ad(dataframe):
    Separation_Words = ['désire','desire','épouserait','epouserait','contracterait',\
                        'recherche','cherche','épouser','epouser']#mot marquant la séparation des deux parties 
    #on enlève les adds qui ne contienne pas les mots de séparation
    dataframe=dataframe[dataframe.Text.apply(lambda text : True in [word in text.lower() for word in Separation_Words])]
    dataframe1=dataframe.copy()
    dataframe1['Slices'] = dataframe1.SpacyDoc.map(lambda doc : separate_in_two_LI(doc))
    dataframe1['SelfDescription'] = dataframe1['Slices'].map(lambda slices : slices[0])
    dataframe1['OtherDescription'] = dataframe1['Slices'].map(lambda slices : slices[1])
    return dataframe1.drop(columns=['Slices'])

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
    
def clean_text(text):
    #enlève les charactères qui sont de trop
    expr_to_replace_by_space = ['\n',' i ','.—','.-','. -','. —']
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
            
    #enleve les mots souvents mal OCRisé
    spell_error = [[' tille',' fille'],[' bran',' brun'],[' dama', ' dame'],[" bel'e"," belle"],\
                   [" fi's"," fils"]]#important de garder les espaces au début.
    followed_char = ' .,'
    for char in followed_char:
        for error in spell_error:
            err = error[0] + char
            correction = error[1]+char
            if err in text:
                text = text.replace(err,correction)
            
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
    
def extract_age(wordgroups):
    wordgroup =''.join(wordgroups)
    if 'ans' in wordgroup.lower():
        return extract_number(''.join(wordgroups))
    else:
        return None
    

def separate_in_two_LI(doc):
    Separation_Words_LI = ['désire','desire','épouserait','epouserait','contracterait',\
                        'recherche','cherche','épouser','epouser']#mot marquant la séparation des deux parties 
    for tok in doc:
        if True in [word in tok.text.lower()  for word in Separation_Words_LI]:
            return [doc[:tok.i],doc[tok.i:]]
    empty=nlp('')
    return [empty,empty]

def Full_Token_List(dataframe):
    Token_Serie = dataframe.SpacyDoc.explode().copy()
    Pos_Serie=Token_Serie.map(lambda tok :tok.pos_)
    Token_df =pd.DataFrame()
    Token_df['Token']=Token_Serie
    return Token_df

#Retourne True si le début du texte présente la structure type d'une annonce sauf pour 'RV' ou l'on considère la fin de l'annonce
def start_asanad(text, periodic):
    
    if periodic == 'RV':
        if ('. —' in text[-13:] or '. -' in text[-13:] or '.-' in text[-13:] or '.—' in text[-13:])\
           and (any(c.isdigit() for c in text[-6:]) or any(c=='N' for c in text[-10:])):
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
    
def end_asanad(text,periodic):
    #retourne True si y'a un point dans les 4 derniers charactères (au cas ou y'a un espace à la fin ou un truc dans le genre), sauf pour 
    #'RV' ou la structure de fin est différente
    if periodic == 'RV':
        if any(c.isupper() for c in text[:4]):
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
        dataframe['Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LLDM[0] or coord[2]-coord[0] < LLDM[1] )
        dataframe['Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LLDM[2] or coord[3]-coord[1] < LLDM[3] )
    elif periodic == 'LI':
        dataframe['Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LI[0] or coord[2]-coord[0] < LI[1] )
        dataframe['Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LI[2] or coord[3]-coord[1] < LI[3] )
    elif periodic == 'LTDU':
        dataframe['Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >LTDU[0] or coord[2]-coord[0] < LTDU[1] )
        dataframe['Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LTDU[2] or coord[3]-coord[1] < LTDU[3] )
    elif periodic == 'RV':
        dataframe['Adwidth']=dataframe.Coordinates.map(lambda coord : coord[2]-coord[0] >RV[0] or coord[2]-coord[0] < RV[1] )
        dataframe['Adthickness']=dataframe.Coordinates.map(lambda coord : coord[3]-coord[1] >LTDU[2] or coord[3]-coord[1] < LTDU[3] )
        
    
    return dataframe 
            
        
def store_to_folder(dataframe, file_name = 'dataframe'):
    dataframe.to_csv(dataframe.Folder.iloc[0]+'/' +file_name,index=False)
    
def read_from_folder(folder,file_name = 'dataframefull'):
    dataframe = pd.read_csv(folder + '/' + file_name)
    dataframe.Coordinates = dataframe.Coordinates.map(lambda coord : eval(coord))
    return dataframe

    