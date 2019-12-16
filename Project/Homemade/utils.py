import PIL
from IPython.display import display
import pandas as pd

def store_to_folder(dataframe, file_name = 'dataframe'):
    dataframe.to_json(dataframe.Folder.iloc[0]+'/' +file_name)
    
def read_from_folder(folder,file_name = 'dataframefull'):
    dataframe = pd.read_json(folder + '/' + file_name)
    #dataframe.Coordinates = dataframe.Coordinates.map(lambda coord : eval(coord))
    return dataframe

#retourne la portion d'image correspondante à la page et au coordonnée donné
def paragraph_image(path,coord):
    image = PIL.Image.open(path)
    paragraph_image = image.crop(coord)
    display(paragraph_image)
    image.close()

#pour un index donné, retourne l'image du texte de l'annonce
def show_image(index,dataframe):
    path = dataframe.Folder.loc[index]+'/image/'+dataframe.Image_file.loc[index]
    coords = dataframe.Coordinates.loc[index]
    for coord in coords:
        paragraph_image(path,coord)

#pour un index donnée, affiche l'auteur et son match si il existe
def show_couple(dataframe,index):
    show_image(index,dataframe)
    if type(dataframe.loc[index,'SoulMate']) == str:
        show_image(dataframe.loc[index,'SoulMate'],dataframe)