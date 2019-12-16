import pandas as pd
from PIL import Image

def get_first_page(Ark, folder):
    path = './static/python/' + folder + '/image/' + Ark + '_001.jpg'
    image = Image.open(path)
    return image

def cover(dataframe, index):
    Ark = dataframe.loc[index, 'Ark']
    folder = dataframe.loc[index, 'Folder']
    return get_first_page(Ark, folder)

def page(dataframe, index):
    path = './static/python/' + dataframe.Folder.loc[index]+'/image/'+dataframe.Image_file.loc[index]
    image = Image.open(path)
    return image

def paragraph(dataframe, index):
    image = page(dataframe, index)
    coord = dataframe.Coordinates.loc[index][0]
    paragraph_image = image.crop(coord)
    return paragraph_image