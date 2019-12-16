from flask import Flask
from flask import send_file, request, redirect, url_for
from flask import render_template
import os
import numpy as np
import pandas as pd
from static.python import match

app = Flask(__name__)

Matches = pd.read_json('./static/python/matches_final.json')

@app.route('/')
def accueil():
    return render_template('index.html')
	
@app.route('/trouver_lamour')
def trouver_lamour():
    return render_template('search.html')

@app.route('/trouver_lamour', methods=['POST'])
def find_results():

    ### Dictionary creation ###
    tags = [request.form.get('tag' + str(i)) for i in range(17) if request.form.get('tag' + str(i))!= None]
    description = {
        'sex': request.form.get('gender'),
        'age': [int(request.form.get('age_min')),int(request.form.get('age_max'))],
        'tags': tags,}
    ############################

    ### Getting a match ###
    result = match.get_info(Matches, match.match(Matches, description))
    #######################

    ### Preparing the images and the data###
    if result != None:
        result['cover'].save('./static/python/temp_images/cover.jpg')
        result['page'].save('./static/python/temp_images/page.jpg')
        result['paragraph'].save('./static/python/temp_images/paragraph.jpg')
        if result['soulmate']:
            result['competition'].save('./static/python/temp_images/competition.jpg')
    ############################

    return render_template('results.html', result = result)




if __name__ == '__main__':
    app.run(debug=True)