from flask import Flask, redirect, url_for, request, render_template
import re
from closest_postcodes import k_closest_postcodes
import pandas as pd

app = Flask(__name__)

def process_postcode(code):
	"""Convert in the same format as in zips_coordinates.csv"""
	#Step 1: convert all to upper and remove spaces
	code = code.upper()
	code = re.sub(" ", "", code)
	#Step 2: Add a space between first 3 and last 3 letters
	code = code[:3]+" "+code[3:]
	return code

@app.route('/', methods=['GET', 'POST'])
def search():
	if request.method=='GET':
		return render_template('search.html')
	elif request.method=='POST':
		post_code  = request.form['post_code']
		
		#process the post_code to put in the right format
		post_code = process_postcode(post_code)

		#get the k closest post codes
		k=10
		top_k = k_closest_postcodes(k, post_code)
		#Remove space from all post codes upon Ishan's request
		top_k = [re.sub(" ", "", i) for i in top_k]
		print(top_k)

		return redirect(url_for('dashboard'))

@app.route('/main')
def dashboard():
	return render_template('main.html')

@app.route('/viz1')
def vis1():
	return "Viz1"

@app.route('/viz2')
def vis2():
	return "Viz2"

@app.route('/viz3')
def vis3():
	return "Viz3"

@app.route('/viz4')
def vis4():
	return "Viz4"

@app.route('/viz5')
def vis5():
	return "Viz5"


if __name__ == '__main__':
   app.run(debug=True)


