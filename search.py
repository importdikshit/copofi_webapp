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
		print(top_k)

		
		return "Done"

if __name__ == '__main__':
   app.run(debug=True)


