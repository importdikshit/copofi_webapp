from flask import Flask, redirect, url_for, request, render_template, flash
import re
import numpy as np
from closest_postcodes import k_closest_postcodes, get_zip_coords
from API.airbnb_api import *
import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings
import json
warnings.filterwarnings('ignore')

app = Flask(__name__)
app.secret_key = 'some_secret'

key = "AIzaSyDm4Ft5drUeDbn3FD4Rg3_jhfZOEN4QSrE" #Google Maps Geocoding API key
url1 = "https://maps.googleapis.com/maps/api/geocode/json?address="
url2 = "W&key="


@app.route('/', methods=['GET', 'POST'])
def search():
	if request.method=='GET':
		return render_template('search.html')
	elif request.method=='POST':
		address  = request.form['place']

	if address.isupper() and " " not in address:
		#it is a zipcode
		lat, lon = get_zip_coords(address)
	else:
		#Get the coordinates of the place we are looking for the given location
		url = url1+address+url2+key
		try:
			google_results = requests.get(url).json()['results'][0]['geometry']['location']
		except IndexError as e:
			print(requests.get(url).json())
			flash("An error occured. Try another area or try to be more specific!")
			return render_template("search.html")
		
		lat = google_results['lat']
		lon = google_results['lng']

	#Run knn with the above latitude and longitude
	k=10
	top_k = k_closest_postcodes(k, lat, lon)

	if len(top_k)==0:
		try:
			raise ValueError
		except ValueError:
			print(requests.get(url).json())
			flash("No properties found in that area! Try another area or try to be more specific!")
			return render_template("search.html")
	else:
		print("---------------------------------------")
		print("Top 10 ZIPS: ",top_k)
		print("---------------------------------------")
		closest_zip = top_k[0]
		print("Closest ZIPCODE: ", closest_zip)
		print("---------------------------------------")

		# # #Query Airbnb for all the properties in that zipcode
		# overview_data = overview(closest_zip+"-London")
		# room_ids = overview_data[2]
		# # host_id = overview_data[3]

		# # # Get df for room_ids
		# rooms_df = rooms(room_ids)
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


