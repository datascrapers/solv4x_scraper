import requests
import pandas as pd
from pathlib import Path #Used for CSV Creation
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Store Api Key Into a Variable
api_key = 'L08gD6TFlLdYhl1sJKagbCVA5AJmcjCVOlWbUEdz'

# Series Id(PADD_KEY). This identifies the specific data we are targeting. Find this key in the API documents.
PADD_NAMES = ['Demand Forecast', 'Solar Generation', 'Wind Generation']
PADD_KEY = ['EBA.CAL-ALL.DF.H ','EBA.CAL-ALL.NG.SUN.H','EBA.CAL-ALL.NG.WND.H']

padd_count = 0 #Indexes what Padd we are currently on


#here we are creating a list to store the json responses for each iteration of the loop
jsons = []



# Pull in data via EIA API and add them to a pandas dataframe
for i in range(len(PADD_KEY)): #This loop makes one call to the api per iteration to the current PADD_KEY at PADD_KEY[i] and inserts the data into a column in the dataframe

    #Specific Series Call (This is the address to where the data is stored in the api)
    url = 'https://api.eia.gov/series/?api_key=' + api_key + '&series_id=' + PADD_KEY[i]+"&num=24"

    #Here we are passing in the url to requests.get() and saving the response to the variable r
    r = requests.get(url)

    #here we are storing the response (r) to json format
    json_data = r.json()

    #here we append the current response to the list of jsons
    jsons.append(json_data)
    
    #this checks for an error in the response
    if r.status_code == 200:
        print('Success!')
    else:
        print('Error')
    
    #On the first iteration we create the data frame (padd_count == 0)
    if padd_count == 0:
      df = pd.DataFrame(json_data.get('series')[0].get('data'),
                    columns = ['Date', PADD_NAMES[0]])
      # df.set_index('Date', drop=True, inplace=True)

    #If not on the first iteration we add a column to the data frame.
    else:
      #Selects current json
      next_padd = jsons[padd_count]
      #selects the "data" key in the json
      next_padd_data = list(next_padd.get('series')[0].get('data'))
      #creates list to store the data from the json
      next_padd_datalist = []
      #adds the data to the list
      for i in range(0,len(next_padd_data)):
        next_padd_datalist.append(next_padd_data[i][1])
      #Adds list to the column in the dataframe
      df[PADD_NAMES[padd_count]] = next_padd_datalist

    #increment to next padd
    padd_count += 1 

#prints out dataframe
print(df)


#createt csv 
filepath = Path('/Users/thomas/Documents/EIAScraper/EnergyDataFrameToCSV.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)  
df.to_csv(filepath)



#Initialize the database credentials
cred = credentials.Certificate("/content/serviceAccountKey.json")
firebase_admin.initialize_app(cred)

#Database Connection
db = firestore.client()

#Add Data From the Dataframe to the Database

for index, row in df.iterrows():

  db.collection('California Renewable Energy').document(row['Date']).add({'Date' : str(row['Date']),'Demand Forecast' : int(row['Demand Forecast']), 'Solar Generation' : int(row['Solar Generation']), 'Wind Generation' : int(row['Wind Generation'])})