import requests
import pandas as pd
from pandas import json_normalize
import json
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import datetime
from datetime import date

#----------------------------------------------------------------------------------------------------------------------------------------
#Add your SQL Server database parameters before running
ServerName = "Your server name"
DatbaseName = "Your database name"
#----------------------------------------------------------------------------------------------------------------------------------------

class cityDictionary:
    def __init__(self):
        self.coordinatesByCity = [
            {"city":"Cleveland", "latitude":"41.4995", "longitude":"-81.6954"},
            {"city":"Akron", "latitude":"41.0817", "longitude":"-81.5114"},
            {"city":"Youngstown", "latitude":"41.1035", "longitude":"-80.6520"},
            {"city":"Toledo", "latitude":"41.6510", "longitude":"-83.5419"}
        ]

class TemperatureAPI:
    def __init__(self, latitude, longitude, startDate, endDate):
        self.latitude = latitude
        self.longitude = longitude
        self.startDate = startDate
        self.endDate = endDate
        self.temperatureUrl = "https://archive-api.open-meteo.com/v1/archive?latitude=" + latitude + "&longitude=" + longitude + "&start_date=" + startDate + "&end_date=" + endDate + "&hourly=temperature_2m&timezone=America%2FNew_York&temperature_unit=fahrenheit"

    def APICall(self):
        try:
            jsonResponse = requests.get(self.temperatureUrl, timeout=30)
            jsonResponse.raise_for_status()
            response = json.loads(jsonResponse.text)

            if "hourly" not in response:
                print("There was an error processing data for " + self.latitude + ", " + self.longitude + " " + " and it will be skipped.")
                return pd.DataFrame() #Safe exit
            elif not response["hourly"]:
                print("There was no temperature data available for " + self.latitude + ", " + self.longitude + " " + " and it will be skipped.")
                return pd.DataFrame() #Safe exit

            dfLatitude = response["latitude"]
            dfLongitude = response["longitude"]
            dfElevation = response["elevation"]
            dfContent = pd.DataFrame(response["hourly"])
            dfContent["latitude"] = dfLatitude
            dfContent["longitude"] = dfLongitude
            dfContent["elevation"] = dfElevation
            dfContent.rename(columns={"time": "Timestamp"}, inplace=True)
            dfContent.rename(columns={"temperature_2m": "Temperature"}, inplace=True)
            dfContent["Timestamp"] = pd.to_datetime(dfContent["Timestamp"])
            dfContent["date"] = dfContent["Timestamp"].dt.date
            dfContent["time"] = dfContent["Timestamp"].dt.time
            return dfContent
        
        except requests.exceptions.RequestException as exceptionMessage:
            print("The following error was raised while requesting data for " + self.latitude + ", " + self.longitude + " " + " and it will be skipped.")
            print("Error: " + str(exceptionMessage))
            return pd.DataFrame() #Safe exit
    
class APIResultCompiler:
    def __init__(self,pickedCities, startDate, endDate):
        self.pickedCities = pickedCities
        self.startDate = startDate
        self.endDate = endDate
        self.coordsLookup = cityDictionary()

    def compiler(self):
        dfsToCombine = []
        for city in self.pickedCities:
            latitudeInput = [entry["latitude"] for entry in self.coordsLookup.coordinatesByCity if entry["city"] == city][0]
            longitudeInput = [entry["longitude"] for entry in self.coordsLookup.coordinatesByCity if entry["city"] == city][0]
            myAPICall = TemperatureAPI(latitudeInput,longitudeInput,self.startDate,self.endDate)
            dfCity = myAPICall.APICall()
            dfCity["city"] = city
            dfsToCombine.append(dfCity)
        combinedContent = pd.concat(dfsToCombine,ignore_index=True)
        rowCount = len(combinedContent)
        return combinedContent, rowCount

class TemperatureAnalysis:
    def __init__(self, APIResponse):
        self.APIResponse = APIResponse

    def APIResponsePrinter(self):
        print(self.APIResponse)

    def DataPlotter(self):
        plt.figure(figsize=(10, 4))
        for city, group in self.APIResponse.groupby("city"):
            plt.plot(
                group["Timestamp"],
                group["Temperature"],
                marker="o",
                linestyle="-",
                label=city
        )
        plt.title("Temperature Over Time")
        plt.xlabel("Date/Time")
        plt.ylabel("Temperature (°F)")
        plt.grid(True)
        plt.tight_layout()
        plt.legend()
        plt.show()

class TemperatureETL:
    def __init__(self, APIResponse):
        self.APIResponse = APIResponse

    def SQLServerETL(self):
        engine = create_engine("mssql+pyodbc://@" + ServerName + "/" + DatbaseName + "?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")
        self.APIResponse.to_sql("Temperature", con=engine, if_exists= "append", index=False)

def main():
    masterCityDictionary = cityDictionary()

    citiesToPick = []
    for city in masterCityDictionary.coordinatesByCity:
        citiesToPick.append(city["city"])

    pickedcities = []
    pickedCitiesCount = 0
    nextStep = "Add"

    while len(citiesToPick) > 0 and nextStep != "Done":
        pickedCitiesCount = len(pickedcities)

        noPickedCitiesPrompt = "Pick a city:"
        pickedCitiesPrompt = "Type ""Done"" to proceed with the existing cities or add another:"
        if pickedCitiesCount == 0:
            prompt = noPickedCitiesPrompt
        else:
            prompt = pickedCitiesPrompt
        print(prompt)
        for city in citiesToPick:
            print(city)

        while True:
            response = input(">")
            if response.casefold() in [item.casefold() for item in citiesToPick] or response.casefold() == "Done".casefold():
                break
            else:
                print("That is an invalid response. Try again.")

        if response.casefold() == "Done".casefold():
            nextStep = "Done"
        else:
            pickedCity = next((city for city in citiesToPick if city.casefold() == response.casefold()),None)
            pickedcities.append(pickedCity)
            citiesToPick.remove(pickedCity)

    print("Enter a start date:")
    while True:
        startdate = input(">")
        try:
            startDateValidation = datetime.datetime.strptime(startdate, "%Y-%m-%d")
            if startDateValidation.date() < date.today():
                break
            else:
                print("The date entered must be a previous date. Please try again.")
        except:
            print("The date format entered is invalid. Please try again.")

    print("Enter an end date:")
    while True:
        endDate = input(">")
        try:
            endDateValidation = datetime.datetime.strptime(endDate, "%Y-%m-%d")
            if endDateValidation.date() < date.today():
                if endDateValidation.date() >= startDateValidation.date():
                    break
                else:
                    print("The end date must be on or after the start date. Please try again.")
            else:
                print("The date entered must be a previous date. Please try again.")
        except:
            print("The date format entered is invalid. Please try again.")

    dfCompiler = APIResultCompiler(pickedcities,startdate,endDate)
    resultsset = dfCompiler.compiler()

    print("What would you like to do?")

    while True:
        print("(A) Print Raw Data\n(B) Plot Data on Graph\n(C) Import into SQL Server")
        processSelected = input(">").upper()
        if processSelected != "A" and processSelected != "B" and processSelected != "C":
            print("That was not a valid selection. Please try again.")
        else:
            break

    if resultsset[1] == 0:
        print("Sorry, there was no data in any of your city requests to process.")
    else:
         match processSelected:
            case "A":
                print(resultsset[0])
            case "B":
                DataAnalyzer = TemperatureAnalysis(resultsset[0])
                DataAnalyzer.DataPlotter()
            case "C":
                DataMover = TemperatureETL(resultsset[0])
                DataMover.SQLServerETL()
                print(str(resultsset[1]) + " rows have been imported into SQL Server.")

if __name__ == "__main__":
    main()