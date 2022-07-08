import csv
import json
import requests
stocks = ["AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "AMGN", "AMT", "AMZN", "AVGO", "AXP", "BA", "BAC", "BK", "BKNG", "BLK", "BMY", "C", "CAT", "CHTR", "CL", "CMCSA", "COF", "COP"]
url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
for stock in stocks:
    querystring = {"ticker_symbol": stock, "years": "5", "format": "json"}
    headers = {
        "X-RapidAPI-Key": "f486f6522emsh8297d4d2fca6cb9p1a6980jsn0124435a1254",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    filename = "/Users/sumanchoudhary/PycharmProjects/pythonProject/python-spark-assignment/Data"+stock+".csv"
    columns = ["Open", "High", "Low", "Close", "Adj_close", "Volume", "Date", "Stock_Name"]
    rows = []
    res = json.loads(response.text)
    for i in res["historical prices"]:
        templist = []
        templist = list(i.values())
        templist.append(stock)
        rows.append(templist)

    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(columns)
        csvwriter.writerows(rows)