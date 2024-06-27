
import requests
import requests
import json
import os
import pandas as pd
import time

city_data = {};

# POBIERANIE DANYCH MIAST Z GUS
for i in range(46):
    url = f"https://bdl.stat.gov.pl/api/v1/units?page={i}&page-size=100";
    response = requests.get(url)
    responseData = response.json()
    print("responseData.results",responseData)
    for city in responseData["results"]:
        if city["level"] == 6:
            city_data[city["name"]] = {"name": city["name"], "id": city["id"]}
            if "parentId" in city:
                city_data[city["name"]]["parentId"] = city["parentId"]

if os.path.exists('./api_data/miasta.json') and os.path.getsize('./api_data/miasta.json') > 0:
    # Odczytanie istniejącej zawartości pliku
    with open('./api_data/miasta.json', 'r', encoding='utf-8') as file:
        city_data = json.load(file)
with open('./api_data/miasta.json', 'w', encoding='utf-8') as file:
    json.dump(city_data, file, ensure_ascii=False, indent=4)
print("Dane zostały zapisane do pliku agregats.json")

# # POBIERANIE TEMATÓW
with open('./api_data/variables2.json', 'r', encoding='utf-8') as file:
    subjects = json.load(file)
# # POBIERANIE MIAST
with open('./api_data/miasta.json', 'r', encoding='utf-8') as file:
    cities = json.load(file)
#
# # OPRAWA DANYCH Z VARIABLES
result_data = {}
subjectsDisc = {}
subjectsArr = []
subjectsIds = ""
for subject in subjects:
    for value in subject["tematy"]:
        subjectsDisc[value["id"]] = subject["name"]
        subjectsArr.append(value["id"])
        subjectsIds += f"var-id={value['id']}&"

print(subjectsIds)
categoricalData = {}
count = 0
fileCount = 0

arr_id = []
arr_category_id = []
arr_category_name = []
arr_city_id = []
arr_city_name = []
arr_year = []
arr_value = []

# POBRANIE WSZYSTKICH DANYCH DLA KONKRETNEGO MIASTA
# NIE DALO SIĘ POGRAĆ PO KATEGRIACH, TYLKO PO MIASTACH BO ZA DUŻO REQUESTÓW BYŁO
count= 0
for city_key,city_value in cities.items():
    var_url = f"https://bdl.stat.gov.pl/api/v1/data/by-unit/{city_value["id"]}?{subjectsIds}aggregate-id=1&page=0&page-size=100"
    max_retries = 5
    retry_count = 0
    print(f"{count} / {len(cities)}")
    count +=1
    try:
        response = requests.get(var_url)
        response.raise_for_status()
        responseData = response.json()
        with open(f'./api_data/data2/{city_value["id"]}.json', 'w', encoding='utf-8') as file:
            json.dump(responseData, file, ensure_ascii=False, indent=4)
        print("Dane zostały zapisane do pliku agregats.json")
    except requests.exceptions.RequestException as e:
        print(f"Błąd połączenia: {e}, próba {retry_count + 1} z {max_retries}")
        retry_count += 1
        if retry_count < max_retries:
            time.sleep(30)  # Odczekaj 30 sekund przed ponowną próbą
        else:
            print("Nie udało się połączyć po maksymalnej liczbie prób.")

for root, dirs, files in os.walk("./api_data/data2"):
    for file in files:
        print(f"{fileCount} / {len(files)}")
        fileCount+=1
        file_path = os.path.join(root, file)
        with open(file_path, 'r', encoding='utf-8') as file:
            city_data = json.load(file)
            if "results" in city_data:
                # poziom kategorii
                for result in city_data["results"]:
                    if "values" in result:
                        years = [0] * 29
                        for value in result["values"]:
                            arr_id.append(count)
                            arr_category_id.append(result["id"])
                            arr_category_name.append(subjectsDisc[result["id"]])
                            arr_city_id.append(city_data["unitId"])
                            arr_city_name.append(city_data["unitName"])
                            arr_year.append(int(value["year"]))
                            arr_value.append(value["val"])
                            years[int(value["year"]) - 1995] = 1
                            count+=1

                        for index, year in enumerate(years):
                            if year == 0 :
                                arr_id.append(count)
                                arr_category_id.append(result["id"])
                                arr_category_name.append(subjectsDisc[result["id"]])
                                arr_city_id.append(city_data["unitId"])
                                arr_city_name.append(city_data["unitName"])
                                arr_year.append(1995+index)
                                arr_value.append("-")

output = {
    "id":arr_id,
    "categoryId":arr_category_id,
    "categoryName":arr_category_name,
    'cityId':arr_city_id,
    "cityName":arr_city_name,
    "year":arr_year,
    "value":arr_value,
}
# Tworzenie DataFrame
df = pd.DataFrame(output)

# Zapisywanie do pliku CSV
df.to_csv('./api_data/output2.csv', index=False)




