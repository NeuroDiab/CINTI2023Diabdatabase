import sys
from datetime import datetime
from time import sleep
import TransformModules.ReadFileNames as rfn
import glob
import pandas as pd
import os
import json
from tqdm import tqdm
import dask.dataframe as dd

def Upload(path):

    diabetes_sub = path[:-1] + "diabetes_subset/*"
    ListOfDirs = rfn.ListOfFiles(diabetes_sub)
    resultList = []

    # iterate through patients
    for Files in ListOfDirs:
        # if os.path.basename(Files).replace("0", "") == "1":
        resultList = resultList + OnePatientDiabetic(Files)

    '''healthy_sub = path[:-1] + "healthy_subset/*"
    ListOfDirs = rfn.ListOfFiles(healthy_sub)

    # iterate through patients
    for Files in ListOfDirs:
        resultList = resultList + OnePatientHealthy(Files)'''

    return resultList

def OnePatientDiabetic(Files):
    # patient datas
    pat_id = int(os.path.basename(Files).replace("0", ""))
    '''pat_weight = 73  # average because lack of data
    pat_insulinType = None
    status = "diabetic"

    patAttribs = {"id" : pat_id, "weight" : pat_weight, "insulinType" : pat_insulinType,
                  "status" : status}'''

    glucoseDF = pd.read_csv(glob.glob(Files + "/glucose*.csv", recursive=True).pop())
    glucoseDF.rename(columns={"glucose": "value"}, inplace=True)
    glucoseDF.rename(columns={"type": "measureType"}, inplace=True)
    glucoseDF['timestamp'] = (glucoseDF.apply(SetGlucoseDate, axis=1)).astype(float)
    glucoseDF['type'] = "glucose"
    glucoseDF['patID'] = pat_id
    del glucoseDF['date']
    del glucoseDF['time']

    glucoseDF.dropna(subset=['value'])

    insulinDF = pd.read_csv(glob.glob(Files + "/insulin.csv", recursive=True).pop())
    insulinDF = insulinDF.melt(id_vars=["date", "time", "comment"],
           var_name="subtype",
           value_name="value")
    insulinDF.dropna(subset=['value'], inplace=True)
    insulinDF['value'] = insulinDF['value'].astype(float)
    insulinDF['timestamp'] = (insulinDF.apply(SetGlucoseDate, axis=1)).astype(float)
    insulinDF['subtype'] = (insulinDF.apply(SetSubType, axis=1)).astype(str)
    insulinDF['type'] = "insulin"
    insulinDF['patID'] = pat_id
    insulinDF['intoStomach'] = False
    del insulinDF['date']
    del insulinDF['time']



    # measureTypes = ["food", "glucose", "insulin", "Accel", "Breathing", "ECG", "BB","RR", "Summary"]

    # ------------Food---------------------
    '''foodDF = pd.read_csv(glob.glob(Files + "/food.csv", recursive=True).pop())
    del foodDF['picture']'''

    # -------------------------sensor data upload----------------------------------
    sensorData = glob.glob(Files + "/sensor_data/*")
    sensorRes = []
    for data in tqdm(sensorData):
        sensorDataDate = glob.glob(data + "/*")
        for csv in sensorDataDate:
            csvName = os.path.basename(csv).split("-")[1].split("_")[3]

            if "Accel.csv" == csvName:
                accelDDF = dd.read_csv(csv)
                #accelDF = accelDDF.compute()
                accelDDF = accelDDF.map_partitions(lambda part: part[part.index % 10 == 0], meta=accelDDF)
                accelDDF["timestamp"] = (accelDDF.apply(SetDate, axis=1, meta=('timestamp', 'f8')))
                accelDDF['type'] = "acceleration"
                accelDDF['patID'] = pat_id
                del accelDDF['Time']
                sensorRes.append(accelDDF)

            elif "ECG.csv" == csvName:
                ecgDDF = dd.read_csv(csv)
                #ecgDF = ecgDDF.compute()
                ecgDDF = ecgDDF.map_partitions(lambda part: part[part.index % 25 == 0], meta=ecgDDF)
                ecgDDF["timestamp"] = (ecgDDF.apply(SetDate, axis=1, meta=('timestamp', 'f8')))
                ecgDDF['type'] = "ECG"
                ecgDDF['patID'] = pat_id
                del ecgDDF['Time']
                sensorRes.append(ecgDDF)

            '''if "Summary.csv" == csvName:
                summaryDF = csv_to_df(csv)
                sensorRes.append(summaryDF)

            elif "Accel.csv" == csvName:
                accelDF = csv_to_df(csv)
                sensorRes.append(accelDF)

            elif "Breathing.csv" == csvName:
                breathingDF = csv_to_df(csv)
                sensorRes.append(breathingDF)

            elif "ECG.csv" == csvName:
                ecgDF = csv_to_df(csv)
                sensorRes.append(ecgDF)
                            
            elif "BB.csv" == csvName:
                bbDF = csv_to_df(csv)
                sensorRes.append(bbDF)

            elif "RR.csv" == csvName:
                rrDF = csv_to_df(csv)
                sensorRes.append(rrDF)'''

    return [glucoseDF, insulinDF] + sensorRes


'''def OnePatientHealthy(Files):
    # patient datas
    pat_id = os.path.basename(Files)
    pat_id = int(pat_id[0:2].replace("0", "") + pat_id[2]) + 9
    pat_weight = 62  # average because lack of data
    pat_insulinType = None
    status = "healthy"

    patAttribs = {"id" : pat_id, "weight" : pat_weight, "insulinType" : pat_insulinType,
                  "status" : status}

    # dates
    dates = []
    dates.append("default")
    sensor_dates = []
    sensorData = glob.glob(Files + "/sensor_data/*")
    for sensorDay in sensorData:
        dayString = os.path.basename(sensorDay).split("-")[0].replace("_", "-")
        dates.append(dayString) if dayString not in dates else dates
        sensor_dates.append(dayString) if dayString not in sensor_dates else sensor_dates

    glucose = pd.read_csv(glob.glob(Files + "/glucose*.csv", recursive=True).pop())
    attrGluc = list(glucose.columns)
    annot = pd.read_csv(glob.glob(Files + "/annotations.csv", recursive=True).pop())
    attrAnnot = list(annot.columns)
    food = pd.read_csv(glob.glob(Files + "/food.csv", recursive=True).pop())
    attrFood = list(food.columns)

    # Collect all of the dates
    for ind in glucose.index:
        dates.append(str(glucose[attrGluc[0]][ind])) if str(glucose[attrGluc[0]][ind]) \
                                                        not in dates else dates
    for ind in annot.index:
        dates.append(str(annot[attrAnnot[0]][ind])) if str(annot[attrAnnot[0]][ind]) \
                                                       not in dates else dates
    for ind in food.index:
        dates.append(str(food[attrFood[0]][ind])) if str(food[attrFood[0]][ind]) \
                                                     not in dates else dates

    measureTypes = ["food", "glucose", "annotations", "Accel", "Breathing", "ECG",
                    "BB", "RR", "Summary"]

    allData = []

    for date in dates:
        oneDay = {"date": date}

        OneGluc = []
        if date not in list(glucose[attrGluc[0]]):
            pass
        else:
            for ind in glucose.index:
                if str(glucose[attrGluc[0]][ind]) == date:
                    time = str(datetime.strptime(str(glucose[attrGluc[0]][ind])
                                                        + " " + str(glucose[attrGluc[1]][ind]),
                                                        '%Y-%m-%d %H:%M').isoformat()) if str(
                        glucose[attrGluc[0]][ind]) != "nan" or str(
                        glucose[attrGluc[1]][ind]) != "nan" else None

                    gluc = float(glucose[attrGluc[2]][ind]) if str(glucose[attrGluc[2]][ind]) \
                                                               != "nan" else None
                    type = str(glucose[attrGluc[3]][ind]) if str(
                        glucose[attrGluc[3]][ind]) != "nan" else None
                    com = str(glucose[attrGluc[4]][ind]) if str(
                        glucose[attrGluc[4]][ind]) != "nan" else None

                    OneGluc.append({
                        attrGluc[1] : time,
                        attrGluc[2] : gluc,
                        attrGluc[3] : type,
                        attrGluc[4] : com})

        oneDay.update({"glucose": OneGluc})

        OneAnnot = []
        if date not in list(annot[attrAnnot[0]]):
            pass
        else:
            for ind in annot.index:
                if str(annot[attrAnnot[0]][ind]) == date:
                    start_time = str(datetime.strptime(str(annot[attrAnnot[0]][ind])
                                                              + " " + str(annot[attrAnnot[1]][ind]),
                                                              '%Y-%m-%d %H:%M').isoformat()) if str(
                        annot[attrAnnot[0]][ind]) != "nan" or str(
                        annot[attrAnnot[1]][ind]) != "nan" else None
                    end_time = str(datetime.strptime(str(annot[attrAnnot[2]][ind])
                                                            + " " + str(annot[attrAnnot[3]][ind]),
                                                            '%Y-%m-%d %H:%M').isoformat()) if str(
                        annot[attrAnnot[2]][ind]) != "nan" or str(
                        annot[attrAnnot[3]][ind]) != "nan" else None
                    type = str(annot[attrAnnot[4]][ind]) if str(
                        annot[attrAnnot[4]][ind]) != "nan" else None
                    desc = str(annot[attrAnnot[5]][ind]) if str(
                        annot[attrAnnot[5]][ind]) != "nan" else None

                    OneAnnot.append({
                        "start_time": start_time,
                        "end_time": end_time,
                        "type": type,
                        "desc": desc})

        oneDay.update({"annotations": OneAnnot})

        # ------------Food---------------------
        PatFood = []
        if date not in list(food[attrFood[0]]):
            pass
        else:
            for ind in food.index:
                if str(food[attrFood[0]][ind]).replace("/", "-") == date:
                    time = str(datetime.strptime(str(food[attrFood[0]][ind]).replace("/", "-")
                                                        + " " + str(food[attrFood[1]][ind]).
                                                 replace("/", "-"),'%Y-%m-%d %H:%M').isoformat()) \
                        if str(food[attrFood[0]][ind]) != "nan" or str(food[attrFood[1]][ind])\
                           != "nan" else None
                    pic = str(food[attrFood[2]][ind]) if str(food[attrFood[2]][ind]) != "nan" \
                        else None
                    desc = str(food[attrFood[3]][ind]) if str(food[attrFood[3]][ind]) != "nan" \
                        else None
                    cal = int(food[attrFood[4]][ind]) if str(food[attrFood[4]][ind]) != "nan" \
                        else None
                    bal = str(food[attrFood[5]][ind]) if str(food[attrFood[5]][ind]) != "nan" \
                        else None
                    qual = str(food[attrFood[6]][ind]) if str(food[attrFood[6]][ind]) != "nan" \
                        else None

                    PatFood.append({
                        attrFood[1]: time,
                        attrFood[2]: pic,
                        attrFood[3]: desc,
                        attrFood[4]: cal,
                        attrFood[5]: bal,
                        attrFood[6]: qual})

        oneDay.update({"food": PatFood})


        # -------------------------sensor data upload----------------------------------
        if date in sensor_dates:
            sensorData = glob.glob(Files + "/sensor_data/*/" + date.replace("-", "_") + "*",
                                   recursive=True)
            for data in sensorData:
                csvName = os.path.basename(data).split("-")[1].split("_")[3]

                if "Summary.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"Summary": array})

                elif "Accel.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"Accel": array})

                elif "Breathing.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"Breathing": array})

                elif "ECG.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"ECG": array})

                elif "BB.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"BB": array})

                elif "RR.csv" == csvName:
                    array = csv_to_json(data)
                    oneDay.update({"RR": array})
        allData.append(oneDay)'''

def SetDate(row):
    if row['Time'] is None or row['Time'] == 0 or row['Time'] == '0':
        return None
    else:
        return str(datetime.strptime(row['Time'], '%d/%m/%Y %H:%M:%S.%f').timestamp())

def SetGlucoseDate(row):
    return str(datetime.strptime(str(row['date']) + " " + str(row['time']), '%Y-%m-%d %H:%M:%S').timestamp())

def SetSubType(row):
    if row['subtype'] == "fast_insulin":
        return "short"
    elif row['subtype'] == "slow_insulin":
        return "long"
    else:
        return None
