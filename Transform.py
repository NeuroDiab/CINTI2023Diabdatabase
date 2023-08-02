import xml.etree.ElementTree as ET
from datetime import datetime
import TransformModules.ReadFileNames as rfn
import pandas as pd
import json
import xmltodict
import tqdm
import numpy as np

'''def GetResults(XmlRoot, att):
    DataForOneDay = []

    if att == "glucose_level":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts" : ts, "value" : val})

    elif att == "finger_stick":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basal":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "temp_basal":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['ts_begin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_begin'] != "" and eve.attrib['ts_begin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['ts_end'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_end'] != "" and eve.attrib['ts_end'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end" : ts2, "value" : val})

    elif att == "bolus":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['ts_begin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_begin'] != "" and eve.attrib['ts_begin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['ts_end'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_end'] != "" and eve.attrib['ts_end'] != " " else None
                    type = eve.attrib['type'] if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    dose = float(eve.attrib['dose']) if eve.attrib['dose'] != "" and eve.attrib['dose'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end": ts2, "type": type, "dose" : dose})

    elif att == "meal":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    type = eve.attrib['type'] \
                        if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    carb = float(eve.attrib['carbs']) \
                        if eve.attrib['carbs'] != "" and eve.attrib['carbs'] != " " else None
                    DataForOneDay.append({"ts" : ts, "type" : type, "carbs" : carb})

    elif att == "sleep":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['ts_begin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_begin'] != "" and eve.attrib['ts_begin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['ts_end'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_end'] != "" and eve.attrib['ts_end'] != " " else None
                    q = float(eve.attrib['quality']) if eve.attrib['quality'] != "" and eve.attrib[
                        'quality'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end": ts2, "quality": q})

    elif att == "work":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['ts_begin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_begin'] != "" and eve.attrib['ts_begin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['ts_end'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_end'] != "" and eve.attrib['ts_end'] != " " else None
                    inten = float(eve.attrib['intensity']) if eve.attrib['intensity'] != "" and eve.attrib[
                        'intensity'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end": ts2, "intensity": inten})

    elif att == "stressors":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    type = eve.attrib['type'] \
                        if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    desc = eve.attrib['description'] \
                        if eve.attrib['description'] != "" and eve.attrib['description'] != " " else None
                    DataForOneDay.append({"ts" : ts, "type" : type, "description" : desc})

    elif att == "hypo_event":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                for sym in eve.iter('symptom'):
                    if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                        ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                            if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                        name = sym.attrib['name'] \
                            if sym.attrib['name'] != "" and sym.attrib['name'] != " " else None
                        DataForOneDay.append({"ts": ts, "symptom": name})

    elif att == "illness":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['ts_begin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_begin'] != "" and eve.attrib['ts_begin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['ts_end'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts_end'] != "" and eve.attrib['ts_end'] != " " else None
                    type = eve.attrib['type'] if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    desc = eve.attrib['description'] \
                        if eve.attrib['description'] != "" and eve.attrib['description'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end": ts2, "type": type, "description" : desc})

    elif att == "exercise":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    intens = int(eve.attrib['intensity']) \
                        if eve.attrib['intensity'] != "" and eve.attrib['intensity'] != " " else None
                    type = eve.attrib['type'] \
                        if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    dur = float(eve.attrib['duration']) \
                        if eve.attrib['duration'] != "" and eve.attrib['duration'] != " " else None
                    comp = eve.attrib['competitive'] \
                        if eve.attrib['competitive'] != "" and eve.attrib['competitive'] != " " else None
                    DataForOneDay.append({"ts" : ts, "intensity" : intens,
                                          "type" : type, "duration" : dur, "competitive" : comp})

    elif att == "basis_heart_rate":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basis_gsr":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basis_skin_temperature":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basis_air_temperature":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basis_steps":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})

    elif att == "basis_sleep":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts1 = str(datetime.strptime(eve.attrib['tbegin'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['tbegin'] != "" and eve.attrib['tbegin'] != " " else None
                    ts2 = str(datetime.strptime(eve.attrib['tend'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['tend'] != "" and eve.attrib['tend'] != " " else None
                    q = float(eve.attrib['quality']) if eve.attrib['quality'] != "" and eve.attrib[
                        'quality'] != " " else None
                    type = float(eve.attrib['type']) if eve.attrib['type'] != "" and eve.attrib['type'] != " " else None
                    DataForOneDay.append({"ts_begin": ts1, "ts_end": ts2, "quality": q, "type" : type})

    elif att == "acceleration":
        for data in XmlRoot.iter(att):
            for eve in data.iter('event'):
                temptime = eve.attrib[eve.keys()[0]].split(sep=" ", maxsplit=1)
                if str((datetime.strptime(temptime[0], '%d-%m-%Y')).strftime('%Y-%m-%d')) == day:
                    ts = str(datetime.strptime(eve.attrib['ts'], '%d-%m-%Y %H:%M:%S').isoformat()) \
                        if eve.attrib['ts'] != "" and eve.attrib['ts'] != " " else None
                    val = float(eve.attrib['value']) if eve.attrib['value'] != "" and eve.attrib['value'] != " " else None
                    DataForOneDay.append({"ts": ts, "value": val})
    return DataForOneDay'''


def Upload(path):
    glucoseDF = pd.DataFrame()
    fingerStickDF = pd.DataFrame()
    basalDF = pd.DataFrame()
    tempBasalDF = pd.DataFrame()
    bolusDF = pd.DataFrame()
    mealDF = pd.DataFrame()
    sleepDF = pd.DataFrame()
    workDF = pd.DataFrame()
    stressorDF = pd.DataFrame()
    hypoEventDF = pd.DataFrame()
    illnessDF = pd.DataFrame()
    exerciseDF = pd.DataFrame()
    basisHeartRateDF = pd.DataFrame()
    basisGsrDF = pd.DataFrame()
    basisSkinTempDF = pd.DataFrame()
    basisAirTempDF = pd.DataFrame()
    basisStepDF = pd.DataFrame()
    basisSleepDF = pd.DataFrame()
    accelerationDF = pd.DataFrame()

    resDFs = [glucoseDF, fingerStickDF, basalDF, tempBasalDF, bolusDF, mealDF, sleepDF, workDF, stressorDF, hypoEventDF,
            illnessDF, exerciseDF, basisHeartRateDF, basisGsrDF, basisSkinTempDF, basisAirTempDF, basisStepDF,
            basisSleepDF, accelerationDF]

    ListOfNames = rfn.ListOfFiles(path)
    for Name in tqdm.tqdm(ListOfNames):
        newDFs = OnePatient(Name)
        for x in range(len(resDFs)):
            resDFs[x] = pd.concat([resDFs[x], newDFs[x]], axis=0, ignore_index=True)


    return resDFs

def OnePatient(Files):
    glucoseDF = pd.DataFrame()
    fingerStickDF = pd.DataFrame()
    basalDF = pd.DataFrame()
    tempBasalDF = pd.DataFrame()
    bolusDF = pd.DataFrame()
    mealDF = pd.DataFrame()
    sleepDF = pd.DataFrame()
    workDF = pd.DataFrame()
    stressorDF = pd.DataFrame()
    hypoEventDF = pd.DataFrame()
    illnessDF = pd.DataFrame()
    exerciseDF = pd.DataFrame()
    basisHeartRateDF = pd.DataFrame()
    basisGsrDF = pd.DataFrame()
    basisSkinTempDF = pd.DataFrame()
    basisAirTempDF = pd.DataFrame()
    basisStepDF = pd.DataFrame()
    basisSleepDF = pd.DataFrame()
    accelerationDF = pd.DataFrame()

    with open(Files, 'r', encoding='utf-8') as file:
        my_xml = file.read()

    my_dict = xmltodict.parse(my_xml)

    MeasureTypes = ["glucose_level", "finger_stick", "basal", "temp_basal", "bolus", "meal", "sleep", "work",
                       "stressors", "hypo_event", "illness", "exercise", "basis_heart-rate", "basis_gsr",
                       "basis_skin_temperature", "basis_air_temperature", "basis_steps", "basis_sleep", "acceleration"]

    id = int(my_dict['patient']['@id'])
    weight = my_dict['patient']['@weight']
    insulin = my_dict['patient']['@insulin_type']

    for mtype in MeasureTypes:

        if mtype == "glucose_level":
            glucoseDF = pd.DataFrame.from_dict(my_dict['patient'][mtype]['event'])
            glucoseDF.rename(columns={"@value": "value"}, inplace=True)
            glucoseDF['value'] = glucoseDF.apply(SetValue, axis=1).astype(float)
            glucoseDF['timestamp'] = glucoseDF.apply(SetDate, axis=1).astype('float64')
            glucoseDF['patID'] = id
            glucoseDF['type'] = "glucose"
            glucoseDF['value'] = glucoseDF['value'].astype(float)
            del glucoseDF['@ts']

        elif mtype == "finger_stick":
            pass

        elif mtype == "basal":
            pass

        elif mtype == "temp_basal":
            pass

        elif mtype == "bolus":
            bolusDF = pd.DataFrame.from_dict(my_dict['patient'][mtype]['event'])
            bolusDF.rename(columns={"@dose": "value"}, inplace=True)
            bolusDF['timestamp'] = bolusDF.apply(SetDateBegin, axis=1).astype('float64')
            bolusDF['patID'] = id
            bolusDF['type'] = "insulin"
            bolusDF['intoStomach'] = False
            bolusDF['subtype'] = 10
            bolusDF['value'] = bolusDF['value'].astype(float)
            del bolusDF['@ts_begin']
            del bolusDF['@ts_end']
            del bolusDF['@type']
            if '@bwz_carb_input' in bolusDF.columns:
                del bolusDF['@bwz_carb_input']

        elif mtype == "meal":
            mealDF = pd.DataFrame.from_dict(my_dict['patient'][mtype]['event'])
            mealDF['timestamp'] = mealDF.apply(SetDate, axis=1).astype('float64')
            mealDF['type'] = "meal"
            mealDF['patID'] = id

            mealDF['id'] = range(0, 0+len(mealDF))
            mealDF['text'] = "Carbohydrate"
            mealDF['unit'] = "GRAM"
            mealDF.rename(columns={"@carbs": "amount"}, inplace=True)
            mealDF['amount'] = mealDF['amount'].astype('int64')
            mealDF.rename(columns={"@type": "mealType"}, inplace=True)
            mealDF['details'] = None
            mealDF['weights'] = 1
            del mealDF['@ts']

            timestamps = [*set(list(mealDF["timestamp"]))]
            foods = CreateFood(mealDF)
            for time in timestamps:
                myfood = (d['res'] for d in foods if d['time'] == time)
                mealDF.loc[mealDF["timestamp"] == time, "foods"] = json.dumps(list(myfood)[0])

            del mealDF['details']
            del mealDF['id']
            del mealDF['text']
            del mealDF['unit']
            del mealDF['amount']
            del mealDF['mealType']
            del mealDF['weights']

            mealDF = mealDF.drop_duplicates(subset=['timestamp', 'patID'], keep="first")

        elif mtype == "sleep":
            pass

        elif mtype == "work":
            pass

        elif mtype == "stressors":
            pass

        elif mtype == "hypo_event":
            pass

        elif mtype == "illness":
            pass

        elif mtype == "exercise":
            try:
                if type(my_dict['patient'][mtype]['event']) == dict:
                    exerciseDF = pd.DataFrame.from_dict([(my_dict['patient'][mtype]['event'])])
                else:
                    exerciseDF = pd.DataFrame.from_dict(my_dict['patient'][mtype]['event'])
                exerciseDF.rename(columns={"@intensity": "intensity"}, inplace=True)
                exerciseDF.rename(columns={"@type": "exerciseType"}, inplace=True)
                exerciseDF["exerciseType"] = exerciseDF["exerciseType"].replace(' ', np.nan, regex=True)
                exerciseDF.rename(columns={"@competitive": "competitive"}, inplace=True)
                exerciseDF['timestamp'] = exerciseDF.apply(SetDate, axis=1).astype('float64')
                exerciseDF['patID'] = id
                exerciseDF['type'] = "exercise"

                exerciseDF['intensity'] = exerciseDF['intensity'].astype('int64')
                exerciseDF['endTimestamp'] = exerciseDF.apply(EndTimestamp, axis=1).astype('float64')
                del exerciseDF['@ts']
                del exerciseDF['@duration']
            except TypeError:
                pass

        elif mtype == "basis_heart_rate":
            pass

        elif mtype == "basis_gsr":
            pass

        elif mtype == "basis_skin_temperature":
            pass

        elif mtype == "basis_air_temperature":
            pass

        elif mtype == "basis_steps":
            pass

        elif mtype == "basis_sleep":
            pass

        elif mtype == "acceleration":
            pass

    return [glucoseDF, fingerStickDF, basalDF, tempBasalDF, bolusDF, mealDF, sleepDF, workDF, stressorDF, hypoEventDF,
            illnessDF, exerciseDF, basisHeartRateDF, basisGsrDF, basisSkinTempDF, basisAirTempDF, basisStepDF,
            basisSleepDF, accelerationDF]

def SetDate(row):
    return datetime.strptime(str(row['@ts']), '%d-%m-%Y %H:%M:%S').timestamp()

def SetDateBegin(row):
    return datetime.strptime(str(row['@ts_begin']), '%d-%m-%Y %H:%M:%S').timestamp()

def SetValue(row):
    # mmol/l = mg/dl / 18
    return round(float(float(row['value'])/18), 1)

def EndTimestamp(row):
    return row['timestamp'] + (int(row['@duration']) * 60)

def CreateFood(df):
    foodDict = []
    timestamps = [*set(list(df["timestamp"]))]
    for time in timestamps:
        res = df.groupby(["timestamp"]).get_group(time)
        del res['timestamp']
        resDict = res.to_dict('records')
        for dict in resDict:
            dict['details'] = {"mealType": dict['mealType']}
            dict.pop('mealType')
            dict.pop('patID')
            dict.pop('type')
        foodDict.append({"time": time, "res": resDict})

    return foodDict