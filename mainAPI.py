import pymongo as pymongo
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
import redis
import json
import orjson
import os
from dotenv import load_dotenv

load_dotenv()

r = redis.Redis(
  host=str(os.getenv("REDISHOST")),
  port=int(os.getenv("REDISPORT")),
  password=str(os.getenv("REDISPASSWORD")))

fixPassword = str(os.getenv("FIXPASSWORD"))

# The connection string of the MongoDB server
MongoConnection = os.getenv("MONGOCONNECT") + "?replicaSet=" + os.getenv("MONGOREPLICA") + "&authSource=admin"

# Connect to MongoDB server
mongoclient = pymongo.MongoClient(MongoConnection)
mongodb = mongoclient["diabetes_database"]

def getAlluser():
    users = []
    for key in r.scan_iter():
        users.append(str(key.decode()))
    return users

app = FastAPI()


@app.get("/api", response_class=HTMLResponse)
async def read_root():
    return """
    <html>
        <head>
            <title>Diabetes server API</title>
        </head>
        <body style="background-color:powderblue;">
            <h1>Diabetes server API</h1>
            <br>
            <br>
            <b>/getData/:</b>
                <ul>
                    <li>user: string</li>
                    <li>collection: string</li>
                    <li>limit: integer</li>
                    <li>qtype: string</li>
                    <li>patid: integer</li>
                    <li>myquery: string</li>
                </ul>
            <br>
            <br> <br> <br> <br> <br> <br> <br> <br> <br> <br> <br>
            <img src="https://nik.uni-obuda.hu/wp-content/uploads/2020/09/cropped-nik_inv_edited.png"
             alt="HTML5 Icon" style="width:512px" >
            <p>Készítette: Kis András Nándor</p>
        </body>
    </html>
    """


# ------------MongoDB---------------------------------------------------------

@app.get("/api/getData/")
async def read_all_mongo(collection: str, user: str = "default", limit: int = 0, qtype: str = None, patid: int = None,
                         myquery: str = None):
    if user not in getAlluser():
        return {"no user": True}

    mongocol = mongodb[collection]

    if myquery is not None:
        try:
            result = mongocol.find(json.loads(myquery), {'_id': False}).limit(limit)
        except:
            return {"Bad query": True}
    else:
        if qtype is None:
            if patid is None:
                result = mongocol.find({}, {'_id': False}).limit(limit)
            else:
                result = mongocol.find({'patID': patid}, {'_id': False}).limit(limit)
        else:
            if patid is None:
                result = mongocol.find({'type': qtype}, {'_id': False}).limit(limit)
            else:
                result = mongocol.find({'type': qtype, 'patID': patid}, {'_id': False}).limit(limit)

    if result == []:
        return {"Bad attributes": True}
    else:

        async def generate():
            yield b"["
            for idx, item in enumerate(result):
                if idx > 0:
                    yield b","
                yield orjson.dumps(item)
            yield b"]"

        # Use a StreamingResponse to stream the data from the generator function
        return StreamingResponse(generate(), media_type="application/json")


@app.delete("/api/MongoDB/delete/{collection}/")
async def delete_id_mongo(collection: str, password: str):
    mongocol = mongodb[collection]
    if password == fixPassword:
        if mongocol.drop():
            return {"ok": True}
        else:
            return {"ok": False}


@app.get("/api/MongoDB/getColl/")
async def get_users(password: str):
    if password == fixPassword:
        return mongodb.list_collection_names()

# ------------------------------------Redis---------------------------------------

@app.get("/api/redis/admin/getUsers/")
async def get_users(password: str):
    if password == fixPassword:
        users = {}
        for key in r.scan_iter():
            username = key.decode()
            users.update({username : r.ttl(username)})
        return users

@app.post("/api/redis/admin/addUser/{key}/{ttl}/")
async def add_user(key: str, ttl: int, password: str):
    if password == fixPassword:
        r.set(name=key, value="", ex=ttl)
        return {"ok" : True}

@app.delete("/api/redis/admin/delUser/{key}/")
async def del_user(key: str, password: str):
    if password == fixPassword:
        r.delete(key)
        return {"ok" : True}

# ------------------------------------PatientIdentificationCallback------------------------------


if __name__ == "__main__":
    pass
