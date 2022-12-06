import os, gridfs, pika, json
from flask import Flask, request, send_file
from flask_pymongo import PyMongo
from auth import validate
from auth_svc import access
from storage import util
from bson.objectid import ObjectId

server = Flask(__name__)

MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
MONGODB_CLUSTER_URL = os.getenv("MONGODB_CLUSTER_URL")

mongodb_video_uri = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_URL}/videos"
mongodb_mp3_uri = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_URL}/mp3s"

mongo_video = PyMongo(
    server,
    uri = mongodb_video_uri
)

mongo_mp3 = PyMongo(
    server,
    uri = mongodb_mp3_uri    
)

# Enable us to use mongodb gridfs
fs_videos = gridfs.GridFS(mongo_video.db) 
fs_mp3s = gridfs.GridFS(mongo_mp3.db)

# Configure our RabbitMQ connection
# Make communication with our RabbitMQ synchronous
connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq")) # We pass the host for our RabbitMD queue.
channel = connection.channel()

# Creation of our queue and declaring it durable
channel.queue_declare(queue=os.getenv("VIDEO_QUEUE"), durable=True)
channel.queue_declare(queue=os.getenv("MP3_QUEUE"), durable=True)

@server.route("/login", methods=["POST"])
def login():
    token, err = access.login(request)

    if not err:
        return token
    else:
        return err

@server.route("/upload", methods=["POST"])
def upload():
    # access will contains our JWT payload
    access, err = validate.token(request)
    
    # We need to parse in JSON format
    access = json.loads(access)

    if access["admin"]:
        if len(request.files) > 1 or len(request.files) < 1:
            return "exactly 1 file required", 400

        for _, f in request.files.items():
            err = util.upload(f, fs_videos, channel, access)

            if  err:
                return err

        return "success!", 200
    else:
        return "not authorized", 401

@server.route("/download", methods=["POST"]) 
def download():
    # access will contains our JWT payload
    access, err = validate.token(request)

    if err:
        return err
    
    # We need to parse in JSON format
    access = json.loads(access)

    if access["admin"]:
        # get fid from the request
        fid_string = request.args.get("fid")

        if not fid_string:
            return "fid is required", 400
        
        try:
            # get the object from mongodb
            out = fs_mp3s.get(ObjectId(fid_string))
            return send_file(out, download_name=f"{fid_string}.mp3")
        except Exception as err:
            print(err)
            return "internal server error", 500

    return "not authorized", 401

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8080)