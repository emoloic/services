import pika, json, tempfile, os
from bson.objectid import ObjectId
import moviepy.editor

def start(message, fs_videos, fs_mp3s, channel):
    """
    Function that receives the message from the video queue, uploads the video from mongodb,
    converts it to mp3 format and send back the converted file in MongoDB.
    Then sends a message to the "mp3" queue.
    """
    # Load our message
    message = json.loads(message)

    # Create an empty tempary file (in a tmp directory) and write our video content to that temporary file
    tf = tempfile.NamedTemporaryFile()
    # Get out video file from gridfs
    out = fs_videos.get(ObjectId(message["video_fid"]))
    # Add video contents to empty file
    tf.write(out.read())
    # Create audio from temp video file
    audio = moviepy.editor.VideoFileClip(tf.name).audio
    tf.close() # After we close the file, the temp file will automatically be deleted

    # Write the video to the temp directory
    tf_path = tempfile.gettempdir() + f"/{message['video_fid']}.mp3"
    audio.write_audiofile(tf_path)

    # Save the file to mongodb
    f = open(tf_path, "rb")
    data = f.read()
    fid = fs_mp3s.put(data)
    f.close()
    os.remove(tf_path)

    # Update our message to insert the mp3_fid field
    message["mp3_fid"] = str(fid)

    try:
        #  The producer never sends any messages directly to a queue.
        #  It can only send messages to an exchange.
        #  We were using a default exchange, which we identify by the empty string ("")
        #  Messages are routed to the queue with the name specified by routing_key, if it exists
        channel.basic_publish(
            exchange="",
            routing_key=os.getenv("MP3_QUEUE"),
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
    except Exception as err:
        # Remove the mp3 in mongodb if we can't add the message to the queue.
        fs_mp3s.delete(fid)
        return "failed to publish message"