import pika, json

def upload(f, fs, channel, access):
    """
    Function to put our video file to MongoDB and to send a message to our queue (video)
    """
    try:
        # Put our file into our collection (videos)
        fid = fs.put(f) # file id is returned
    except Exception as err:
        print(err)
        return "internal server error", 500

    # Create a message to put in our queue.
    message = {
        "video_fid": str(fid),
        "mp3_fid": None,
        "username": access["username"], # help us to identify who owns the file
    }

    try:
        #  The producer never sends any messages directly to a queue.
        #  It can only send messages to an exchange.
        #  We were using a default exchange, which we identify by the empty string ("")
        #  Messages are routed to the queue with the name specified by routing_key, if it exists
        channel.basic_publish(
            exchange="",
            routing_key="video", # send msg to our queue called video
            body=json.dumps(message), # message to be sent in JSON string format
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ), # Our messages are persisted in our queue. The queue is going to be retained even if the pod restarts.
        )
    except Exception as err:
        print(err)
        # delete the file if unsuccessfully added to the queue.
        fs.delete(fid)
        return "internall server error", 500