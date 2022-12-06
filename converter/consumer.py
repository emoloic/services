import pika, sys, os, time
from pymongo import MongoClient
import gridfs
from convert import to_mp3

MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
MONGODB_CLUSTER_URL = os.getenv("MONGODB_CLUSTER_URL")

def main():
    mongodb_uri = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER_URL}"
    client = MongoClient(MONGODB_USERNAME, 27017)
    db_videos = client.videos
    db_mp3s = client.mp3s
    # gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    # rabbitmq connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel = connection.channel()

    def callback(ch, method, properties, body):
        """
        Whenever we receive a message, this callback function is called by the Pika library. 
        """
        err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
        # If there is an error to process the message, we're not going to acknowledge that we received and processed the message.
        # So the message won't be remove from the queue and we'll be process later.

        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # RabbitMQ will not dispatch a new message to a consumer until it has processed and acknowledged the previous one.
    channel.basic_qos(prefetch_count=1)

    # Start consuming message from the queue
    channel.basic_consume(
        queue=os.getenv("VIDEO_QUEUE"), on_message_callback=callback
    )

    print("Waiting for messages. To exit press CTRL+C")

    # Run our consumer
    # Our consumer is going to be listening to the queue
    channel.start_consuming() 

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)