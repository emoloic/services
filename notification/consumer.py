import pika, sys, os, time
from send import email

def main():
    # rabbitmq connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel = connection.channel()

    def callback(ch, method, properties, body):
        err = email.notification(body)
        # If there is an error to process the message, we're not going to acknowledge that we received an processed the message.
        # So the message won't be remove from the queue and we'll be process later.

        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ will not dispatch a new message to a consumer until it has processed and acknowledged the previous one.
    channel.basic_qos(prefetch_count=1)

    # Start consuming message from the queue
    channel.basic_consume(
        queue=os.getenv("MP3_QUEUE"), on_message_callback=callback
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