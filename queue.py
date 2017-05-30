import pika

class Queue():

    def __init__(self, routing_key, host, port):
        '''
        Our queue is a wrapper for passing and subscribing to messages on a given channel for RabbitMq
        :param routing_key: The channel we'll publish messages on
        :param host: Our rabbit mq host
        :param port: The port to connect to
        '''
        self.q_name = routing_key
        self.host = host
        self.port = port

    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.q_name, body=str(message))

    def consume(self, callback):
        self.channel.basic_consume(callback, queue=self.q_name, no_ack=True)
        self.channel.start_consuming()

    def __enter__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()
        self.channel.queue_declare('insurance_info')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

