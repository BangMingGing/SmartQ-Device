import os
import sys
import argparse
import pika
import pickle
import time
import subprocess


RABBITMQ_SERVER_IP = '203.255.57.129'
RABBITMQ_SERVER_PORT = '5672'


class Device():
    
    def __init__(self, device_name, smartQ_email):
        self.credentials = pika.PlainCredentials('rabbitmq', '1q2w3e4r')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_SERVER_IP, RABBITMQ_SERVER_PORT, 'vhost', self.credentials))
        self.channel = self.connection.channel()

        self.device_name = device_name
        self.queue_name = device_name
        self.smartQ_email = smartQ_email
        
        # Queue 선언
        queue = self.channel.queue_declare(device_name)
        # Queue-Exchange Binding
        self.channel.queue_bind(exchange=smartQ_email, queue=device_name, routing_key=f'to{device_name}')

        


    
    def callback(self, ch, method, properties, body):
        message = pickle.loads(body, encoding='bytes')
        header = message['header']
        file_name = message['name']
        contents = message['contents']

        if header == 'model':
            with open(f'{file_name}', 'wb') as f:
                f.write(contents)

            model = ['python', 'Inference_worker.py', file_name, 'image.jpg']

            result_message = {}
            result_message['email'] = self.smartQ_email
            result_message['device_name'] = self.device_name
            result_message['model_name'] = file_name

            start_time = time.time()
            results = ''
            try:
                results = subprocess.check_output(model, shell=False, encoding='UTF-8')
            except :
                result_message['error'] = 'error occured'
            end_time = time.time()
            
            result_message['work_time'] = end_time - start_time
            
            print(results)
            tmp = results.split('^')
            result_message[tmp[0]] = tmp[1]
            result_message[tmp[2]] = tmp[3]
            

            print('result : ', result_message)

            self.publish(result_message)
            os.remove(f'{file_name}')

        elif header == 'image':
            with open(f'{file_name}', 'wb') as f:
                f.write(contents)
            print("image saved")


        ch.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(on_message_callback=self.callback, queue=self.queue_name)
        print(f'[{self.device_name}] Start Consuming')
        self.channel.start_consuming()
        

    def publish(self, message):
        self.channel.basic_publish(
            exchange='output',
            routing_key='toMongoDB',
            body=pickle.dumps(message)
        )
        

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--smartq_id', help='smartq_id argument')
    parser.add_argument('--device_name', help='device_name argument')
    
    args, unknown = parser.parse_known_args()
    
    smartQ_email = args.smartq_id
    device_name = args.device_name

    process = Device(device_name=device_name, smartQ_email=smartQ_email)
    process.consume()

    
