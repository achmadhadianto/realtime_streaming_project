from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator #providers.standard. saat test manual
import uuid #harus di taruh disini #sebelumnya error Object of type UUID is not JSON serializable

default_args = {
    'owner' : 'hadi_data_engineer',
    'start_date' : datetime(2025, 7, 30, 22, 00)
}

def get_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    # print(res.json()) -#menamgpilkan dalam format json
    res = res.json()
    res = res['results'][0] #ambil hanya result, tanpa info
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4()) #ubah menjadi string
    data['first_name'] = res['name']['first'] #object bersarang
    data['last_name'] = res['name']['last'] #object bersarang
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}" #tanpa str tetap valid
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    import time
    from kafka import KafkaProducer
    import logging

    # print("Starting stream_data function...")
    
    # try:
    #     # Test import first
    #     from kafka import KafkaProducer
    #     print("Kafka import successful")
    # except ImportError as e:
    #     print(f"Failed to import Kafka: {str(e)}")
    #     raise
    
    # try:
    #     print("Creating Kafka producer...")
    #     producer = KafkaProducer(
    #         bootstrap_servers=['broker:29092'], 
    #         max_block_ms=5000,
    #         value_serializer=lambda v: json.dumps(v).encode('utf-8')
    #     )
    #     print("Kafka producer created successfully")
        
    #     curr_time = time.time()
    #     message_count = 0
        
    #     while True:
    #         if time.time() > curr_time + 60:  # 1 minute
    #             print(f"Streaming completed. Sent {message_count} messages")
    #             break
                
    #         try:
    #             print(f"Processing message {message_count + 1}...")
    #             res = get_data()
    #             formatted_data = format_data(res)
                
    #             print("Sending message to Kafka...")
    #             future = producer.send('users_created', formatted_data)
    #             result = future.get(timeout=10)
    #             print(f"Message sent successfully: {result}")
                
    #             message_count += 1
    #             time.sleep(2)  # 2 second delay
                
    #         except Exception as e:
    #             print(f'Error in streaming loop: {str(e)}')
    #             time.sleep(5)
    #             continue
                
    # except Exception as e:
    #     print(f"Fatal error in stream_data: {str(e)}")
    #     raise
    # finally:
    #     try:
    #         if 'producer' in locals():
    #             print("Closing Kafka producer...")
    #             producer.flush()
    #             producer.close()
    #             print("Kafka producer closed")
    #     except:
    #         pass

    # res = get_data()
    # res = format_data(res)
    # print(json.dumps(res, indent=3))

    # producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) #membuat publish / pengirim data #localhost:9092 saat test manual kafka
    # producer.send('users_created', json.dumps(res).encode('utf-8')) #topic users_created

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute #mengirim data sebanyak mungkin selama 1 menit
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            # continue #ada error tetap sukses status task nya
            raise  # biar task gagal beneran

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data) #script test tadi dipanggil daily interval

# stream_data()


"""

#analyze

    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    # print(res.json()) -#menamgpilkan dalam format json
    res = res.json()
    res = res['results'][0] #ambil hanya result, tanpa info
    # print(res) #berhasil tapi tampilan jelek
    print(json.dumps(res, indent=3)) #tampilan bagus, sudah standar


    name = "Hadi"
    print(f"Hello, {name}!") #Hello, Hadi!

    # mount volume, buat folder nya dulu
    mkdir -p /home/hadi/projects/zk-data /home/hadi/projects/zk-log /home/hadi/projects/kafka-data
    #beri permission
    sudo chown -R 1000:1000 /home/hadi/projects/zk-data
    sudo chown -R 1000:1000 /home/hadi/projects/zk-log
    sudo chown -R 1000:1000 /home/hadi/projects/kafka-data

    #control-center lebih lama dari kafka-ui

    #check port yang aktif pakai 8081
    sudo lsof -i :8081
    sudo netstat -tuln | grep 8081

    #mengapa berbeda test manual python dengan airflow ke kafka
    Kalau kirim data ke Kafka dari Airflow (di container): gunakan broker:29092
    Kalau kirim data ke Kafka dari laptop lokal (misal pakai kafka-console-producer di luar Docker): gunakan localhost:9092

    chmod +x script/entrypoint.sh

    chmod +x logs ##beri izin akses
    sudo chmod -R 777 logs ##beri izin baca tulis

    ##masih masalah
    PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'

    ##masih gagal, cek id
    docker exec -it realtime_streaming_project-webserver-1 id ##sudah mati

    sudo chown -R 1000:0 logs
    sudo chmod -R 777 logs

    ##masih gagal, coba beri owner ke airflow
    sudo chown -R 50000:0 logs
    sudo chmod -R 755 logs
 
    #setelah di cek owner masih hadi
    ls -ld logs
    drwxr-xr-x 1 hadi hadi 0 Aug  3 13:46 logs

    #trouble lama, harus taruh di folder C untuk writeable, kalau hanya read bisa di D

    sudo chown -R 1000:1000 /home/hadi/projects/logs ##test gagal
    sudo chown -R 50000:0 /home/hadi/projects/logs #karena milik airflow #yaps berhasil

    #buat foldernya, buat volume mountnya, lalu beri akses writeable
    sudo chown -R 1000:1000 /home/hadi/projects/schema_registry_data
    sudo chown -R 1000:1000 /home/hadi/projects/control_center_data

    #check id container
    docker exec -it <nama-container> id

    #memang airflow beda sendiri uid nya, demi keamanan default pakai non root

mkdir -p /home/hadi/projects/spark_data/master
mkdir -p /home/hadi/projects/spark_logs/master
mkdir -p /home/hadi/projects/spark_data/worker
mkdir -p /home/hadi/projects/spark_logs/worker
mkdir -p /home/hadi/projects/cassandra_data

sudo chown -R 1000:1000 /home/hadi/projects/spark_data/master
sudo chown -R 1000:1000 /home/hadi/projects/spark_logs/master
sudo chown -R 1000:1000 /home/hadi/projects/spark_data/worker
sudo chown -R 1000:1000 /home/hadi/projects/spark_logs/worker
sudo chown -R 1000:1000 /home/hadi/projects/cassandra_data

chmod -R a+rX /home/hadi/.ivy2.5.2/jars
-R: recursive ke semua file dan subfolder
a+rX: semua user (user, group, others) dapat:
    r: read file
    X: execute folder (dan file yang sudah executable)

#read/write/execute
mkdir -p ./spark-apps
sudo chmod -R 777 ./spark-apps
#tanpa write
chmod -R 755 ./spark-apps


- /home/hadi/.ivy2.5.2:/root/.ivy2
chmod -R a+rX /home/hadi/.ivy2.5.2

"""
