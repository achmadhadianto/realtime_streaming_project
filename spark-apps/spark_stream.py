"""

pip install cassandra-driver
# pip install spark pyspark => ini salah ya
# hapus pip uninstall spark

pip install pyspark => ketinggian 4.0, 
# check => pip list | grep spark
#downgrade
pip install pyspark==3.5.1

check and download dr mvn repository 'spark-cassandra-connector' dan 'spark-sql-kafka'

"""

import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster #fungsi ini akan di pakai sehingga Cluster tidak tertukar
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid


def create_keyspace(session):
    #create keyspace here #dengan koneksi session tadi, buat ini
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """) ##setingkat database/schema di rdbms

    print('Keyspace created successfully')

def create_table(session):
    #create table here
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT )
    """) ##ddl tabel yang dibuat

    print("Table created successfully")


# ini untuk testing manual, kwargs itu untuk unpack data yang dibungkus dict
# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')


    # try:
    #     user_id = uuid.UUID(user_id)  # konversi sebelum insert    
    #     session.execute("""
    #         INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
    #             post_code, email, username, dob, registered_date, phone, picture)
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     """, (user_id, first_name, last_name, gender, address,
    #           postcode, email, username, dob, registered_date, phone, picture))
    #     logging.info(f"Data inserted for {first_name} {last_name}")

    #     # insert_data(session, **fake_data)

    # except Exception as e:
    #     # logging.error(f"Could not insert data due to {e}") #non aktifkan dulu, ganti print
    #     print(f"[DEBUG] Logging failed with error: {e}")  # Gunakan print untuk debug
    #     raise e #untuk stop jika error


# data = {
#     "id": str(uuid4()),
#     "first_name": "Hadi",
#     "last_name": "Sutanto",
#     "gender": "male",
#     "address": "Jalan Kenangan No. 7",
#     "post_code": "12345",
#     "email": "hadi@example.com",
#     "username": "hadikeren",
#     "dob": "1995-01-01T00:00:00Z",
#     "registered_date": "2020-01-01T00:00:00Z",
#     "phone": "081234567890",
#     "picture": "https://example.com/picture.jpg"
# }

# insert_data(session, **data)
    # fake_data = {
    #     "id": "1051c876-41f3-4a73-ab5a-14ce9f7d3ac6",
    #     "first_name": "Felix",
    #     "last_name": "Evans",
    #     "gender": "male",
    #     "address": "1163 Hoon Hay Road, Porirua, Waikato, New Zealand",
    #     "post_code": "38829",
    #     "email": "felix.evans@example.com",
    #     "username": "greenswan623",
    #     "dob": "1973-06-10T04:13:01.554Z",
    #     "registered_date": "2016-05-14T05:07:19.167Z",
    #     "phone": "(185)-271-6028",
    #     "picture": "https://randomuser.me/api/portraits/med/men/2.jpg"
    #     }

def create_spark_connection():
    # creating spark connection
    s_conn = None

    try:
        # s_conn = SparkSession.builder \
        #     .appName('SparkDataStreaming') \
        #     .config('spark.jars.packages',  "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
        #                                     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0" ) \
        #     .config('spark.cassandra.connection.host', 'localhost') \
        #     .getOrCreate() #ini untuk test local, spark.jars.packages ambil dari maven repository
        
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate() #langsung ambil dari folder mount, sesuaikan dengan nama file
            #.config('spark.cassandra.connection.host', 'localhost') \ ini, harusnya bukan localhost, tapi merujuk ke cassandra container
            #format ini akan jadi format spark_conn, format key, value
            #tambahkan semua drivernya, masih error sama, tidak terbaca

                    #     .config('spark.jars.packages', 
                    # "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                    # "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                    # "org.apache.kafka:kafka-clients:3.4.0") \
                    ### bagian ini saat run manual saja
                    
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    

    return s_conn    

def connect_to_kafka(spark_conn):
    #broker
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        #ganti dari 'localhost:9092' ke 'broker:29092'
    return spark_df ##formatnya dari key value sudah menjadi dataframe

def create_cassandra_connection():
    try:

        # connecting to the cassandra cluster
        # cluster = Cluster(['localhost']) #fungsi cassandra #ingat di atas from cassandra.cluster import Cluster 
        cluster = Cluster(['cassandra']) 

        cas_session = cluster.connect()

        return cas_session
    
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel ##hasilnya dari dataframe sudah jadi structured data frame, ada schema nya


if __name__ == "__main__":
    #create spark connection
    print("Starting Spark session")
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        #connect to kafka with spark connection
        print("Reading from Kafka topic")
        spark_df = connect_to_kafka(spark_conn) #hasil connection tadi di pakai untuk connect kafka
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection() #session ini akan dipakai untuk create keyspace dan table 

        if session is not None:
            print("Transforming DataFrame")
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")
            # print("Writing to Cassandra")
            # insert_data(session) #disini masalahnya, insert tanpa parameter

            # def write_to_cassandra(batch_df, batch_id): #memproses setiap micro-batch secara manual menggunakan kode Python standar
            #     print(f"Processing batch {batch_id}")
            #     batch_df.show()
            #     try:
            #         batch_df.write \
            #             .format("org.apache.spark.sql.cassandra") \
            #             .mode("append") \
            #             .option("keyspace", "spark_streams") \
            #             .option("table", "created_users") \
            #             .save()
            #     except Exception as e:
            #         print(f"Failed to write batch {batch_id}: {e}")
                    
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                            .option('checkpointLocation', '/opt/bitnami/spark/checkpoints/spark_streaming')
                            .option('keyspace', 'spark_streams')
                            .option('table', 'created_users')
                            .option("failOnDataLoss", "false") #memberitahu Spark agar tidak mati total kalau ada data yang gagal
                            .start()) #stand by ambil data dari kafka, isi ke keyspace dan table di cassandra (pada format org.apache.spark.sql.cassandra)
                            #checkpoint untuk menyimpan data sementara proses spark, semisal darimana, sampai mana
                            #detailkan nama folder check point, Hanya 1 stream job menggunakan path checkpoint ini — jangan sharing checkpoint folder antar job.
                            #meski sudah di tulis pada create_spark_connection, tetap harus di tulis ulang saat writeStream
            try:
              streaming_query.awaitTermination()
            except KeyboardInterrupt:
                print("Stopping stream gracefully...")
                streaming_query.stop()



"""
###ANALYZE

#pertama run error
25/08/04 00:23:44 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)
com.datastax.spark.connector.writer.NullKeyColumnException: Invalid null value for key column id

#check cassandra schema
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042

#check tabel
describe spark_streams.created_users;

select * from spark_streams.created_users;

#ingat, test manual local berbeda kebutuhan depedency dengan airflow scheduler

#di tutorial insert di skip dulu setelah table selesai di create, streaming query juga di skip dulu

data['id'] = uuid.uuid4()
# ternyata di generate dari dag pertama, ada tambahan kode ini

# bagaimana cara delete data yg terlanjur tanpa id? check dulu
docker exec -it broker bash
#lalu
find / -name kafka-topics.sh 2>/dev/null
#tidak ada, bahkan ls pun tidak ada

#solusi, test:
docker run --rm -it --network container:broker confluentinc/cp-kafka:7.4.0 \
  kafka-topics --bootstrap-server localhost:9092 --list
#delete topic
docker run --rm -it --network container:broker confluentinc/cp-kafka:7.4.0 \
  kafka-topics --bootstrap-server localhost:9092 --delete --topic users_created

#cari folder jar
ls -lah /home/hadi/.ivy2.5.2/jars | grep spark-cassandra
#atau juga di subfolder lain
find . -type f -name "*spark-cassandra*"
#tidak ada, jadi tambahkan manual, meski tadi insert sudah bisa

#kalau jalan via docker
spark-submit --master spark://localhost:7077 spark_stream.py

#kalau mau jalan local
spark-submit --master local[*] spark_stream.py ##atau [1] atau [4] 1 atau 4 core cpu, * berarti semua

##pertama run gagal karena file jar tidak ditemukan, debug:
docker exec -it realtime_streaming_project-spark-master-1 bash
ls -lah /opt/spark/jars/

#check folder nya
docker exec -it realtime_streaming_project-spark-master-1 bash
I have no name!@04ad86da1270:/opt/bitnami/spark$ ls
LICENSE  NOTICE  R  README.md  RELEASE  bin  conf  conf.default  data  examples  hive-jackson  jars  kubernetes  licenses  logs  python  sbin  tmp  venv  work  yarn
I have no name!@04ad86da1270:/opt/bitnami/spark$ 

#test manual
spark-submit \
  --master spark://localhost:7077 \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,/opt/spark/jars/spark-cassandra-connector_2.13-3.5.1.jar \
  /opt/spark-apps/spark_stream.py ##gagal

##harus dibedakan jalan dari dalam container
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,/opt/spark/jars/spark-cassandra-connector_2.13-3.5.1.jar \
  /opt/spark-apps/spark_stream.py

##dengan jalan dari local
spark-submit \
  --master spark://localhost:7077 \
  --jars /home/hadi/.ivy2.5.2/jars/org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.0.jar,/home/hadi/.ivy2.5.2/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar \
  ./spark-apps/spark_stream.py

##saat nama tidak persis di folder, errornya:
25/08/05 23:54:57 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
25/08/05 23:54:57 WARN DependencyUtils: Local jar /home/hadi/.ivy2.5.2/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar does not exist, skipping.
25/08/05 23:54:57 WARN DependencyUtils: Local jar /home/hadi/.ivy2.5.2/jars/spark-cassandra-connector_2.13-3.5.1.jar does not exist, skipping.
Starting Spark session

##setelah namanya persis di folder:
-oke tadi sudah hilang, lanjut error:
ERROR SparkContext: Failed to add /opt/spark/jars/spark-cassandra-connector_2.13-3.5.1.jar to Spark environment
java.io.FileNotFoundException: Jar /opt/spark/jars/spark-cassandra-connector_2.13-3.5.1.jar not found

##setelah di script di samakan persis dengan nama di folder, error masih sama:
Failed to add /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar to Spark environment
java.io.FileNotFoundException: Jar /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar not found

##di container sudah ada padahal, dan namanya persis
-rw-rw-r-- 1 1000 1000 457K May 19 11:02 org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.0.jar
-rw-rw-r-- 1 1000 1000 1.6M Jun 25  2024 com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar

##test siapa ini

#buat folder dan beri permission untuk mount folder checkpoint
mkdir -p /home/hadi/projects/spark_checkpoints/master/spark_streaming
mkdir -p /home/hadi/projects/spark_checkpoints/worker/spark_streaming

sudo chown -R 1000:1000 /home/hadi/projects/spark_checkpoints/master/spark_streaming
sudo chown -R 1000:1000 /home/hadi/projects/spark_checkpoints/worker/spark_streaming

spark-submit --master spark://localhost:7077 ./spark-apps/spark_stream.py

# masih error
25/08/06 21:15:44 ERROR SparkContext: Failed to add /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar to Spark environment
java.io.FileNotFoundException: Jar /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar not found
#padahal di foldernya ada dan sudah diberi akses
docker exec -it realtime_streaming_project-spark-master-1 bash
ls -lah /opt/spark/jars/

-rw-rw-r-- 1 1000 1000 1.1M Jun 25  2024 com.datastax.spark_spark-cassandra-connector-driver_2.13-3.5.1.jar

#coba test beri permission penuh
docker exec -u root -it realtime_streaming_project-spark-master-1 bash
chmod 755 /opt/spark/jars

tambah lagi permission
chmod 644 /opt/spark/jars/*.jar

##masih juga error 
25/08/06 22:07:11 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/08/06 22:07:11 ERROR SparkContext: Failed to add /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar to Spark environment
java.io.FileNotFoundException: Jar /opt/spark/jars/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar not found

#ganti tarik online, sesuaikan broker kafka
#ganti mount sesuai bitnami
#masuk dan jalankan script di dalam docker
- /home/hadi/.ivy2.5.2:/root/.ivy2
chmod -R a+rX /home/hadi/.ivy2.5.2

docker exec -it realtime_streaming_project-spark-master-1 bash

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

#masih error 
error basedir must be absolute

Spark gunakan ~/.ivy2 atau dari HOME
Tapi kalau user tidak punya nama (I have no name!) dan HOME tidak jelas → path jadi ?/.ivy2..., yang dianggap relative → error

#setting root dan home
docker exec -e SPARK_USER=root -e HOME=/root -it realtime_streaming_project-spark-master-1 bash
#tidak apa apa permission denied
export HOME=/root
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

#masih bermasalah, tambahkan spark.jars.ivy
docker exec -it realtime_streaming_project-spark-master-1 bash
export HOME=/root
/opt/bitnami/spark/bin/spark-submit \
  --conf spark.jars.ivy=/root/.ivy2 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

#oke berhasil, sekarang errornya ModuleNotFoundError: No module named 'cassandra'

Scala / Java-based seperti Spark-Cassandra Connector	.jar	Lewat --jars atau --packages
Python-native (PyPI) seperti cassandra-driver	.whl atau pip module	Harus di-install via pip (tidak bisa hanya .jar)

#buat image custom
docker build -t custom-spark-cassandra .

#masih kurang kafka connector, tambahkan di perintah bash
docker exec -it realtime_streaming_project-spark-master-1 bash

export HOME=/root
/opt/bitnami/spark/bin/spark-submit \
  --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

#sekarang permission denied
sudo chown -R 1001:1001 /home/hadi/.ivy2.5.2
# oke masalah ini sudah clear, lanjut

#downgrade kafka
/opt/bitnami/spark/bin/spark-submit \
  --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

###HOME itu untuk directory case seperti ini
# Set environment variables
export IVY_HOME=/tmp/.ivy2
export SPARK_HOME=/opt/bitnami/spark
mkdir -p /tmp/.ivy2

# Jalankan dengan spark-submit
spark-submit \
  --master local[*] \
  --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
  /opt/spark-apps/spark_stream.py

##jadi masalahnya adalah filenya ada, tapi dalam bentuk mount volume, padahal docker butuhnya image dalam 1 kesatuan
##jadi harus buat image custom dengan depedency bawaan

##docker tidak bisa perintah COPY dari folder lain, harus copy file dulu ke folder project

mkdir -p jars
cp -r ~/.ivy2.5.2/jars/* jars/

#beri permission
sudo chown -R 1001:1001 jars/

#beri ownership yang benar di dalam image
RUN chown -R 1001:1001 /opt/bitnami/spark/jars_custom

#karena docker config di script sudah di hapus, karena gagal download dalam private network docker, maka
#jalankan saat spark-submit
docker exec -it realtime_streaming_project-spark-master-1 bash

#karena sudah tidak pakai --packages lagi, home bisa di hapus
export HOME=/root
/opt/bitnami/spark/bin/spark-submit \
  --conf spark.jars.ivy=/root/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_stream.py

#ganti dengan ini
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/bitnami/spark/jars_custom/com.datastax.spark_spark-cassandra-connector_2.13-3.5.1.jar,/opt/bitnami/spark/jars_custom/org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.0.jar \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

  #coba test load semua
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
   --jars $(echo /opt/spark/jars_custom/*.jar | tr ' ' ',') \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

#masih ada error baru: WARNING:root:Kafka dataframe could not be created because: Failed to find data source: kafka.
--packages
    Sumber: Mengunduh JAR dari Maven repository (online).
    Format: groupId:artifactId:version, contoh:
    org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0
    Tujuan utama:
        Mempermudah penggunaan dependency tanpa harus download manual.
        Dependency di-cache di $HOME/.ivy2/jars secara otomatis.

--jars
    Sumber: Mengambil JAR dari lokasi lokal, contohnya:
--jars /opt/spark/jars_custom/spark-sql-kafka-0-10_2.13-4.0.0.jar
Tujuan utama:
    Menyediakan JAR secara manual.
    Berguna untuk lingkungan offline atau ketika kamu sudah punya JAR-nya.
    Bisa digunakan untuk JAR kustom atau build sendiri.

##oke jadi pakai packages juga agar download dari maven repo, masalahnya karena kadang Kafka tidak akan jalan tanpa semua transitive JAR tersebut, dan Spark tidak akan memberi tahu persis JAR mana yang hilang.
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  --jars $(echo /opt/spark/jars_custom/*.jar | tr ' ' ',') \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

#tadi salah masih pakai localhost, sudah di ganti ke broker

##oke masih error, kita tambahkan yang tadi sudah di download
spark-cassandra-connector_2.13-3.5.1.jar
spark-sql-kafka-0-10_2.13-4.0.0.jar

downgrade ke 3.5.1 semua
FROM bitnami/spark:3.5.1
/media/hadi/DATA/project/realtime_streaming_project/spark-sql-kafka-0-10_2.13-3.5.1.jar
/media/hadi/DATA/project/realtime_streaming_project/spark-sql-kafka-0-10_2.13-4.0.0.jar

##masih ada error
java.lang.NoClassDefFoundError: scala/$less$colon$less
Caused by: java.lang.ClassNotFoundException: scala.$less$colon$less

#runtime scala nya tidak jalan, solusi tambahkan library scala
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  --packages org.apache.kafka:kafka-clients:3.7.0 \
  --packages org.scala-lang:scala-library:2.13.13 \

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  --packages org.apache.kafka:kafka-clients:3.7.0 \
  --packages org.scala-lang:scala-library:2.13.13 \
  --jars $(echo /opt/spark/jars_custom/*.jar | tr ' ' ',') \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

# coba cek versi scala
spark-shell --version
#versi 3.5.1
Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 17.0.12

#pakai package saja
  --jars $(echo /opt/spark/jars_custom/*.jar | tr ' ' ',') \
  
#Sekarang errornya, scala versi 2.12, tapi scalanya jar nya versi 2.13 -> downgrade
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --packages org.apache.kafka:kafka-clients:3.7.0 \
  --packages org.scala-lang:scala-library:2.12.13 \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

#solusi final full package work 0905 07082025
docker exec -it realtime_streaming_project-spark-master-1 bash

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.7.0,\
org.scala-lang:scala-library:2.12.18,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=cassandra \
  /opt/spark-apps/spark_stream.py

  http://localhost:8080
  http://localhost:9021
  http://localhost:8082
  http://localhost:9090 

"""
