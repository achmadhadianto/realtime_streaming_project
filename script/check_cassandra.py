from cassandra.cluster import Cluster

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('spark_streams')

rows = session.execute('SELECT * FROM created_users LIMIT 10')
for row in rows:
    print(row)


# from cassandra.cluster import Cluster
# from uuid import UUID

# cluster = Cluster(['localhost'], port=9042)
# session = cluster.connect('spark_streams')

# query = """
#     SELECT * FROM created_users
#     WHERE id = %s
# """
# id_to_check = UUID('2da12651-3b4f-4d14-9dde-95379cf52924')
# row = session.execute(query, (id_to_check,)).one()

# if row:
#     print("Data ditemukan:")
#     print(row)
# else:
#     print("Data tidak ditemukan.")

