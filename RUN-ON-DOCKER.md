### Run FiloDB locally with kafka and cassandra on docker

#### Start kafka, zookeeper and cassandra containers
```
docker-compose up
```

#### Create kafka topics 
```
docker exec -it broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev
```
```
docker exec -it broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev-ds-1m
```

#### Generate and import cassandra tables 
```
docker cp /tmp/ddl.cql cassandra:/tmp/
docker exec -it cassandra cqlsh -f /tmp/ddl.cql
```

#### Check cassandra tables
```
docker exec -it cassandra cqlsh -e "DESC TABLES"
```

The next 3 cassandra keyspaces with their tables will be present in the output
```
Keyspace filodb
---------------
prometheus_ingestion_time_index  prometheus_partitionkeys_1
prometheus_pks_by_update_time    prometheus_partitionkeys_2
prometheus_tschunks              prometheus_partitionkeys_3
prometheus_partitionkeys_0     

Keyspace filodb_downsample
--------------------------
prometheus_ds_5_tschunks              prometheus_ds_5_partitionkeys_1
prometheus_ds_1_tschunks              prometheus_ds_5_partitionkeys_0
prometheus_ds_1_ingestion_time_index  prometheus_ds_5_partitionkeys_3
prometheus_ds_5_ingestion_time_index  prometheus_ds_5_partitionkeys_2

Keyspace filodb_admin
---------------------
checkpoints
```
