# Create ORC File From HDFS
Spark create ORC file from HDFS tutorial. This tutorial using `spark-submit` command to execute projecthit, so I build plugins executable jar.<br/>
***Note**: If you want to run this project in console, the you must create your `hive-site.xml` in classpath and configuration for embedded metastore_db.

## Before You Start
1. Start Server
   ```bash
   $ hadoop/sbin/start-dfs.sh
   $ hadoop/sbin/start-yarn.sh
   ```
2. Create ORC Table in Hive Shell
   ```sql
   create table user_orc (
     id int,
     name string,
     email string
   ) stored as orc tblproperties("orc.compress"="zlib");
   ```
