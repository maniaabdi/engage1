Hadoop 2.7 and Rados-Gateway Integration using S3A
==============================================

The hadoop-aws module can be used for Hadoop and Rados-Gateway communication. You need to apply a few changes to convince hadoop to talk with radosgw. 





Instructions
------------------
- Install Hadoop 2.7+. For the information about Hadoop installation, please visit :  https://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-common/SingleCluster.html

- Create a RGW user for S3 Access. For more information please visit: http://docs.ceph.com/docs/master/radosgw/config/
The values of keys->access_key and keys->secret_key are needed for access validatio.

- Add the following keys to your core-site.xml file:
```xml

<property>
  <name>fs.s3a.access.key</name>
  <value>...</value>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <value>...</value>
</property>

<property>
  <name>fs.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>

<property>
  <name>fs.s3a.endpoint</name>
<value> host:port</value>
</property>

<property>
  <name>fs.s3a.connection.ssl.enabled</name>
<value>false</value>
</property>

```


Service-Specific Errors and Warnings

```ini
log4j.logger.com.amazonaws=ERROR
log4j.logger.com.amazonaws.http.AmazonHttpClient=ERROR
log4j.logger.org.apache.hadoop.fs.s3a.S3AFileSystem=WARN
log4j.logger.com.amazonaws.request=DEBUG
```

Copy the jar's needed for the S3A libraries:
```
cp share/hadoop/tools/lib/jackson-* share/hadoop/hdfs/lib/
cp share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar share/hadoop/hdfs/lib/
cp share/hadoop/tools/lib/hadoop-aws-2.7.0.jar share/hadoop/hdfs/lib/
```


You should now be able to run commands:

```shell
$ hadoop fs -ls s3a://bucketname/foo
```
