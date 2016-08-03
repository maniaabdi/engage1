Hadoop 2.7 and S3A Integration
==============================================

The Apache Hadoop "S3A" works with Rados-Gateway


 Here is a small but complete example, using a single-node hadoop system.



Add the following keys to your core-site.xml file:

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
