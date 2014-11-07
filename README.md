Accessing Data Stored in MongoDB  with Apache Flink 0.7+!
===================

Starting from the post at https://flink.incubator.apache.org/news/2014/01/28/querying_mongodb.html here at [Okkam](http://www.okkam.it) we played around withthe new **Apache Flink APIs (0.7+)** and we manage to make a simple mapreduce example.

----------

pom.xml
-------------
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>
	<groupId>org.okkam.flink</groupId>
	<artifactId>flink-mongodb-test</artifactId>
	<version>1.0-SNAPSHOT</version>

	<properties>
		<flink.version>0.7.0-hadoop2-incubating</flink.version>
		<mongodb.hadoop.version>1.3.0</mongodb.hadoop.version>
		<hadoop.version>2.4.0</hadoop.version>
	</properties>
	<build>
		<plugins>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.1</version>
				<configuration>
					<source>1.7</source>
					<target>1.7</target>
				</configuration>
			</plugin>
		</plugins>
	</build>
	<dependencyManagement>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>
	</dependencyManagement>
	<dependencies>
		<!-- Force dependency management for hadoop-common -->
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-hadoop-compatibility</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-java</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-clients</artifactId>
			<version>${flink.version}</version>
			<scope>test</scope>
		</dependency>

		<dependency>
			<groupId>org.mongodb</groupId>
			<artifactId>mongo-hadoop-core</artifactId>
			<version>${mongodb.hadoop.version}</version>
		</dependency>

	</dependencies>

</project>
```
> **Note:**

> - Change ``dbname`` and ``collectioname`` accordingly to your database
> - In the map function read fields you need (e.g. ``jsonld``)
> - Change the output coordinates of the job (default ``test.testData``)


----------


Java code
-------------------

This is a simple code to connecto to a local MongoDB instance:

```java
package org.okkam.flink.mongodb.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;

public class MongodbExample {
	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// create a MongodbInputFormat, using a Hadoop input format wrapper
		HadoopInputFormat<BSONWritable, BSONWritable> hdIf = 
				new HadoopInputFormat<BSONWritable, BSONWritable>(new MongoInputFormat(),
						BSONWritable.class, BSONWritable.class,	new JobConf());
	
		// specify connection parameters
		hdIf.getJobConf().set("mongo.input.uri", 
				"mongodb://localhost:27017/dbname.collectioname");

		DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);
		// a little example how to use the data in a mapper.
		DataSet<Tuple2< Text, BSONWritable>> fin = input.map(
				new MapFunction<Tuple2<BSONWritable, BSONWritable>, 
									Tuple2<Text,BSONWritable> >() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Text,BSONWritable> map(
							Tuple2<BSONWritable, BSONWritable> record) throws Exception {
						BSONWritable value = record.getField(1);
						BSONObject doc = value.getDoc();
						BasicDBObject jsonld = (BasicDBObject) doc.get("jsonld");
						
						String id = jsonld.getString("@id");
						DBObject builder = BasicDBObjectBuilder.start()
				                .add("id", id)
				                .add("type", jsonld.getString("@type"))
				                .get();

				        BSONWritable w = new BSONWritable(builder);
		                return new Tuple2<Text,BSONWritable>(new Text(id), w);
					}
				});

		// emit result (this works only locally)
//		fin.print();
		
		MongoConfigUtil.setOutputURI( hdIf.getJobConf(), 
				"mongodb://localhost:27017/test.testData");
		// emit result (this works only locally)
		fin.output(new HadoopOutputFormat<Text,BSONWritable>(
				new MongoOutputFormat<Text,BSONWritable>(), hdIf.getJobConf()));

		// execute program
		env.execute("Mongodb Example");
	}
}

```

----------


Run the project
-------------------

The easyest way to test the program is to clone the git repository and import the project in Eclipse and then run **MongodbExample** class

Written by Okkam s.r.l. [@okkamit](https://twitter.com/okkamit)
