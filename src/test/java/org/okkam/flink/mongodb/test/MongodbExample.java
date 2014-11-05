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
		HadoopInputFormat<BSONWritable, BSONWritable> hdIf = new HadoopInputFormat<BSONWritable, BSONWritable>(
				new MongoInputFormat(), BSONWritable.class, BSONWritable.class,	new JobConf());
	
		// specify connection parameters
		hdIf.getJobConf().set("mongo.input.uri", "mongodb://localhost:27017/dbname.collectioname");

		DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);
		// a little example how to use the data in a mapper.
		DataSet<String> fin = input.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<BSONWritable, BSONWritable> record) throws Exception {
						BSONWritable value = record.getField(1);
						BSONObject doc = value.getDoc();
						BasicDBObject jsonld = (BasicDBObject) doc.get("jsonld");
//						String someotherfield = (String) doc.get("someotherfield");
						String type = jsonld.getString("@type");
						return type;
					}
				});

		// emit result (this works only locally)
		fin.print();

		// execute program
		env.execute("Mongodb Example");
	}
}