package org.okkam.flink.mongodb.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat;
import org.apache.flink.hadoopcompatibility.mapred.HadoopOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

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
		DataSet<Tuple2< Text, BSONWritable>> fin = input.map(
				new MapFunction<Tuple2<BSONWritable, BSONWritable>, Tuple2<Text,BSONWritable> >() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Text,BSONWritable> map(Tuple2<BSONWritable, BSONWritable> record) throws Exception {
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
		
		MongoConfigUtil.setOutputURI( hdIf.getJobConf(), "mongodb://localhost:27017/test.testData");
		// emit result (this works only locally)
		fin.output(new HadoopOutputFormat<Text,BSONWritable>(new MongoOutputFormat<Text,BSONWritable>(), hdIf.getJobConf()));

		// execute program
		env.execute("Mongodb Example");
	}
}