package com.awcoleman.BouncyCastleGenericCDRHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * Basic Hadoop Driver (with Mapper and Reducer included) for "Simple Generic CDR"
 * 
 * 
 * @author awcoleman
 * @version 20140522
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 * 
 */
public class BasicDriverMapReduce extends Configured implements Tool {

    public static class BasicMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }
    public static class BasicReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        private long total = 0;
        public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
        	for (LongWritable val : values) {
        		total += val.get();
            }
            context.write(key, new LongWritable(total)); 
        }
    }
    
    public int run(String[] args) throws Exception {
    
		if (args.length < 2 ) {
			System.out.println("Missing input and output filenames. Exiting.");
			System.exit(1);
		}
		
		Job job = new Job(super.getConf());
		job.setJarByClass(BasicDriverMapReduce.class);
		job.setJobName("BasicDriver1");
        job.setMapperClass(BasicMapper.class);
        job.setReducerClass(BasicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(RawFileAsBinaryInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    	return job.waitForCompletion(true) ? 0 : 1;
    }
    
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int ret = ToolRunner.run(conf, new BasicDriverMapReduce(), args);
		System.exit(ret);

	}

}
