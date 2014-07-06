package com.awcoleman.BouncyCastleGenericCDRHadoopWithWritable;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 
 * Input Format for "Simple Generic CDR"
 * Reads in entire ASN.1 file as key.
 * 
 * @author awcoleman
 * @version 20140702
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 * 
 */
public class RawFileAsBinaryInputFormat extends FileInputFormat<Text, CallDetailRecord> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename){
		return false;
	}

	@Override
	public RecordReader<Text, CallDetailRecord> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RawFileRecordReader();
	}
}
