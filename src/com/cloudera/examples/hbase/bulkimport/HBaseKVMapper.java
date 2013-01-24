package com.cloudera.examples.hbase.bulkimport;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Mapper Class
 */
public class HBaseKVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] SRV_COL_FAM = "m".getBytes();
	// Number of fields in text file
	final static int NUM_FIELDS = 3;

	CSVParser csvParser = new CSVParser();
	String tableName = "";

	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();

		tableName = c.get("hbase.table.name");
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null;

		// Failed to parse line
		try {
			fields = csvParser.parseLine(value.toString());
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		// Wrong number of fields in a line
		if (fields.length != NUM_FIELDS) {
			context.getCounter("HBaseKVMapper", "INVALID_FIELD_LEN").increment(
					1);
			return;
		}

		hKey.set(String.format("%s", fields[0]).getBytes());

		// If field exists
		if (!fields[1].equals("")) {

			// Save KeyValue Pair
			kv = new KeyValue(hKey.get(), SRV_COL_FAM,
					HColumnEnum.SRV_COL_B.getColumnName(), fields[1].getBytes());
			// Write KV to HBase
			context.write(hKey, kv);
		}

		if (!fields[2].equals("")) {
			// Save KeyValue Pair
			kv = new KeyValue(hKey.get(), SRV_COL_FAM,
					HColumnEnum.SRV_COL_C.getColumnName(), fields[2].getBytes());
			// Write KV to HBase
			context.write(hKey, kv);
		}

		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);

	}
}
