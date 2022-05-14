package com.exercise;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CellphoneTrafficMapper extends Mapper<LongWritable, Text, Text, CellphoneTraffic> {


	@Override
	protected void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		String[] strings = line.toString().split("\t");
		CellphoneTraffic cellphoneTrafficBean = new CellphoneTraffic(strings[1].trim(), Long.parseLong(strings[8].trim()), Long.parseLong(strings[9].trim()));
		context.write(new Text(strings[1]), cellphoneTrafficBean);
	}

}
