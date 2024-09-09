package com.hadoop.aadhar.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UIDMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(UIDMapper.class);
    private Text STATE = new Text();
    private IntWritable COUNT = new IntWritable();

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        if (key.get() == 0)
            LOG.info("Line skipped. Key is {}", key.get());
        else {
            STATE.set(data[1]);
            COUNT.set(Integer.parseInt(data[8]));
            context.write(STATE, COUNT);
            LOG.info("State : {} , Count : {}", STATE.toString(), COUNT.toString());
        }
    }
}
