package com.hadoop.aadhar.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(SortMapper.class);
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
        String dataFromReducer[] = value.toString().split(",");
        STATE.set(dataFromReducer[0].trim());
        COUNT.set(Integer.parseInt(dataFromReducer[1].trim()));
        context.write(COUNT, STATE);
        LOG.info("Count : {} , State : {}", COUNT.get(), STATE.toString());
    }
}
