package com.hadoop.aadhar.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(SortReducer.class);

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
            LOG.info("State : {} , Count : {}", value.toString(), key.get());
        }
    }
}
