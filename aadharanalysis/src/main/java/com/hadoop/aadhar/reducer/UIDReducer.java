package com.hadoop.aadhar.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UIDReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(UIDReducer.class);

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : value)
            sum += val.get();
        LOG.info("Key : {} , Value : {}", key.toString(), sum);
        context.write(key, new IntWritable(sum));
    }
}
