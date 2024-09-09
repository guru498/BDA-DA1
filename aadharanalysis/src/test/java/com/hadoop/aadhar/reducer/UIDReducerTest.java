package com.hadoop.aadhar.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UIDReducerTest {
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        UIDReducer reducer = new UIDReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testReducer() throws Exception {
        List<IntWritable> list = new ArrayList<>();
        list.add(new IntWritable(1234));
        list.add(new IntWritable(5678));
        reduceDriver.setInput(new Text("Transmoovers India"), list);
        reduceDriver.withOutput(new Text("Transmoovers India"), new IntWritable(6912));
        reduceDriver.runTest();
    }
}
