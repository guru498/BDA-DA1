package com.hadoop.aadhar.comparator;

import com.hadoop.aadhar.mapper.SortMapper;
import com.hadoop.aadhar.reducer.SortReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SortComparatorTest {
    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;
    private MapReduceDriver mapReduceDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        SortMapper mapper = new SortMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);

        SortReducer reducer = new SortReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setKeyOrderComparator(new SortComparator());
    }

    @Test
    public void testSort() throws Exception {

        mapReduceDriver.withInput(new LongWritable(0), new Text("The quick,1234"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("The brown fox,5678"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("The brown fox,2678"));

        List<Pair<IntWritable, Text>> output = mapReduceDriver.run();
        Assert.assertEquals(3, output.size());
        Assert.assertEquals(new IntWritable(5678), output.get(0).getSecond());
        Assert.assertEquals(new IntWritable(2678), output.get(1).getSecond());
        Assert.assertEquals(new IntWritable(1234), output.get(2).getSecond());
    }
}
