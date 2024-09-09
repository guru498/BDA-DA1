package com.hadoop.aadhar.driver;

import com.hadoop.aadhar.comparator.SortComparator;
import com.hadoop.aadhar.mapper.SortMapper;
import com.hadoop.aadhar.mapper.UIDMapper;
import com.hadoop.aadhar.reducer.SortReducer;
import com.hadoop.aadhar.reducer.UIDReducer;
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

public class AadharAnalysisDriverTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapDriver<LongWritable, Text, IntWritable, Text> mapSortDriver;
    ReduceDriver<IntWritable, Text, Text, IntWritable> reduceSortDriver;
    private MapReduceDriver mapReduceDriver;
    private MapReduceDriver mapReduceSortDriver;

    /**
     *
     */
    @Before
    public void setUp() {

        UIDMapper mapper = new UIDMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);

        UIDReducer reducer = new UIDReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        SortMapper mapperSort = new SortMapper();
        mapSortDriver = MapDriver.newMapDriver();
        mapSortDriver.setMapper(mapperSort);

        SortReducer reducerSort = new SortReducer();
        reduceSortDriver = ReduceDriver.newReduceDriver(reducerSort);

        mapReduceSortDriver = MapReduceDriver.newMapReduceDriver(mapperSort, reducerSort);
        mapReduceSortDriver.setKeyOrderComparator(new SortComparator());
    }

    @Test
    public void testSort() throws Exception {

        mapReduceDriver.withInput(new LongWritable(0), new Text("Registrar,Enrolment Agency,State,District,Sub District,Pin Code,Gender,Age,Aadhaar generated,Enrolment"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Allahabad,Meja,212303,F,7,1,0,0,1"));
        mapReduceDriver.withInput(new LongWritable(2), new Text("Allahabad Bank,Asha Security Guard Services,Uttar Pradesh,Sonbhadra,Robertsganj,231213,M,8,1,0,0,0"));
        mapReduceDriver.withInput(new LongWritable(3), new Text("Allahabad Bank,SGS INDIA PVT LTD,Uttar Pradesh,Sultanpur,Sultanpur,227812,F,13,1,0,0,1"));

        List<Pair<Text, IntWritable>> output = mapReduceDriver.run();
        Assert.assertEquals(3, output.size());
        Assert.assertEquals(new Text("(A-Onerealtors Pvt Ltd, 1)"), new Text(output.get(0).toString()));
        Assert.assertEquals(new Text("(Asha Security Guard Services, 1)"), new Text(output.get(1).toString()));
        Assert.assertEquals(new Text("(SGS INDIA PVT LTD, 1)"), new Text(output.get(2).toString()));

        mapReduceSortDriver.withInput(new LongWritable(0), new Text("The quick,1234"));
        mapReduceSortDriver.withInput(new LongWritable(1), new Text("The brown fox,5678"));
        mapReduceSortDriver.withInput(new LongWritable(1), new Text("The brown fox,2678"));

        List<Pair<IntWritable, Text>> outputSort = mapReduceSortDriver.run();
        Assert.assertEquals(3, outputSort.size());
        Assert.assertEquals(new IntWritable(5678), outputSort.get(0).getSecond());
        Assert.assertEquals(new IntWritable(2678), outputSort.get(1).getSecond());
        Assert.assertEquals(new IntWritable(1234), outputSort.get(2).getSecond());
    }
}
