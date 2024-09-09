package com.hadoop.aadhar.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SortReducerTest {
    ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        SortReducer reducer = new SortReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testReducer() throws Exception {
        List<Text> listData = new ArrayList<>();
        List<String> testList;
        try (Stream<String> stream = Files.lines(Paths.get("src/test/resources/aadharTest.csv"))) {
            testList = stream.collect(Collectors.toList());
        } catch (IOException e) {
            throw new Exception();
        }
        for (String list : testList) {
            String[] listArr = list.split(",");
            listData.add(new Text(listArr[0]));
            reduceDriver.withOutput(new Text(listArr[0]), new IntWritable(1234));
        }
        reduceDriver.setInput(new IntWritable(1234), listData);
        reduceDriver.runTest();
    }
}
