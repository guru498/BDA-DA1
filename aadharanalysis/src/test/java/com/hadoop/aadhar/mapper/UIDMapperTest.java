package com.hadoop.aadhar.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UIDMapperTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        UIDMapper mapper = new UIDMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testMapper() throws Exception {
        List<String> testList;
        try (Stream<String> stream = Files.lines(Paths.get("src/test/resources/aadharTest.csv"))) {
            testList = stream.collect(Collectors.toList());
        } catch (IOException e) {
            throw new Exception();
        }
        for (String list : testList) {
            String[] listArr = list.split(",");
            mapDriver.withInput(new LongWritable(1), new Text(list));
            mapDriver.addOutput(new Text(listArr[1]), new IntWritable(Integer.parseInt(listArr[8])));
            mapDriver.runTest();
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testMapperToSkipFirstRow() throws Exception {
        mapDriver.withInput(new LongWritable(0), new Text(""));
        mapDriver.runTest();
    }
}
