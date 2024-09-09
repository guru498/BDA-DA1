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

public class SortMapperTest {
    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

    /**
     *
     */
    @Before
    public void setUp() {
        SortMapper mapper = new SortMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testMapper() throws Exception {
        List<String> testList;
        try (Stream<String> stream = Files.lines(Paths.get("src/test/resources/sortMapperData.txt"))) {
            testList = stream.collect(Collectors.toList());
        } catch (IOException e) {
            throw new Exception();
        }
        for (String list : testList) {
            String[] listArr = list.split(",");
            mapDriver.withInput(new LongWritable(), new Text(list));
            mapDriver.addOutput(new IntWritable(Integer.parseInt(listArr[1])), new Text(listArr[0]));
            mapDriver.runTest();
        }
    }
}
