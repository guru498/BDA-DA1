package com.hadoop.aadhar.driver;

import com.hadoop.aadhar.comparator.SortComparator;
import com.hadoop.aadhar.mapper.SortMapper;
import com.hadoop.aadhar.mapper.UIDMapper;
import com.hadoop.aadhar.reducer.SortReducer;
import com.hadoop.aadhar.reducer.UIDReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AadharAnalysisDriver extends Configured implements Tool {
    private static final String TEXT_SEPARATOR = "mapreduce.output.textoutputformat.separator";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Aadhar Analysis <input path> <output path1> <output path2>");
            System.exit(-1);
        }
        setStateWiseJob(args);
        return setSortJob(args);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AadharAnalysisDriver(), args);
    }

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void setStateWiseJob(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Job stateWiseCount = Job.getInstance(getConf());
        stateWiseCount.setJobName("Aadhar Analysis");
        stateWiseCount.setJarByClass(AadharAnalysisDriver.class);

        stateWiseCount.getConfiguration().set(TEXT_SEPARATOR, ",");

        FileInputFormat.addInputPath(stateWiseCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(stateWiseCount, new Path(args[1]));

        stateWiseCount.setMapperClass(UIDMapper.class);
        stateWiseCount.setReducerClass(UIDReducer.class);

        stateWiseCount.setOutputKeyClass(Text.class);
        stateWiseCount.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = FileSystem.newInstance(getConf());
        if (fileSystem.exists(new Path(args[1])))
            fileSystem.delete(new Path(args[1]), true);

        stateWiseCount.waitForCompletion(true);
    }

    /**
     * @param args
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public int setSortJob(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Job sort = Job.getInstance(getConf());
        sort.setJobName("Aadhar Analysis Sort");
        sort.setJarByClass(AadharAnalysisDriver.class);

        FileInputFormat.addInputPath(sort, new Path(args[1]));
        FileOutputFormat.setOutputPath(sort, new Path(args[2]));

        sort.setMapperClass(SortMapper.class);
        sort.setSortComparatorClass(SortComparator.class);
        sort.setReducerClass(SortReducer.class);

        sort.setOutputKeyClass(Text.class);
        sort.setOutputValueClass(IntWritable.class);

        sort.setMapOutputKeyClass(IntWritable.class);
        sort.setMapOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.newInstance(getConf());
        if (fileSystem.exists(new Path(args[2])))
            fileSystem.delete(new Path(args[2]), true);

        return sort.waitForCompletion(true) ? 0 : 1;
    }
}
