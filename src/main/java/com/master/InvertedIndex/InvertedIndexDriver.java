package com.master.InvertedIndex;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf());

        for (int i = 0; i < args.length; i += 1) {
            if ("-skip".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                i += 1;
                job.addCacheFile(new Path(args[i]).toUri());
                // this demonstrates logging
            }
        }
        job.setJobName("invertedIndex");
        job.setJarByClass(InvertedIndexDriver.class);
		
		/* Field separator for reducer output*/
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

		/* This line is to accept input recursively */
        FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);


		/* Delete output filepath if already exists */
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        InvertedIndexDriver invertedDriver = new InvertedIndexDriver();
        int res = ToolRunner.run(invertedDriver, args);
        System.exit(res);
    }
}