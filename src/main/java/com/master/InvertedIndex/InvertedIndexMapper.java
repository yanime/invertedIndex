package com.master.InvertedIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    private Text filename = new Text();
    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    private boolean caseSensitive = false;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();

        String line = value.toString();
        filename = new Text(filenameStr + key);

        if (!caseSensitive) {
            line = line.toLowerCase();
        }
        Text currentWord;
        for (String word : WORD_BOUNDARY.split(line)) {
            if (word.isEmpty() || patternsToSkip.contains(word)) {
                continue;
            }
            currentWord = new Text(word);
            context.write(currentWord,filename);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.caseSensitive = conf.getBoolean("wordcount.case.sensitive",false);
        if (context.getInputSplit() instanceof FileSplit) {
            this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
        } else {
            this.input = context.getInputSplit().toString();
        }
        if (conf.getBoolean("wordcount.skip.patterns", false)) {
            URI[] localPaths = context.getCacheFiles();
            parseSkipFile(localPaths[0]);
        }
    }

    private void parseSkipFile(URI patternsURI) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
            String pattern;
            while ((pattern = fis.readLine()) != null) {
                patternsToSkip.add(pattern);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                    + patternsURI + "' : " + StringUtils.stringifyException(ioe));
        }
    }
}