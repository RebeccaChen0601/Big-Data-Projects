package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.lang.Math;
import java.net.URI;
import java.util.*;
import java.io.IOException;
import java.util.*;
import java.lang.Math;
import java.util.HashMap;
import java.util.HashSet;
import java.lang.Float;
import java.lang.Integer;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    private static final class MyMapper1 extends Mapper<LongWritable,  Text, Text, FloatWritable> {
        private static final Text WORD = new Text();
        private static final FloatWritable ONE = new FloatWritable(1);
        private int word_limit = 40;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            HashSet<String> set = new HashSet();
            WORD.set("*");
            context.write(WORD, ONE);
            for (int i = 0; i < Math.min(tokens.size(), word_limit); i++) {
                String token = tokens.get(i);
                if (!set.contains(token)) {
                    set.add(tokens.get(i));
                    WORD.set(tokens.get(i));
                    context.write(WORD, ONE);
                }
            }
        }
    }

    private static final class MyReducer1 extends
            Reducer<Text, FloatWritable, Text, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<FloatWritable> iter = values.iterator();
            int sum = 0;

            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);

        }
    }

    private static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final FloatWritable ONE = new FloatWritable(1);
        private static int word_limit = 40;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            if (tokens.size() < 2) return;

            HashSet<String> hash_set = new HashSet();
            hash_set.addAll(tokens.subList(0, Math.min(tokens.size(), word_limit)));


            for (String first : hash_set) {
                for (String second : hash_set) {
                    if (first.equals(second)) continue;
                    PAIR.set(first, second);
                    context.write(PAIR, ONE);
                }
            }
        }
    }

    private static final class MyCombiner extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyReducer2 extends
            Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();
        private int threshold = 1;
        HashMap<String, Float> wordToCount;


        private static HashMap<String, Float> getFreqMap(Context context) {
            HashMap<String, Float> wordToCount = new HashMap<String, Float>();
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                URI[] cacheFiles = context.getCacheFiles();
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri.toString());
                    FSDataInputStream fsin = fs.open(path);
                    DataInputStream in = new DataInputStream(fsin);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] listLine = line.split("\t");
                        if (listLine.length == 2) {
                            wordToCount.put(listLine[0], Float.parseFloat(listLine[1]));
                        }
                    }
                    br.close();
                    in.close();
                    fsin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return wordToCount;
        }

        @Override
        public void setup(Context context) {
            threshold = context.getConfiguration().getInt("threshold", 1);
            wordToCount = getFreqMap(context);
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<FloatWritable> iter = values.iterator();
            float pair_sum = 0;

            while (iter.hasNext()) {
                pair_sum += iter.next().get();

            }
            float first_sum = wordToCount.get(key.getLeftElement());
            float second_sum = wordToCount.get(key.getRightElement());
            float num_line = wordToCount.get("*");

            if (pair_sum >= threshold) {
                VALUE.set((float)(Math.log10(pair_sum * num_line / (first_sum * second_sum))), (int) pair_sum);
                context.write(key, VALUE);

            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "line threshold of co-occurrence")
        int threshold = 1;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);
        LOG.info(" - threshold: " + args.threshold);

        Job job1 = Job.getInstance(getConf());
        job1.setJobName(PairsPMI.class.getSimpleName());
        job1.setJarByClass(PairsPMI.class);


        // set threshold
        job1.getConfiguration().setInt("threshold", args.threshold);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job1.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(args.output + "-job1"));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


        job1.setMapperClass(MyMapper1.class);
        job1.setCombinerClass(MyReducer1.class);
        job1.setReducerClass(MyReducer1.class);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // Gather all outputs files of the first job
        FileSystem fs = FileSystem.get(job1.getConfiguration());
        FileStatus[] fileList = fs.listStatus(new Path(args.output + "-job1"),
                new PathFilter(){
                    @Override public boolean accept(Path path){
                        return path.getName().startsWith("part-");
                    }
                } );

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(PairsPMI.class.getSimpleName());
        job2.setJarByClass(PairsPMI.class);

        job2.getConfiguration().setInt("threshold", args.threshold);

        // Delete the output directory if it exists already.
        outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        // Store the output file of the first job into the cache
        for(int i=0; i < fileList.length;i++){
            job2.addCacheFile(fileList[i].getPath().toUri());
        }

        job2.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(FloatWritable.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(PairOfFloatInt.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job2.setMapperClass(MyMapper2.class);
        job2.setCombinerClass(MyCombiner.class);
        job2.setReducerClass(MyReducer2.class);

        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // Delete the tmp folder
        outputDir = new Path(args.output + "-job1");
        FileSystem.get(getConf()).delete(outputDir, true);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}