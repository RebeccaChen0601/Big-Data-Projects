package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import tl.lin.data.map.HashMapWritable;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataInputStream;
import java.util.HashSet;
import java.lang.Math;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.util.*;
import java.lang.Float;
import java.lang.Integer;

public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    private static final class MyMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable> {
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
            float sum = 0;

            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);

        }
    }


    protected static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HashMapWritable<Text, FloatWritable>> {
        private static final Text TEXT = new Text();
        private static final FloatWritable ONE = new FloatWritable(1);
        private int word_limit = 40;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Map<String, HashMapWritable<Text, FloatWritable>> stripes = new HashMap<String, HashMapWritable<Text, FloatWritable>>();
            List<String> tokens = Tokenizer.tokenize(value.toString());

            if (tokens.size() < 2) return;
            HashSet<String> hash_set = new HashSet<>();
            hash_set.addAll(tokens.subList(0, Math.min(tokens.size(), word_limit)));

            for (int i = 0; i < hash_set.size(); i++) {
                for (int j = 0; j < hash_set.size(); j++) {
                    String prev = tokens.get(i);
                    String cur = tokens.get(j);
                    if (stripes.containsKey(prev)) {
                        HashMapWritable<Text, FloatWritable> stripe = stripes.get(prev);
                        if (stripe.containsKey(cur)) {
                            stripe.put(TEXT, new FloatWritable(stripe.get(cur).get()+1.0f));
                        } else {
                            TEXT.set(cur);
                            stripe.put(TEXT, ONE);
                        }
                    } else {
                        HashMapWritable<Text, FloatWritable> stripe = new HashMapWritable();
                        
                        TEXT.set(cur);
                        stripe.put(TEXT, ONE);
                        TEXT.set(prev);
                        stripes.put(TEXT, stripe);
                    }
                }
            }

            for (Text t : stripes.keySet()) {
                context.write(t, stripes.get(t));
            }
        }
    }

    private static final class MyCombiner extends Reducer<Text, HashMapWritable<Text, FloatWritable>, Text, HashMapWritable<Text, FloatWritable>> {
        @Override
        public void reduce(Text key, Iterable<HashMapWritable<Text, PairOfFloatInt>> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HashMapWritable<Text, FloatWritable>> iter = values.iterator();
            HashMapWritable<Text, FloatWritable> map = new HashMapWritable<Text, FloatWritable>();

            while (iter.hasNext()) {
                map.putAll(iter.next());
            }

            context.write(key, map);
        }
    }

    private static final class MyReducer2 extends Reducer<Text, HashMapWritable<Text, FloatWritable>, Text, HashMapWritable<Text, PairOfFloatInt>> {
        private int threshold = 1;
        private static final Text WORD = new Text();
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();
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
        public void reduce(Text key, Iterable<HashMapWritable<Text, FloatWritable>> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HashMapWritable<Text, FloatWritable>> iter = values.iterator();
            HashMapWritable<Text, PairOfFloatInt> map = new HashMapWritable<Text, PairOfFloatInt>();
            HashMapWritable<Text, FloatWritable> map = new HashMapWritable<Text, FloatWritable>();
            float sum = 0.0f;
            while (iter.hasNext()) {
                map.putAll(iter.next());
            }
            

            float first_sum = wordToCount.get(key.get());
            float second_sum;
            float num_line = wordToCount.get("*");

            if (sum >= threshold) {
                for (Text term : map.keySet()) {
                    second_sum = wordToCount.get(term);
                    VALUE.set((float)(Math.log10(sum * num_line / (first_sum * second_sum))), (int) sum);
                    WORD.set(term);
                    map.put(WORD, VALUE);
                }
            

                context.write(key, map);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private StripesPMI() {}

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

        LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - threshold: " + args.threshold);

        Job job1 = Job.getInstance(getConf());
        job1.setJobName(StripesPMI.class.getSimpleName());
        job1.setJarByClass(StripesPMI.class);

        job1.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job1, new Path(args.input));
        FileOutputFormat.setOutputPath(job1, new Path(args.output+"-job1"));

        job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        job1.getConfiguration().setInt("threshold", args.threshold);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        job1.setMapperClass(MyMapper1.class);
        job1.setCombinerClass(MyReducer1.class);
        job1.setReducerClass(MyReducer1.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

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
        job2.setJobName(StripesPMI.class.getSimpleName());
        job2.setJarByClass(StripesPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        // Store the output file of the first job into the cache
        for(int i=0; i < fileList.length;i++){
            job2.addCacheFile(fileList[i].getPath().toUri());
        }

        // Delete the output directory if it exists already.
        outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.getConfiguration().setInt("threshold", args.threshold);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(HashMapWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(HashMapWritable.class);
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
        ToolRunner.run(new StripesPMI(), args);
    }
}
