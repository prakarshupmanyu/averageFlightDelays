package flightDelays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.TreeMap;

/*
* This class is used to analyze the data of US flights from 2000-2008 and compute the average flight delays
* for every route. Then it finds the top k routes with the largest average flight delays.
*
* This job takes 4 arguments:
*
* 1 - inputDir - path to all the files containing the flight data
* 2 - task1OutputDir - path to store the computed average flight delay for every unique route
* 3 - task2OutputDir - path to store the top k routes with largest average flight delays
* 4 - numLargestFlightDelaysRequired - number to routes to output (eg - if this is 100, we output 100 routes with the
* largest flight delays)
*
* @author = Prakarsh Upmanyu
*
* */

public class ComputeTopKFlightDelays extends Configured implements Tool {

    /*
    * This mapper is reads the given input, decides whether it is to be ignored or further processed and then
    * outputs the route as key and <arrival delay>_<count> as value.
    *
    * Sample Input:
    * CSV header - ..., ArrDelay, ..., Origin, Destination, ...
    *              ..., 21, ........., BOS,    NYC, ........
    *              ..., -29, ........, DCH,    BOS, ......
    *              ..., -3, ........., BOS,    NYC, ........
    *
    * Sample Output:
    * Format - Key, value
    *      BOS-NYC, 21_1
    *      DCH-BOS, -29_1
    *      BOS-NYC, -3_1
    * */

    public static class AverageFlightDelayMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text route = new Text();
        private Text delayAndCount = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] inputWords = value.toString().split(",");
            String origin = inputWords[16];
            String destination = inputWords[17];
            String arrivalDelayStr = inputWords[14];

            //ignore the input when any of the required value is NA
            if(origin.equalsIgnoreCase("NA") || destination.equalsIgnoreCase("NA") || arrivalDelayStr.equalsIgnoreCase("NA")){
                return;
            }

            int arrivalDelayInt = 0;

            //If you cannot parse the arrival delay, ignore - this is the case when header of the CSV file is input
            try{
                arrivalDelayInt = Integer.parseInt(arrivalDelayStr);
            }catch(NumberFormatException e){
                return;
            }
            //create the route key as origin-destination
            route.set(origin + "-" + destination);

            //store the delay and count seen so far as the value for the route key
            delayAndCount.set(arrivalDelayInt + "_1");
            context.write(route, delayAndCount);
        }
    }

    /*
    * This combiner is required to incorporate large data as well as skewed data.
    * It works on the output of AverageFlightDelayMapper sums up the delays and the count of every route seen
    * so far by the Mapper.
    *
    * Sample Input:
    * Format - Key, <value 1, value 2, ....>
    *        BOS-NYC, <21_1, -3_1>
    *        DCH-BOS, <-29_1>
    *
    * Sample Output:
    * Format - Key, value
    *       BOS-NYC, 18_2
    *       DCH-BOS, -29_1
    * */
    public static class AverageFlightDelayCombiner extends Reducer<Text, Text, Text, Text> {

        private Text delaySumAndCountSum = new Text();

        @Override
        public void reduce(Text route, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            long delaySum = 0, countSum = 0;
            for(Text val: values){
                String[] delayAndCount = val.toString().split("_");
                delaySum += Long.parseLong(delayAndCount[0]);
                countSum += Long.parseLong(delayAndCount[1]);
            }
            delaySumAndCountSum.set(delaySum + "_" + countSum);
            context.write(route, delaySumAndCountSum);
        }
    }

    /*
    * This reducer finally computes the average for every route after computing the sum of delays for a route and
    * number of times that route is seen in the data and then computing the average.
    *
    * Sample Input:
    * Format - Key, <value 1, value 2, ....>
    *     BOS-NYC, <18_2, 12_3>
    *     DCH-BOS, <-29_1, 20_2, 0_3>
    *
    * Sample Output:
    * Format - Key, value
    *      BOS-NYC, 6.0
    *      DCH-BOS, -1.5
    * */
    public static class AverageFlightDelayReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable average = new DoubleWritable();

        @Override
        public void reduce(Text route, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            long delaySum = 0, countSum = 0;

            for(Text val: values){
                String[] delayAndCount = val.toString().split("_");
                delaySum += Long.parseLong(delayAndCount[0]);
                countSum += Long.parseLong(delayAndCount[1]);
            }
            double avg = (1.0 * delaySum) / countSum;
            average.set(avg);
            context.write(route, average);
        }
    }

    /*
    * This mapper is used to read in the output from task 1, i.e., the average flight delays for each route.
    * This job is implementing top k design pattern to incorporate big data.
    * It works by storing the k routes with largest average flight delay times per mapper and then passing it on to
    * the SINGLE reducer.
    *
    * */
    public static class TopKFlightDelaysMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private TreeMap<Double, Text> topRecMap = new TreeMap<>();
        private long k;

        //setup function to initialize k for each mapper
        @Override
        public void setup(Context context){
            k = Long.parseLong(context.getConfiguration().get("numLargestFlightDelaysRequired"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) {

            String[] inputWords = value.toString().split("\\t");

            if(inputWords.length != 2){
                return;
            }

            String averageDelayStr = inputWords[1];
            double averageDelay = 0;

            try{
                averageDelay = Double.parseDouble(averageDelayStr);
            }catch(NumberFormatException e){
                return;
            }

            topRecMap.put(averageDelay, new Text(value));

            //ensure that the size of treemap never exceeds k
            if(topRecMap.size() > k){
                topRecMap.remove(topRecMap.firstKey());
            }
        }

        //Cleanup function for each mapper to output top k routes from its corresponding input
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for(Text rec: topRecMap.values()){
                context.write(NullWritable.get(), rec);
            }
        }
    }

    /*
    * This reducer takes in the top k routes from each mapper and finds the top k out of all its input.
    * For this design pattern to work, we need to ensure that we use only one reducer.
    * */
    public static class TopKFlightDelaysReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Double, Text> topRecMap = new TreeMap<>();
        private long k;

        //setup function to initialize k for each mapper
        @Override
        public void setup(Context context){
            k = Long.parseLong(context.getConfiguration().get("numLargestFlightDelaysRequired"));
        }

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            for(Text rec: values){
                String[] inputWords = rec.toString().split("\\t");
                double averageDelay = Double.parseDouble(inputWords[1]);

                topRecMap.put(averageDelay, new Text(rec));

                if(topRecMap.size() > k){
                    topRecMap.remove(topRecMap.firstKey());
                }
            }

            for(Text rec: topRecMap.descendingMap().values()){
                context.write(NullWritable.get(), rec);
            }
        }
    }

    public int run(String args[]) throws Exception{
        Configuration conf = this.getConf();

        Job job1 = new Job(conf, "FindAverageFlightDelayPerRoute");
        Job job2 = new Job(conf, "FindRoutesWithLargestFlightDelay");

        job1.setJarByClass(ComputeTopKFlightDelays.class);
        job2.setJarByClass(ComputeTopKFlightDelays.class);

        String inputDir = conf.get("inputDir");
        String task1OutputDir = conf.get("task1OutputDir");
        String task2OutputDir = conf.get("task2OutputDir");

        FileInputFormat.addInputPath(job1, new Path(inputDir));
        FileOutputFormat.setOutputPath(job1, new Path(task1OutputDir));

        FileInputFormat.addInputPath(job2, new Path(task1OutputDir));
        FileOutputFormat.setOutputPath(job2, new Path(task2OutputDir));

        job1.setMapperClass(AverageFlightDelayMapper.class);
        job1.setCombinerClass(AverageFlightDelayCombiner.class);
        job1.setReducerClass(AverageFlightDelayReducer.class);

        job2.setMapperClass(TopKFlightDelaysMapper.class);
        job2.setReducerClass(TopKFlightDelaysReducer.class);

        //we need one reducer for job 2 to implement top k design pattern
        job2.setNumReduceTasks(1);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        if(!job1.waitForCompletion(true)){
            return 1;
        }
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception{
        int rc = ToolRunner.run(new ComputeTopKFlightDelays(), args);
        System.exit(rc);
    }
}
