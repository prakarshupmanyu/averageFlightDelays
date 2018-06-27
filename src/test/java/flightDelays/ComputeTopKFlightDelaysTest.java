package flightDelays;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ComputeTopKFlightDelaysTest {

    private MapDriver<LongWritable, Text, Text, Text> averageFlightDelayMapDriver;
    private ReduceDriver<Text, Text, Text, DoubleWritable> averageFlightDelayReduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> averageFlightDelayMapReduceDriver;

    private MapDriver<LongWritable, Text, NullWritable, Text> topkFlightDelayMapDriver;
    private ReduceDriver<NullWritable, Text, NullWritable, Text> topkFlightDelayReduceDriver;
    private MapReduceDriver<LongWritable, Text, NullWritable, Text, NullWritable, Text> topkFlightDelayMapReduceDriver;

    @Before
    public void setUp() {
        ComputeTopKFlightDelays.AverageFlightDelayMapper mapper = new ComputeTopKFlightDelays.AverageFlightDelayMapper();
        ComputeTopKFlightDelays.AverageFlightDelayReducer reducer = new ComputeTopKFlightDelays.AverageFlightDelayReducer();
        ComputeTopKFlightDelays.AverageFlightDelayCombiner combiner = new ComputeTopKFlightDelays.AverageFlightDelayCombiner();

        averageFlightDelayMapDriver = MapDriver.newMapDriver(mapper);
        averageFlightDelayReduceDriver = ReduceDriver.newReduceDriver(reducer);
        averageFlightDelayMapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        averageFlightDelayMapReduceDriver.setCombiner(combiner);

        ComputeTopKFlightDelays.TopKFlightDelaysMapper mapper2 = new ComputeTopKFlightDelays.TopKFlightDelaysMapper();
        ComputeTopKFlightDelays.TopKFlightDelaysReducer reducer2 = new ComputeTopKFlightDelays.TopKFlightDelaysReducer();

        topkFlightDelayMapDriver = MapDriver.newMapDriver(mapper2);
        topkFlightDelayReduceDriver = ReduceDriver.newReduceDriver(reducer2);
        topkFlightDelayMapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper2, reducer2);

        Configuration conf = new Configuration();
        conf.set("numLargestFlightDelaysRequired", "1");
        topkFlightDelayMapDriver.withConfiguration(conf);
        topkFlightDelayReduceDriver.withConfiguration(conf);
        topkFlightDelayMapReduceDriver.withConfiguration(conf);
    }

    @Test
    //Test case when input is the header of CSV file
    public void testAverageFlightDelayMapperCase0() throws IOException {
        String input = "Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).runTest();
    }

    @Test
    //Test case with valid data
    public void testAverageFlightDelayMapperCase1() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        Text outputKey = new Text("ATL-PHX");
        Text outputValue = new Text("7_1");

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case with negative arrival delay
    public void testAverageFlightDelayMapperCase2() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,-7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        Text outputKey = new Text("ATL-PHX");
        Text outputValue = new Text("-7_1");

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case when Origin is NA
    public void testAverageFlightDelayMapperCase3() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,-7,0,NA,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).runTest();
    }

    @Test
    //Test case when Dest is NA
    public void testAverageFlightDelayMapperCase4() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,-7,0,ATL,NA,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).runTest();
    }

    @Test
    //Test case when ArrDelay is NA
    public void testAverageFlightDelayMapperCase5() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,NA,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).runTest();
    }

    @Test
    //Test case when everything else is NA
    public void testAverageFlightDelayMapperCase6() throws IOException {
        String input = "NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,-7,NA,ATL,PHX,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        Text outputKey = new Text("ATL-PHX");
        Text outputValue = new Text("-7_1");

        averageFlightDelayMapDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case when average is 0
    public void testAverageFlightDelayReducerCase0() throws IOException {
        Text inputKey = new Text("ATL-PHX");
        ImmutableList<Text> inputValue = ImmutableList.of(new Text("7_3"), new Text("-7_1"));

        Text outputKey = new Text("ATL-PHX");
        DoubleWritable outputValue = new DoubleWritable(0);

        averageFlightDelayReduceDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case when average is positive
    public void testAverageFlightDelayReducerCase1() throws IOException {
        Text inputKey = new Text("ATL-PHX");
        ImmutableList<Text> inputValue = ImmutableList.of(new Text("7_3"), new Text("-2_1"));

        Text outputKey = new Text("ATL-PHX");
        DoubleWritable outputValue = new DoubleWritable(1.25);

        averageFlightDelayReduceDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case when average is negative
    public void testAverageFlightDelayReducerCase2() throws IOException {
        Text inputKey = new Text("ATL-PHX");
        ImmutableList<Text> inputValue = ImmutableList.of(new Text("-7_3"), new Text("-2_1"));

        Text outputKey = new Text("ATL-PHX");
        DoubleWritable outputValue = new DoubleWritable(-2.25);

        averageFlightDelayReduceDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case with more number of inputs
    public void testAverageFlightDelayReducerCase3() throws IOException {
        Text inputKey = new Text("ATL-PHX");
        ImmutableList<Text> inputValue = ImmutableList.of(new Text("7_3"), new Text("-2_1"), new Text("10_1"));

        Text outputKey = new Text("ATL-PHX");
        DoubleWritable outputValue = new DoubleWritable(3);

        averageFlightDelayReduceDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Random test case
    public void testAverageFlightDelayReducerCase4() throws IOException {
        Text inputKey = new Text("ATL-PHX");
        ImmutableList<Text> inputValue = ImmutableList.of(new Text("7_3"), new Text("-2_2"));

        Text outputKey = new Text("ATL-PHX");
        DoubleWritable outputValue = new DoubleWritable(1);

        averageFlightDelayReduceDriver.withInput(inputKey, inputValue).withOutput(outputKey, outputValue).runTest();
    }

    @Test
    //Test case for map reduce job 1 as a whole
    public void testAverageFlightDelayMapReducerCase0() throws IOException {
        String input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        LongWritable inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay";
        inputKey = new LongWritable(1);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,NA,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,NA,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,NA,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,77,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,2,0,PHX,ATL,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,-3,0,BOS,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,-1,0,BOS,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        input = "2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,0,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA";
        inputKey = new LongWritable(0);
        averageFlightDelayMapReduceDriver.withInput(inputKey, new Text(input));

        List<Pair<Text, DoubleWritable>> result = averageFlightDelayMapReduceDriver.run();

        Pair route1 = new Pair<Text, DoubleWritable>(new Text("ATL-PHX"), new DoubleWritable(28));
        Pair route2 = new Pair<Text, DoubleWritable>(new Text("BOS-PHX"), new DoubleWritable(-2));
        Pair route3 = new Pair<Text, DoubleWritable>(new Text("PHX-ATL"), new DoubleWritable(2));

        Assert.assertEquals(3, result.size());

        Assert.assertTrue(result.contains(route1));
        Assert.assertTrue(result.contains(route2));
        Assert.assertTrue(result.contains(route3));
    }

    @Test
    //Test case with valid data
    public void testTopKFlightDelayMapperCase1() throws IOException {

        String input = "ALT-PHX\t-2.056";
        LongWritable inputKey = new LongWritable(0);
        Text inputValue = new Text(input);

        Text outputValue = new Text(input);

        topkFlightDelayMapDriver.withInput(inputKey, inputValue).withOutput(NullWritable.get(), outputValue).runTest();
    }

    @Test
    //Test case with valid data
    public void testTopKFlightDelayReducerCase1() throws IOException {
        String input = "ALT-PHX\t-2.056";
        ImmutableList<Text> inputValue = ImmutableList.of(new Text(input));

        Text outputValue = new Text(input);

        topkFlightDelayReduceDriver.withInput(NullWritable.get(), inputValue).withOutput(NullWritable.get(), outputValue).runTest();
    }

    @Test
    //Test case with valid data
    public void testTopKFlightDelayMapReduceeCase1() throws IOException{
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(0), new Text("ALT-PHX\t-2.056"));
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(1), new Text("ALT-PHX\t-2.0"));
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(2), new Text("ALT-PHX\t3.056"));
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(3), new Text("ALT-PHX\t2.056"));
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(4), new Text("ALT-PHX\t-12.056"));
        topkFlightDelayMapReduceDriver.withInput(new LongWritable(5), new Text("ALT-PHX\t12.056"));
        List<Pair<NullWritable, Text>> result = topkFlightDelayMapReduceDriver.run();

        Assert.assertEquals(1, result.size());

        Pair<NullWritable, Text> output = new Pair<>(NullWritable.get(), new Text("ALT-PHX\t12.056"));
        Assert.assertTrue(result.contains(output));
    }
}
