/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import com.opencsv.CSVParser;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageDelayOfFlight {
    // global counter to count

  public enum FLIGHT_COUNTER{
    TOTAL_DELAY,
    TOTAL_FLIGHTS,
  }
  public static class FlightMapper extends Mapper<Object, Text, Text, Text> {
    private CSVParser csvparser = new CSVParser();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//    private Text outputKey = new Text();
//    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String cols[] = csvparser.parseLine(value.toString());
//      StringBuilder str = new StringBuilder();
      try {
        if (isValidFlight(cols) && cols != null && cols.length > 1){
          if (cols[11].toLowerCase().equals("ord")){
            context.write(new Text(cols[5] + "," + cols[17].toLowerCase()),
                new Text(cols[11].toLowerCase() + "," + cols[24]+","+cols[35]+","+cols[37]));
          }else if(cols[17].toLowerCase().equals("jfk")){
            context.write(new Text(cols[5] + "," + cols[11].toLowerCase()),
                new Text(cols[17].toLowerCase() + "," + cols[24]+","+cols[35]+","+cols[37]));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }


    }
    private boolean isValidFlight(String cols[]) throws Exception {
      if (isActiveFlight(cols) && isTwoValidLegFlight(cols) && isValidDateRange(cols)){
        return true;
      }
      return false;
    }

    private boolean isTwoValidLegFlight(String cols[]){
      if(cols[11].toLowerCase().equals("ord") || cols[17].toLowerCase().equals("jfk")){
        if (cols[11].toLowerCase().equals("ord") && cols[17].toLowerCase().equals("jfk")) {
          return false;
        } else {
          return true;
        }
      }else{
        return false;
      }
    }
    private boolean isActiveFlight(String cols[]){
      if(cols[41].contains(("1")) && cols[43].contains("1")){
        return false;
      }
      return true;
    }
    private boolean isValidDateRange(String cols[]) throws Exception{
      boolean result = false;
      try {
        //test
        Date startDate = dateFormat.parse("2007-12-01");
        Date endDate = dateFormat.parse("2008-01-01");
//        Date startDate = dateFormat.parse("2007-06-01");
//        Date endDate = dateFormat.parse("2008-06-01");
        Date flightDate = dateFormat.parse(cols[5]);
        result = flightDate.after(startDate) && flightDate.before(endDate);
      }catch(ParseException e){
        e.printStackTrace();
      } finally {
        return result;
      }
    }
  }

  public static class flightReducer
      extends Reducer<Text, Text, Text, Text> {
    private int totalFlights;
    private float totalDelay;

    protected void setup(Context context) throws IOException, InterruptedException {
      this.totalDelay = 0;
      this.totalFlights = 0;
    }

    public void reduce(Text key, Iterable<Text> values,
        Context context
    ) throws IOException, InterruptedException {
      List<String> flights1 = new ArrayList<String>();
      List<String> flights2 = new ArrayList<String>();

      for (Text val : values){
        String flight = val.toString();
        if (flight.contains("ord")){
          flights1.add(flight);
        } else if(flight.contains("jfk")){
          flights2.add(flight);
        }
      }
      for (String f1 : flights1){
        for (String f2 : flights2){
          String[] flight1 = f1.split(",");
          String[] flight2 = f1.split(",");
          if ()
        }
      }

    }
    private boolean isSameDate(){

    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.getCounter(FLIGHT_COUNTER.TOTAL_DELAY).increment((long)totalDelay);
      context.getCounter(FLIGHT_COUNTER.TOTAL_FLIGHTS).increment(totalFlights);
    }
  }
  // line index : array index : attribute
    // 11 : 0 : Origin
    // 17 : 1 : Destination
    // 5  : 2 : FlightDate
    // 24 : 3 : DepTime
    // 35 : 4 : ArrTime
    // 37 : 5 : ArrDelayMinutes

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: avgFlightDelay <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Avg flight delay");
    job.setJarByClass(AverageDelayOfFlight.class);
    job.setMapperClass(FlightMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
