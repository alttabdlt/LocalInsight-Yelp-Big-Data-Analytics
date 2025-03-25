package localinsight.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * MapReduce job for analyzing temporal patterns in reviews and check-ins.
 */
public class TemporalPatterns {

  /**
   * Mapper class to extract temporal data from reviews.
   */
  public static class TemporalMapper extends Mapper<Object, Text, Text, Text> {
    
    private SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat dayOfWeekFormat = new SimpleDateFormat("EEEE");
    private SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        JSONObject record = new JSONObject(value.toString());
        
        // Check if this is a review or check-in record
        String type = "";
        String businessId = "";
        Date date = null;
        
        if (record.has("business_id") && record.has("date")) {
          // This is a review
          type = "review";
          businessId = record.getString("business_id");
          date = inputFormat.parse(record.getString("date"));
        } else if (record.has("business_id") && record.has("time")) {
          // This is a check-in
          type = "checkin";
          businessId = record.getString("business_id");
          
          // Handle check-in time data (format may vary)
          JSONObject timeData = record.getJSONObject("time");
          // For simplicity, we're using a dummy date for check-ins
          // In reality, you would parse the check-in time structure
          date = new Date();
        } else {
          // Skip other record types
          return;
        }
        
        if (date != null) {
          String dayOfWeek = dayOfWeekFormat.format(date);
          String hour = hourFormat.format(date);
          
          // Output temporal data (business_id as key)
          String temporalData = String.join("|", type, dayOfWeek, hour);
          context.write(new Text(businessId), new Text(temporalData));
        }
      } catch (JSONException | ParseException e) {
        context.getCounter("Data Cleaning", "Corrupted Records").increment(1);
      }
    }
  }
  
  /**
   * Reducer class to create temporal profiles by business ID.
   */
  public static class TemporalReducer extends Reducer<Text, Text, Text, Text> {
    
    private static final String[] DAYS_OF_WEEK = {
      "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    };
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      
      // Count occurrences by day and hour
      Map<String, Integer> reviewsByDay = new HashMap<>();
      Map<String, Integer> reviewsByHour = new HashMap<>();
      Map<String, Integer> checkinsByDay = new HashMap<>();
      Map<String, Integer> checkinsByHour = new HashMap<>();
      
      // Initialize counters
      for (String day : DAYS_OF_WEEK) {
        reviewsByDay.put(day, 0);
        checkinsByDay.put(day, 0);
      }
      
      for (int hour = 0; hour < 24; hour++) {
        String hourStr = String.format("%02d", hour);
        reviewsByHour.put(hourStr, 0);
        checkinsByHour.put(hourStr, 0);
      }
      
      // Process all temporal records
      for (Text value : values) {
        String[] fields = value.toString().split("\\|");
        String type = fields[0];
        String dayOfWeek = fields[1];
        String hour = fields[2];
        
        if (type.equals("review")) {
          reviewsByDay.put(dayOfWeek, reviewsByDay.getOrDefault(dayOfWeek, 0) + 1);
          reviewsByHour.put(hour, reviewsByHour.getOrDefault(hour, 0) + 1);
        } else if (type.equals("checkin")) {
          checkinsByDay.put(dayOfWeek, checkinsByDay.getOrDefault(dayOfWeek, 0) + 1);
          checkinsByHour.put(hour, checkinsByHour.getOrDefault(hour, 0) + 1);
        }
      }
      
      // Create day profile strings
      StringBuilder dayProfile = new StringBuilder();
      for (String day : DAYS_OF_WEEK) {
        if (dayProfile.length() > 0) {
          dayProfile.append(",");
        }
        dayProfile.append(day).append(":")
            .append(reviewsByDay.getOrDefault(day, 0)).append("/")
            .append(checkinsByDay.getOrDefault(day, 0));
      }
      
      // Create hour profile strings
      StringBuilder hourProfile = new StringBuilder();
      for (int hour = 0; hour < 24; hour++) {
        String hourStr = String.format("%02d", hour);
        if (hourProfile.length() > 0) {
          hourProfile.append(",");
        }
        hourProfile.append(hourStr).append(":")
            .append(reviewsByHour.getOrDefault(hourStr, 0)).append("/")
            .append(checkinsByHour.getOrDefault(hourStr, 0));
      }
      
      // Combine profiles
      String result = "day_profile:" + dayProfile.toString() 
          + "|hour_profile:" + hourProfile.toString();
      
      context.write(key, new Text(result));
    }
  }
  
  /**
   * Main method to configure and run the MapReduce job.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TemporalPatterns <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Temporal Pattern Analysis");
    
    job.setJarByClass(TemporalPatterns.class);
    job.setMapperClass(TemporalMapper.class);
    job.setReducerClass(TemporalReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}