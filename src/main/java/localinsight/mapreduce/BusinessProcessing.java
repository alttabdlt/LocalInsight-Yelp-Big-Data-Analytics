package localinsight.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * MapReduce job for processing business data from the Yelp dataset.
 * Extracts business attributes and organizes them by location.
 */
public class BusinessProcessing {

  /**
   * Mapper class to extract key business attributes from JSON data.
   */
  public static class BusinessMapper extends Mapper<Object, Text, Text, Text> {
    
    /**
     * Extracts the main category from a business's category list.
     * Returns the first category or "uncategorized" if none exists.
     */
    private String extractMainCategory(JSONObject business) throws JSONException {
      if (business.has("categories")) {
        JSONArray categories = business.getJSONArray("categories");
        if (categories.length() > 0) {
          return categories.getString(0);
        }
      }
      return "uncategorized";
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        JSONObject business = new JSONObject(value.toString());
        String businessId = business.getString("business_id");
        String name = business.getString("name");
        String category = extractMainCategory(business);
        double stars = business.getDouble("stars");
        int reviewCount = business.getInt("review_count");
        
        // Extract location information
        JSONObject location = business.getJSONObject("location");
        String city = location.getString("city");
        String state = location.getString("state");
        
        // Output by location (key: state:city, value: business details)
        String locationKey = state + ":" + city;
        String businessDetails = String.join("|", 
            businessId, name, category, String.valueOf(stars), String.valueOf(reviewCount));
        
        context.write(new Text(locationKey), new Text(businessDetails));
      } catch (JSONException e) {
        context.getCounter("Data Cleaning", "Corrupted Records").increment(1);
      }
    }
  }
  
  /**
   * Reducer class to organize businesses by location.
   */
  public static class BusinessReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      
      StringBuilder businessList = new StringBuilder();
      for (Text value : values) {
        if (businessList.length() > 0) {
          businessList.append("\n");
        }
        businessList.append(value.toString());
      }
      
      context.write(key, new Text(businessList.toString()));
    }
  }
  
  /**
   * Main method to configure and run the MapReduce job.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: BusinessProcessing <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Business Processing");
    
    job.setJarByClass(BusinessProcessing.class);
    job.setMapperClass(BusinessMapper.class);
    job.setReducerClass(BusinessReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}