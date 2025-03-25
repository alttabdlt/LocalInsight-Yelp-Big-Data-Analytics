package localinsight.mapreduce;

import java.io.IOException;
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
 * MapReduce job for processing review data and extracting sentiment indicators.
 */
public class ReviewSentiment {

  /**
   * Mapper class to extract sentiment data from review JSON.
   */
  public static class SentimentMapper extends Mapper<Object, Text, Text, Text> {
    
    // Simple sentiment word lists for demonstration
    private static final String[] POSITIVE_WORDS = {
      "good", "great", "excellent", "amazing", "awesome", "fantastic", "love", "best"
    };
    
    private static final String[] NEGATIVE_WORDS = {
      "bad", "terrible", "awful", "horrible", "poor", "disappointing", "worst", "hate"
    };
    
    /**
     * Simple sentiment analysis by counting positive and negative word occurrences.
     */
    private double analyzeSentiment(String text) {
      text = text.toLowerCase();
      int positiveCount = 0;
      int negativeCount = 0;
      
      for (String word : POSITIVE_WORDS) {
        if (text.contains(word)) {
          positiveCount++;
        }
      }
      
      for (String word : NEGATIVE_WORDS) {
        if (text.contains(word)) {
          negativeCount++;
        }
      }
      
      if (positiveCount + negativeCount == 0) {
        return 0.0; // Neutral
      }
      
      return (double) (positiveCount - negativeCount) / (positiveCount + negativeCount);
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        JSONObject review = new JSONObject(value.toString());
        String businessId = review.getString("business_id");
        String userId = review.getString("user_id");
        int stars = review.getInt("stars");
        String text = review.getString("text");
        
        // Calculate sentiment score
        double sentimentScore = analyzeSentiment(text);
        
        // Output review data (business_id as key)
        String reviewData = String.join("|", 
            userId, 
            String.valueOf(stars), 
            String.valueOf(sentimentScore),
            String.valueOf(text.length()));
        
        context.write(new Text(businessId), new Text(reviewData));
      } catch (JSONException e) {
        context.getCounter("Data Cleaning", "Corrupted Records").increment(1);
      }
    }
  }
  
  /**
   * Reducer class to aggregate sentiment data by business ID.
   */
  public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      
      int totalReviews = 0;
      double totalStars = 0.0;
      double totalSentiment = 0.0;
      int totalTextLength = 0;
      Map<String, Integer> userCounts = new HashMap<>();
      
      for (Text value : values) {
        String[] fields = value.toString().split("\\|");
        String userId = fields[0];
        double stars = Double.parseDouble(fields[1]);
        double sentiment = Double.parseDouble(fields[2]);
        int textLength = Integer.parseInt(fields[3]);
        
        totalReviews++;
        totalStars += stars;
        totalSentiment += sentiment;
        totalTextLength += textLength;
        
        // Count reviews per user
        userCounts.put(userId, userCounts.getOrDefault(userId, 0) + 1);
      }
      
      // Calculate averages
      double avgStars = totalReviews > 0 ? totalStars / totalReviews : 0;
      double avgSentiment = totalReviews > 0 ? totalSentiment / totalReviews : 0;
      double avgTextLength = totalReviews > 0 ? (double) totalTextLength / totalReviews : 0;
      
      // Count unique users
      int uniqueUsers = userCounts.size();
      
      // Create aggregated output
      String result = String.format("%d|%.2f|%.2f|%.2f|%d", 
          totalReviews, avgStars, avgSentiment, avgTextLength, uniqueUsers);
      
      context.write(key, new Text(result));
    }
  }
  
  /**
   * Main method to configure and run the MapReduce job.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ReviewSentiment <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Review Sentiment Analysis");
    
    job.setJarByClass(ReviewSentiment.class);
    job.setMapperClass(SentimentMapper.class);
    job.setReducerClass(SentimentReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}