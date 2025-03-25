#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Temporal Analysis of Yelp Reviews and Check-ins
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofweek, hour, count, avg, 
    to_timestamp, date_format, when, explode, split
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json

def main():
    """Main execution function for temporal analysis"""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("YelpTemporalAnalysis") \
        .getOrCreate()
    
    # Load business data
    business_df = spark.read.json("hdfs://localhost:9000/user/localinsight/raw/business/business.json")
    
    # Load review data
    reviews_df = spark.read.json("hdfs://localhost:9000/user/localinsight/raw/reviews/review.json")
    
    # Load check-in data
    checkin_df = spark.read.json("hdfs://localhost:9000/user/localinsight/raw/checkin/checkin.json")
    
    # Add timestamp columns to review data
    reviews_with_time = reviews_df.withColumn(
        "review_timestamp", to_timestamp(col("date"))
    ).withColumn(
        "review_year", year(col("review_timestamp"))
    ).withColumn(
        "review_month", month(col("review_timestamp"))
    ).withColumn(
        "review_day", dayofweek(col("review_timestamp"))
    ).withColumn(
        "review_hour", hour(col("review_timestamp"))
    )
    
    # Process check-in data (structure may vary)
    # Check the actual structure of the check-in data first
    print("Checkin Data Schema:")
    checkin_df.printSchema()
    
    # Try to process check-in data based on expected structure
    try:
        # This assumes a structure where "time" is a map of date/time strings to counts
        # Adjust based on actual structure
        checkin_exploded = process_checkin_data(checkin_df, spark)
        
        # Analyze check-in patterns
        print("\nCheck-in Patterns by Day of Week:")
        checkin_exploded.groupBy("day_of_week").agg(
            count("*").alias("checkin_count")
        ).orderBy("day_of_week").show()
    except Exception as e:
        print(f"Error processing check-in data: {e}")
        # Create empty dataframe for check-ins if processing fails
        checkin_schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("checkin_timestamp", StringType(), True),
            StructField("checkin_count", IntegerType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("hour_of_day", IntegerType(), True)
        ])
        checkin_exploded = spark.createDataFrame([], checkin_schema)
    
    # Perform temporal analysis of reviews
    temporal_review_analysis(reviews_with_time)
    
    # Analyze review patterns by year
    yearly_pattern(reviews_with_time)
    
    # Analyze review patterns by day of week
    day_of_week_pattern(reviews_with_time)
    
    # Analyze review patterns by hour of day
    hourly_pattern(reviews_with_time)
    
    # Analyze business hour patterns and their correlation with success
    business_hour_analysis(business_df, spark)
    
    # Calculate review velocity for businesses
    review_velocity = calculate_review_velocity(reviews_with_time, spark)
    
    # Save processed data
    review_velocity.write.mode("overwrite").json("hdfs://localhost:9000/user/localinsight/processed/temporal")
    
    # Stop Spark session
    spark.stop()

def process_checkin_data(checkin_df, spark):
    """Process check-in data based on its structure"""
    
    # Sample a few rows to understand structure
    checkin_sample = checkin_df.limit(2)
    checkin_sample.show(truncate=False)
    
    # This is a simplified approach - actual implementation would need
    # to adapt to the specific structure of the check-in data
    
    # Assuming "time" field is a string map with "date-time" -> count
    # First, convert to rows with (business_id, date-time, count)
    
    # Create a temporary view
    checkin_df.createOrReplaceTempView("checkins")
    
    # Using SQL for clearer processing
    checkin_exploded = spark.sql("""
        SELECT 
            business_id,
            explode(time) as checkin_data
        FROM checkins
    """)
    
    # Add time components
    checkin_processed = checkin_exploded.withColumn(
        "checkin_timestamp", col("checkin_data._1")
    ).withColumn(
        "checkin_count", col("checkin_data._2").cast("int")
    ).withColumn(
        "day_of_week", date_format(to_timestamp(col("checkin_timestamp")), "EEEE")
    ).withColumn(
        "hour_of_day", hour(to_timestamp(col("checkin_timestamp")))
    )
    
    return checkin_processed

def temporal_review_analysis(reviews_df):
    """Analyze temporal patterns in reviews"""
    
    print("\n=== Temporal Review Analysis ===")
    
    # Count reviews by year/month
    print("\nReview Count by Year:")
    reviews_df.groupBy("review_year").count().orderBy("review_year").show()
    
    # Star rating trends over time
    print("\nAverage Star Rating by Year:")
    reviews_df.groupBy("review_year").agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_stars")
    ).orderBy("review_year").show()

def yearly_pattern(reviews_df):
    """Analyze review patterns by year"""
    
    print("\n=== Yearly Review Pattern Analysis ===")
    
    # Review count by year and month
    print("\nReview Count by Year and Month:")
    reviews_df.groupBy("review_year", "review_month").agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_stars")
    ).orderBy("review_year", "review_month").show(24)
    
    # Find months with highest review counts
    print("\nAverage Reviews by Month (all years):")
    reviews_df.groupBy("review_month").agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_stars")
    ).orderBy("review_month").show()

def day_of_week_pattern(reviews_df):
    """Analyze review patterns by day of week"""
    
    print("\n=== Day of Week Pattern Analysis ===")
    
    # Map day of week number to name
    day_mapping = {
        1: "Sunday",
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday"
    }
    
    # Add day name column
    reviews_day = reviews_df.withColumn(
        "day_name",
        when(col("review_day") == 1, "Sunday")
        .when(col("review_day") == 2, "Monday")
        .when(col("review_day") == 3, "Tuesday")
        .when(col("review_day") == 4, "Wednesday")
        .when(col("review_day") == 5, "Thursday")
        .when(col("review_day") == 6, "Friday")
        .when(col("review_day") == 7, "Saturday")
    )
    
    # Reviews by day of week
    print("\nReview Count by Day of Week:")
    reviews_day.groupBy("day_name").agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_stars")
    ).orderBy(
        # Custom sorting to get days in order
        when(col("day_name") == "Sunday", 1)
        .when(col("day_name") == "Monday", 2)
        .when(col("day_name") == "Tuesday", 3)
        .when(col("day_name") == "Wednesday", 4)
        .when(col("day_name") == "Thursday", 5)
        .when(col("day_name") == "Friday", 6)
        .when(col("day_name") == "Saturday", 7)
    ).show()

def hourly_pattern(reviews_df):
    """Analyze review patterns by hour of day"""
    
    print("\n=== Hourly Pattern Analysis ===")
    
    # Reviews by hour of day
    print("\nReview Count by Hour of Day:")
    reviews_df.groupBy("review_hour").agg(
        count("*").alias("review_count"),
        avg("stars").alias("avg_stars")
    ).orderBy("review_hour").show(24)

def business_hour_analysis(business_df, spark):
    """Analyze business hours patterns and their correlation with ratings"""
    
    print("\n=== Business Hours Analysis ===")
    
    # Check how many businesses have hours information
    has_hours = business_df.filter(col("hours").isNotNull()).count()
    total_businesses = business_df.count()
    
    print(f"\nBusinesses with hours data: {has_hours} out of {total_businesses} ({has_hours/total_businesses:.2%})")
    
    # This is simplified - a comprehensive analysis would parse the hours structure
    # and analyze patterns by day/time
    
    # Count businesses open on Monday
    business_df.createOrReplaceTempView("businesses")
    
    # Using SQL for easier analysis of the hours structure
    print("\nBusinesses open by day of week:")
    day_counts = spark.sql("""
        SELECT 
            COUNT(CASE WHEN hours.Monday IS NOT NULL THEN 1 END) as monday_count,
            COUNT(CASE WHEN hours.Tuesday IS NOT NULL THEN 1 END) as tuesday_count,
            COUNT(CASE WHEN hours.Wednesday IS NOT NULL THEN 1 END) as wednesday_count,
            COUNT(CASE WHEN hours.Thursday IS NOT NULL THEN 1 END) as thursday_count,
            COUNT(CASE WHEN hours.Friday IS NOT NULL THEN 1 END) as friday_count,
            COUNT(CASE WHEN hours.Saturday IS NOT NULL THEN 1 END) as saturday_count,
            COUNT(CASE WHEN hours.Sunday IS NOT NULL THEN 1 END) as sunday_count
        FROM businesses
        WHERE hours IS NOT NULL
    """)
    
    day_counts.show()
    
    # Analyze relation between business hours and ratings
    print("\nAverage ratings by opening days count:")
    spark.sql("""
        SELECT 
            (CASE 
                WHEN hours.Monday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Tuesday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Wednesday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Thursday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Friday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Saturday IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN hours.Sunday IS NOT NULL THEN 1 ELSE 0 END
            ) as days_open,
            AVG(stars) as avg_stars,
            COUNT(*) as business_count
        FROM businesses
        WHERE hours IS NOT NULL
        GROUP BY days_open
        ORDER BY days_open
    """).show()

def calculate_review_velocity(reviews_df, spark):
    """Calculate review velocity metrics for businesses"""
    
    print("\n=== Review Velocity Analysis ===")
    
    # Group by business ID and year
    yearly_reviews = reviews_df.groupBy("business_id", "review_year").agg(
        count("*").alias("yearly_reviews"),
        avg("stars").alias("yearly_avg_stars")
    )
    
    # Calculate review velocity (yearly growth rate)
    yearly_reviews.createOrReplaceTempView("yearly_reviews")
    
    # Using window functions with SQL for easier implementation
    velocity_df = spark.sql("""
        SELECT 
            r1.business_id,
            r1.review_year as year1,
            r1.yearly_reviews as reviews1,
            r2.review_year as year2,
            r2.yearly_reviews as reviews2,
            (r2.yearly_reviews - r1.yearly_reviews) as review_growth,
            CASE 
                WHEN r1.yearly_reviews > 0 
                THEN (r2.yearly_reviews - r1.yearly_reviews) / r1.yearly_reviews 
                ELSE NULL 
            END as growth_rate,
            r1.yearly_avg_stars as stars1,
            r2.yearly_avg_stars as stars2,
            (r2.yearly_avg_stars - r1.yearly_avg_stars) as star_change
        FROM yearly_reviews r1
        JOIN yearly_reviews r2 
            ON r1.business_id = r2.business_id 
            AND r1.review_year + 1 = r2.review_year
        WHERE r1.yearly_reviews > 0
        ORDER BY growth_rate DESC
    """)
    
    print("\nReview Velocity (Top Growth Businesses):")
    velocity_df.show(10)
    
    print("\nReview Velocity (Bottom Growth Businesses):")
    velocity_df.orderBy("growth_rate").show(10)
    
    # Calculate average metrics by business
    velocity_metrics = velocity_df.groupBy("business_id").agg(
        avg("growth_rate").alias("avg_growth_rate"),
        avg("star_change").alias("avg_star_change"),
        count("*").alias("years_with_data")
    )
    
    return velocity_metrics

if __name__ == "__main__":
    main()