#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Geographic Analysis of Yelp Business Data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, avg, stddev, percentile_approx
from pyspark.sql.types import DoubleType, StringType
import json

def main():
    """Main execution function for geographic analysis"""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("YelpGeographicAnalysis") \
        .getOrCreate()
    
    # Load business data
    business_df = spark.read.json("hdfs:///user/localinsight/raw/business/yelp_academic_dataset_business.json")
    
    # Load processed review data (from previous jobs)
    try:
        reviews_df = spark.read.json("hdfs:///user/localinsight/processed/reviews")
    except:
        # If processed reviews don't exist, load the raw reviews
        reviews_df = spark.read.json("hdfs:///user/localinsight/raw/reviews/yelp_academic_dataset_review.json")
        # Aggregate reviews by business
        reviews_df = reviews_df.groupBy("business_id").agg(
            {"stars": "avg", "useful": "avg"}
        ).withColumnRenamed("avg(stars)", "avg_stars") \
         .withColumnRenamed("avg(useful)", "avg_useful")
    
    # Print schema to understand the data
    print("Business Schema:")
    business_df.printSchema()
    
    # Extract location data
    location_df = business_df.select(
        "business_id", 
        "name",
        "stars",
        "review_count",
        "is_open",
        "categories",
        "latitude",
        "longitude",
        "city",
        "state",
        "postal_code"
    )
    
    # Print count of businesses by state
    print("\nBusiness Count by State:")
    location_df.groupBy("state").count().orderBy(col("count").desc()).show(10)
    
    # Print count of businesses by city (top 20)
    print("\nBusiness Count by City (Top 20):")
    location_df.groupBy("city", "state").count() \
        .orderBy(col("count").desc()).show(20)
    
    # Join with review data
    geo_analysis_df = location_df.join(reviews_df, "business_id")
    
    # Analyze business density by postal code
    print("\nBusiness Density and Ratings by Postal Code (Top 20 by density):")
    postal_analysis = geo_analysis_df.groupBy("postal_code", "city", "state").agg(
        count("business_id").alias("business_count"),
        avg("stars").alias("avg_business_stars"),
        avg("avg_stars").alias("avg_review_stars"),
        avg("review_count").alias("avg_review_count")
    ).orderBy(col("business_count").desc())
    
    postal_analysis.show(20)
    
    # Analyze by state
    print("\nBusiness Analysis by State:")
    state_analysis = geo_analysis_df.groupBy("state").agg(
        count("business_id").alias("business_count"),
        avg("stars").alias("avg_business_stars"),
        stddev("stars").alias("stddev_stars"),
        avg("avg_stars").alias("avg_review_stars"),
        avg("review_count").alias("avg_review_count"),
        percentile_approx("stars", 0.5).alias("median_stars")
    ).orderBy(col("business_count").desc())
    
    state_analysis.show()
    
    # Create GeoJSON for top states
    print("\nGenerating GeoJSON for top states...")
    top_states_df = state_analysis.orderBy(col("business_count").desc()).limit(10)
    top_states = [row.state for row in top_states_df.select("state").collect()]
    
    # Filter businesses in top states
    top_state_businesses = geo_analysis_df.filter(col("state").isin(top_states))
    
    # Convert to GeoJSON format
    geojson_output = create_geojson(top_state_businesses)
    
    # Save GeoJSON to a file
    try:
        with open("/tmp/yelp_business_map.geojson", "w") as f:
            json.dump(geojson_output, f)
        
        print("GeoJSON saved to /tmp/yelp_business_map.geojson")
        
        # Save to HDFS as well
        spark.createDataFrame([{"geojson": json.dumps(geojson_output)}]) \
            .write.mode("overwrite").json("hdfs:///user/localinsight/processed/geojson")
    except Exception as e:
        print(f"Error saving GeoJSON: {e}")
    
    # Save processed data
    geo_analysis_df.write.mode("overwrite").json("hdfs:///user/localinsight/processed/geographic")
    
    # Stop Spark session
    spark.stop()

def create_geojson(df):
    """Create GeoJSON from dataframe of businesses"""
    
    # Create a GeoJSON FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Sample a subset of businesses for the map (up to 5000)
    sample_df = df.sample(fraction=0.1, seed=42).limit(5000)
    
    # Convert each business to a GeoJSON Feature
    for row in sample_df.collect():
        if row.latitude and row.longitude:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row.longitude), float(row.latitude)]
                },
                "properties": {
                    "name": row.name,
                    "business_id": row.business_id,
                    "stars": float(row.stars),
                    "city": row.city,
                    "state": row.state,
                    "review_count": int(row.review_count),
                    "is_open": int(row.is_open)
                }
            }
            
            # Add categories if available
            if row.categories:
                feature["properties"]["categories"] = row.categories
            
            geojson["features"].append(feature)
    
    return geojson

if __name__ == "__main__":
    main()