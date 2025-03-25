#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Yelp Business Success Factors Analysis using Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, udf, when
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator

def main():
    """Main execution function for business analysis"""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("YelpBusinessAnalysis") \
        .getOrCreate()
    
    # Load business data
    business_df = spark.read.json("hdfs:///user/localinsight/raw/business/yelp_academic_dataset_business.json")
    
    # Load review data
    reviews_df = spark.read.json("hdfs:///user/localinsight/raw/reviews/yelp_academic_dataset_review.json")
    
    # Print schemas to understand the data
    print("Business Schema:")
    business_df.printSchema()
    
    print("Reviews Schema:")
    reviews_df.printSchema()
    
    # Process business attributes
    business_processed = process_business_attributes(business_df)
    
    # Process reviews
    reviews_processed = process_reviews(reviews_df)
    
    # Join business data with review data
    joined_df = business_processed.join(reviews_processed, "business_id")
    
    # Display sample data
    print("Sample of joined data:")
    joined_df.show(5, truncate=False)
    
    # Feature engineering for analysis
    analysis_df = feature_engineering(joined_df)
    
    # Run regression model for star prediction
    regression_model(analysis_df)
    
    # Run classification model for business success prediction
    classification_model(analysis_df)
    
    # Save processed data for later use
    analysis_df.write.mode("overwrite").json("hdfs:///user/localinsight/processed/business_analysis")
    
    # Stop Spark session
    spark.stop()

def process_business_attributes(business_df):
    """Process and extract business attributes"""
    
    # Extract attributes from nested structure
    # Note: This simplified implementation handles a few key attributes
    # A complete implementation would process all relevant attributes
    
    # Define attribute extraction UDFs
    def extract_wifi(attributes):
        if attributes and "WiFi" in attributes:
            wifi = attributes["WiFi"]
            if wifi == "free" or wifi == "'free'":
                return 2.0  # Free WiFi
            elif wifi == "paid" or wifi == "'paid'":
                return 1.0  # Paid WiFi
        return 0.0  # No WiFi
    
    def extract_price_level(attributes):
        if attributes and "RestaurantsPriceRange2" in attributes:
            try:
                return float(attributes["RestaurantsPriceRange2"])
            except (ValueError, TypeError):
                pass
        return 0.0  # Default price level
    
    def extract_noise_level(attributes):
        if attributes and "NoiseLevel" in attributes:
            noise = attributes["NoiseLevel"]
            if noise == "quiet" or noise == "'quiet'":
                return 1.0
            elif noise == "average" or noise == "'average'":
                return 2.0
            elif noise == "loud" or noise == "'loud'":
                return 3.0
            elif noise == "very_loud" or noise == "'very_loud'":
                return 4.0
        return 2.0  # Default average noise level
    
    # Register UDFs
    extract_wifi_udf = udf(extract_wifi, DoubleType())
    extract_price_udf = udf(extract_price_level, DoubleType())
    extract_noise_udf = udf(extract_noise_level, DoubleType())
    
    # Process business data
    processed_df = business_df.select(
        "business_id", 
        "name",
        "stars",
        "review_count",
        col("is_open").cast("double"),
        col("attributes"),
        col("categories")
    )
    
    # Extract attributes
    processed_df = processed_df.withColumn(
        "has_wifi", extract_wifi_udf(col("attributes"))
    ).withColumn(
        "price_level", extract_price_udf(col("attributes"))
    ).withColumn(
        "noise_level", extract_noise_udf(col("attributes"))
    )
    
    # Convert categories string to array if needed
    processed_df = processed_df.withColumn(
        "category_array", 
        when(col("categories").isNotNull(), 
             split(col("categories"), ", ")).otherwise(None)
    )
    
    # Select final columns
    final_df = processed_df.select(
        "business_id", 
        "name",
        "stars",
        "review_count",
        "is_open",
        "has_wifi",
        "price_level",
        "noise_level",
        "category_array"
    )
    
    return final_df

def process_reviews(reviews_df):
    """Process review data and aggregate by business"""
    
    # Aggregate reviews by business
    reviews_agg = reviews_df.groupBy("business_id").agg(
        {"stars": "avg", "useful": "avg", "funny": "avg", "cool": "avg"}
    )
    
    # Rename columns
    reviews_agg = reviews_agg.withColumnRenamed("avg(stars)", "avg_review_stars") \
                            .withColumnRenamed("avg(useful)", "avg_useful") \
                            .withColumnRenamed("avg(funny)", "avg_funny") \
                            .withColumnRenamed("avg(cool)", "avg_cool")
    
    return reviews_agg

def feature_engineering(joined_df):
    """Create features for analysis"""
    
    # Select relevant features
    analysis_df = joined_df.select(
        "business_id",
        "name",
        "stars",                # Target variable (business rating)
        "review_count",
        "is_open",
        "has_wifi",
        "price_level",
        "noise_level",
        "avg_review_stars",
        "avg_useful",
        "avg_funny",
        "avg_cool"
    )
    
    # Fill missing values
    analysis_df = analysis_df.fillna({
        "price_level": 0.0,
        "noise_level": 2.0,     # Average noise level
        "has_wifi": 0.0,        # No WiFi
        "avg_useful": 0.0,
        "avg_funny": 0.0,
        "avg_cool": 0.0
    })
    
    # Create success indicator (above 4 stars)
    analysis_df = analysis_df.withColumn(
        "success", 
        (col("stars") >= 4.0).cast("double")
    )
    
    return analysis_df

def regression_model(df):
    """Build regression model to predict star ratings"""
    
    print("\n=== Star Rating Prediction Model ===")
    
    # Define features
    feature_cols = ["review_count", "is_open", "has_wifi", 
                    "price_level", "noise_level", 
                    "avg_useful", "avg_funny", "avg_cool"]
    
    # Create feature vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    final_data = assembler.transform(df)
    
    # Split into training and test sets
    train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
    
    # Create and train model
    lr = LinearRegression(featuresCol="features", labelCol="stars")
    model = lr.fit(train_data)
    
    # Evaluate model on test data
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="stars", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
    print(f"R-squared: {r2:.4f}")
    
    # Extract feature importance
    feature_importance = list(zip(feature_cols, model.coefficients))
    print("\nFeature Importance (Coefficients):")
    for feature, importance in sorted(feature_importance, key=lambda x: abs(x[1]), reverse=True):
        print(f"{feature}: {importance:.4f}")

def classification_model(df):
    """Build classification model to predict business success"""
    
    print("\n=== Business Success Prediction Model ===")
    
    # Define features
    feature_cols = ["review_count", "is_open", "has_wifi", 
                    "price_level", "noise_level", 
                    "avg_useful", "avg_funny", "avg_cool"]
    
    # Create feature vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    final_data = assembler.transform(df)
    
    # Split into training and test sets
    train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
    
    # Create and train model
    rf = RandomForestClassifier(featuresCol="features", labelCol="success", 
                              numTrees=100, maxDepth=5)
    model = rf.fit(train_data)
    
    # Evaluate model on test data
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="success")
    auc = evaluator.evaluate(predictions)
    
    print(f"Area Under ROC Curve (AUC): {auc:.4f}")
    
    # Extract feature importance
    feature_importance = list(zip(feature_cols, model.featureImportances))
    print("\nFeature Importance:")
    for feature, importance in sorted(feature_importance, key=lambda x: x[1], reverse=True):
        print(f"{feature}: {importance:.4f}")

if __name__ == "__main__":
    main()