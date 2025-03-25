#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Predictive Modeling for Yelp Business Success
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
import os

def main():
    """Main execution function for predictive modeling"""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("YelpPredictiveModeling") \
        .getOrCreate()
    
    # Load processed data from previous analyses
    print("Loading processed data...")
    
    try:
        # Try loading previously processed data
        business_analysis_df = spark.read.json("hdfs:///user/localinsight/processed/business_analysis")
        geo_df = spark.read.json("hdfs:///user/localinsight/processed/geographic")
        temporal_df = spark.read.json("hdfs:///user/localinsight/processed/temporal")
        
        print("Successfully loaded processed data")
    except Exception as e:
        print(f"Error loading processed data: {e}")
        print("Loading raw data instead...")
        
        # If processed data is not available, load and process raw data
        business_df = spark.read.json("hdfs:///user/localinsight/raw/business/yelp_academic_dataset_business.json")
        reviews_df = spark.read.json("hdfs:///user/localinsight/raw/reviews/yelp_academic_dataset_review.json")
        
        # Process business attributes
        business_analysis_df = process_business_data(business_df)
        
        # Process review data
        review_summary = reviews_df.groupBy("business_id").agg(
            count("*").alias("review_count"),
            avg("stars").alias("avg_review_stars")
        )
        
        # Join business and review data
        business_analysis_df = business_analysis_df.join(review_summary, "business_id")
        geo_df = business_analysis_df  # Simplified geo data
        temporal_df = None  # Temporal data not available
    
    # Build dataset for modeling
    model_df = prepare_model_dataset(business_analysis_df, geo_df, temporal_df, spark)
    
    # Print data summary
    print("\nData Summary for Modeling:")
    model_df.describe().show()
    
    # Train-test split
    train_data, test_data = model_df.randomSplit([0.8, 0.2], seed=42)
    
    # Build and evaluate models
    build_star_rating_model(train_data, test_data)
    build_business_success_classifier(train_data, test_data)
    build_longevity_model(train_data, test_data)
    
    # Save model data for visualization
    model_df.write.mode("overwrite").json("hdfs:///user/localinsight/processed/model_data")
    
    # Stop Spark session
    spark.stop()

def process_business_data(business_df):
    """Process raw business data for modeling"""
    
    # Extract business attributes
    business_df = business_df.select(
        "business_id",
        "name",
        "stars",
        "review_count",
        col("is_open").cast("double"),
        "categories",
        "latitude",
        "longitude",
        col("attributes"),
        col("location.city").alias("city"),
        col("location.state").alias("state")
    )
    
    # Create success indicator
    business_df = business_df.withColumn(
        "success",
        (col("stars") >= 4.0).cast("double")
    )
    
    # Extract key attributes (this is simplified)
    business_df = business_df.withColumn(
        "has_wifi",
        when(
            col("attributes.WiFi").isin("'free'", "free"), 1.0
        ).when(
            col("attributes.WiFi").isin("'paid'", "paid"), 0.5
        ).otherwise(0.0)
    ).withColumn(
        "takes_credit_cards",
        when(
            col("attributes.BusinessAcceptsCreditCards").isin("true", "True", "'true'", "'True'"), 1.0
        ).otherwise(0.0)
    )
    
    return business_df

def prepare_model_dataset(business_df, geo_df, temporal_df, spark):
    """Prepare dataset for predictive modeling"""
    
    # Start with business data
    model_df = business_df
    
    # Add geographic features if available
    if geo_df is not None:
        # For simplicity, we're assuming that geo_df has the same business_id
        # In a real implementation, you'd join on business_id if they're different DataFrames
        if "state" in geo_df.columns and "state" not in model_df.columns:
            state_indexer = StringIndexer(
                inputCol="state", outputCol="state_idx", handleInvalid="skip"
            )
            model_df = state_indexer.fit(geo_df).transform(geo_df)
    
    # Add temporal features if available
    if temporal_df is not None and "business_id" in temporal_df.columns:
        # Join with temporal velocity metrics
        temporal_features = temporal_df.select(
            "business_id", 
            "avg_growth_rate", 
            "avg_star_change",
            "years_with_data"
        )
        model_df = model_df.join(temporal_features, "business_id", "left")
        
        # Fill missing values
        model_df = model_df.fillna({
            "avg_growth_rate": 0.0,
            "avg_star_change": 0.0,
            "years_with_data": 0.0
        })
    
    # Create a success column based on stars (consider businesses with 4+ stars as successful)
    model_df = model_df.withColumn("success", when(col("stars") >= 4.0, 1.0).otherwise(0.0))
    
    # Create final modeling dataset with selected features
    # Select features that should be available in most scenarios
    final_cols = ["business_id", "stars", "review_count", "is_open", "success"]
    
    # Add columns that might not be in all DataFrames
    for col_name in ["has_wifi", "takes_credit_cards", "state_idx", 
                     "avg_growth_rate", "avg_star_change", "years_with_data"]:
        if col_name in model_df.columns:
            final_cols.append(col_name)
    
    # Select final columns
    final_df = model_df.select(final_cols)
    
    # Convert columns to numeric if they're not already
    for col_name in final_cols:
        if col_name != "business_id":
            final_df = final_df.withColumn(col_name, col(col_name).cast("double"))
    
    # Fill missing values
    final_df = final_df.fillna(0.0)
    
    return final_df

def build_star_rating_model(train_data, test_data):
    """Build regression model to predict star ratings"""
    
    print("\n=== Building Star Rating Prediction Model ===")
    
    # Identify feature columns (all except business_id, stars, and success)
    feature_cols = [col for col in train_data.columns 
                    if col not in ["business_id", "stars", "success"]]
    
    print(f"Using features: {feature_cols}")
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols, 
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Create model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="stars",
        numTrees=50,
        maxDepth=8
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Train model
    print("Training model...")
    model = pipeline.fit(train_data)
    
    # Make predictions
    predictions = model.transform(test_data)
    
    # Evaluate model
    evaluator = RegressionEvaluator(
        labelCol="stars", 
        predictionCol="prediction", 
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
    print(f"R-squared: {r2:.4f}")
    
    # Extract feature importance
    rf_model = model.stages[-1]
    feature_importance = list(zip(feature_cols, rf_model.featureImportances))
    
    print("\nFeature Importance:")
    for feature, importance in sorted(feature_importance, key=lambda x: x[1], reverse=True):
        print(f"{feature}: {importance:.4f}")
    
    # Save model
    try:
        model_path = "hdfs:///user/localinsight/models/star_rating_model"
        model.write().overwrite().save(model_path)
        print(f"Model saved to {model_path}")
    except Exception as e:
        print(f"Error saving model: {e}")
        
        # Try saving locally if HDFS fails
        try:
            local_path = "/tmp/yelp_star_rating_model"
            model.write().overwrite().save(local_path)
            print(f"Model saved locally to {local_path}")
        except Exception as e2:
            print(f"Error saving model locally: {e2}")

def build_business_success_classifier(train_data, test_data):
    """Build classification model to predict business success"""
    
    print("\n=== Building Business Success Prediction Model ===")
    
    # Identify feature columns (all except business_id, stars, and success)
    feature_cols = [col for col in train_data.columns 
                    if col not in ["business_id", "stars", "success"]]
    
    print(f"Using features: {feature_cols}")
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols, 
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Create model
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="success",
        numTrees=100,
        maxDepth=5
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Train model
    print("Training model...")
    model = pipeline.fit(train_data)
    
    # Make predictions
    predictions = model.transform(test_data)
    
    # Evaluate model
    evaluator = BinaryClassificationEvaluator(
        labelCol="success", 
        rawPredictionCol="rawPrediction"
    )
    auc = evaluator.evaluate(predictions)
    
    print(f"Area Under ROC Curve (AUC): {auc:.4f}")
    
    # Extract feature importance
    rf_model = model.stages[-1]
    feature_importance = list(zip(feature_cols, rf_model.featureImportances))
    
    print("\nFeature Importance:")
    for feature, importance in sorted(feature_importance, key=lambda x: x[1], reverse=True):
        print(f"{feature}: {importance:.4f}")
    
    # Save model
    try:
        model_path = "hdfs:///user/localinsight/models/success_classifier_model"
        model.write().overwrite().save(model_path)
        print(f"Model saved to {model_path}")
    except Exception as e:
        print(f"Error saving model: {e}")
        
        # Try saving locally if HDFS fails
        try:
            local_path = "/tmp/yelp_success_classifier_model"
            model.write().overwrite().save(local_path)
            print(f"Model saved locally to {local_path}")
        except Exception as e2:
            print(f"Error saving model locally: {e2}")

def build_longevity_model(train_data, test_data):
    """Build model to predict business longevity (is_open)"""
    
    print("\n=== Building Business Longevity Prediction Model ===")
    
    # Identify feature columns (all except business_id, is_open, and success)
    feature_cols = [col for col in train_data.columns 
                    if col not in ["business_id", "is_open", "success"]]
    
    print(f"Using features: {feature_cols}")
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols, 
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Create model (logistic regression for interpretability)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="is_open",
        maxIter=20,
        regParam=0.1
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, lr])
    
    # Train model
    print("Training model...")
    model = pipeline.fit(train_data)
    
    # Make predictions
    predictions = model.transform(test_data)
    
    # Evaluate model
    evaluator = BinaryClassificationEvaluator(
        labelCol="is_open", 
        rawPredictionCol="rawPrediction"
    )
    auc = evaluator.evaluate(predictions)
    
    print(f"Area Under ROC Curve (AUC): {auc:.4f}")
    
    # Extract coefficients
    lr_model = model.stages[-1]
    coefficients = lr_model.coefficients.toArray()
    feature_importance = list(zip(feature_cols, coefficients))
    
    print("\nFeature Coefficients:")
    for feature, coef in sorted(feature_importance, key=lambda x: abs(x[1]), reverse=True):
        print(f"{feature}: {coef:.4f}")
    
    # Save model
    try:
        model_path = "hdfs:///user/localinsight/models/longevity_model"
        model.write().overwrite().save(model_path)
        print(f"Model saved to {model_path}")
    except Exception as e:
        print(f"Error saving model: {e}")
        
        # Try saving locally if HDFS fails
        try:
            local_path = "/tmp/yelp_longevity_model"
            model.write().overwrite().save(local_path)
            print(f"Model saved locally to {local_path}")
        except Exception as e2:
            print(f"Error saving model locally: {e2}")

if __name__ == "__main__":
    main()