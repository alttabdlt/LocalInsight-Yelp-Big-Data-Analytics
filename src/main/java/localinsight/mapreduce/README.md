# MapReduce Jobs for Yelp Data Processing

This directory contains the MapReduce jobs used to process the Yelp dataset:

## 1. BusinessProcessing.java

Processes the business data from the Yelp dataset, extracting key attributes and organizing businesses by location.

### Mapper
- Input: Raw business JSON data
- Process: Extracts business ID, name, category, stars, review count, and location
- Output: (state:city, business details)

### Reducer
- Input: (state:city, [business details])
- Process: Organizes businesses by location
- Output: (state:city, list of businesses)

## 2. ReviewSentiment.java

Processes review data and extracts sentiment indicators.

### Mapper
- Input: Raw review JSON data
- Process: Extracts business ID, user ID, stars, and performs simple sentiment analysis on text
- Output: (business_id, review details with sentiment score)

### Reducer
- Input: (business_id, [review details])
- Process: Aggregates review data by business
- Output: (business_id, aggregated review metrics)

## 3. TemporalPatterns.java

Analyzes temporal patterns in reviews and check-ins.

### Mapper
- Input: Review and check-in JSON data
- Process: Extracts time-related information from reviews and check-ins
- Output: (business_id, temporal data)

### Reducer
- Input: (business_id, [temporal data])
- Process: Creates temporal profiles by day of week and hour of day
- Output: (business_id, temporal profile)

## Usage

To compile these MapReduce jobs:

```bash
javac -classpath $(hadoop classpath) *.java
jar cf yelp-analysis.jar *.class
```

To run a job:

```bash
hadoop jar yelp-analysis.jar localinsight.mapreduce.BusinessProcessing \
  /user/localinsight/raw/business/yelp_academic_dataset_business.json \
  /user/localinsight/processed/business_processed
```