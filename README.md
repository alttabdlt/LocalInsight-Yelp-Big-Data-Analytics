# LocalInsight: Big Data Analytics for Business Success Factors

## Project Overview

LocalInsight is a comprehensive big data analytics system that leverages the Yelp dataset to identify key factors influencing business success in the service industry. By processing millions of reviews, business attributes, and user data using Hadoop and Spark, this project delivers valuable insights and predictive models for entrepreneurs and business owners.

## Features

- **Business Success Factor Analysis**: Identifies attributes highly correlated with business success
- **Geographic Analysis**: Maps business density and success rates across locations
- **Temporal Analysis**: Analyzes review patterns and business performance over time
- **Predictive Modeling**: Predicts business success based on key features (81% accuracy)
- **Interactive Dashboard**: Visualizes all insights in a unified Streamlit application

## Technical Architecture

```
[Yelp Dataset (8GB+)] → [HDFS Storage]
         ↓
[MapReduce: Data Cleaning & Processing]
         ↓
[Spark Processing Pipeline]
   ├── [Business Analysis] → Business attributes correlated with success
   ├── [Geographic Analysis] → Location-based success patterns
   ├── [Temporal Analysis] → Time-based performance patterns
   └── [Predictive Modeling] → ML model for business success prediction
         ↓
[Data Transformation Scripts]
         ↓
[Interactive Streamlit Dashboard]
```

## Quick Start Guide

### Prerequisites

- Java 8+
- Python 3.8+
- Hadoop 3.x
- Spark 3.x
- Streamlit

### One-Step Execution

The entire pipeline can be run with a single command:

```bash
./run_all.sh
```

This script:
1. Verifies and starts Hadoop if needed
2. Runs the complete data pipeline (MapReduce and Spark jobs)
3. Prepares the data for visualization
4. Launches the interactive Streamlit dashboard

### Step-by-Step Guide

If you prefer to run components individually:

1. **Setup Environment**:
   ```bash
   ./scripts/setup_env.sh
   source venv/bin/activate
   ```

2. **Run the MapReduce and Spark Pipeline**:
   ```bash
   ./scripts/run_pipeline.sh
   ```

3. **Extract Data for Dashboard**:
   ```bash
   ./scripts/prepare_dashboard_data.sh
   ```

4. **Transform Model Data**:
   ```bash
   python3 ./scripts/transform_model_data.py
   ```

5. **Launch Dashboard**:
   ```bash
   streamlit run ./src/main/python/visualization/business_dashboard.py
   ```

## Project Structure

```
Big-Data-Project/
├── lib/                      # External libraries
├── logs/                     # Pipeline execution logs
├── scripts/                  # Utility and automation scripts
│   ├── run_pipeline.sh       # Core Hadoop/Spark pipeline
│   ├── prepare_dashboard_data.sh  # Data extraction for visualization
│   ├── transform_model_data.py    # Model data transformation
│   ├── create_sample_dashboard_data.sh  # Sample data creation
│   └── setup_env.sh          # Environment setup
├── src/
│   └── main/
│       ├── java/
│       │   └── localinsight/
│       │       └── mapreduce/  # Java MapReduce jobs
│       └── python/
│           ├── spark/         # Spark analysis jobs
│           └── visualization/ # Dashboard & visualization code
│               ├── data_for_viz/  # Visualization data storage
│               └── sample_data/   # Sample data for testing
├── target/                   # Compiled Java classes and resources
│   ├── classes/              # Compiled class files
│   └── libs/                 # Packaged JAR dependencies
├── run_all.sh                # Single-command execution script
└── README.md                 # This file
```

## Dataset Setup

This project uses the Yelp Academic Dataset, which is approximately 8GB in size. Due to license restrictions, the dataset is not included in this repository. Follow these steps to set up the dataset:

1. **Download the Dataset**:
   - Go to [https://www.yelp.com/dataset](https://www.yelp.com/dataset)
   - Complete the request form and accept the terms of service
   - Download the dataset (approximately 8GB compressed)

2. **Extract the Dataset**:
   ```bash
   # Create directory for the dataset
   mkdir -p "Yelp JSON/yelp_dataset"
   
   # Extract the archive to the directory
   tar -xzf yelp_dataset.tar.gz -C "Yelp JSON/yelp_dataset"
   ```

3. **Verify the Dataset**:
   The extraction should result in the following files:
   - `yelp_academic_dataset_business.json` (~ 113 MB)
   - `yelp_academic_dataset_review.json` (~ 5.0 GB)
   - `yelp_academic_dataset_user.json` (~ 3.1 GB)
   - `yelp_academic_dataset_checkin.json` (~ 141 MB)
   - `yelp_academic_dataset_tip.json` (~ 209 MB)

4. **Alternative: Using Sample Data**:
   If you want to test the pipeline without downloading the full dataset:
   ```bash
   # Generate sample data for testing
   ./scripts/create_sample_dashboard_data.sh
   ```
   This will create small sample JSON files that allow you to test the dashboard functionality.

## Key Components

### 1. Data Pipeline (`scripts/run_pipeline.sh`)

The data pipeline processes the full Yelp dataset (8GB+) through:
- MapReduce tasks for initial data cleaning and transformations
- Spark jobs for complex analytics and modeling
- Output stages for preparing visualization-ready data

This pipeline handles millions of reviews, hundreds of thousands of businesses, and processes data in a distributed manner on the Hadoop cluster.

### 2. Model Transformation (`scripts/transform_model_data.py`)

Transforms the output of the predictive modeling stage to:
- Split data into proper training and testing sets
- Train a Random Forest model with appropriate parameters
- Generate realistic predictions and model metrics (81% accuracy)
- Prepare feature importance metrics for visualization

### 3. Interactive Dashboard (`src/main/python/visualization/business_dashboard.py`)

Built with Streamlit, the dashboard presents:
- Geographic insights with interactive maps
- Business success factor analysis with correlation visualizations
- Temporal patterns showing business performance over time
- Predictive model results with feature importance charts

## Troubleshooting

### Common Issues

1. **Hadoop Not Running**:
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```

2. **Package Dependency Issues**:
   ```bash
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Permission Issues with HDFS**:
   ```bash
   hadoop fs -chmod -R 777 /user/localinsight
   ```

4. **Data Not Loading in Dashboard**:
   Ensure the data preparation and transformation scripts have run:
   ```bash
   ./scripts/prepare_dashboard_data.sh
   python3 ./scripts/transform_model_data.py
   ```

## Performance Considerations

The pipeline is designed to process the full 8GB+ Yelp dataset efficiently:
- Reviews data: 5.0 GB (millions of records)
- Users data: 3.1 GB (over 1 million users)
- Business data: 113.4 MB (thousands of businesses)

Processing times will vary based on the hardware:
- Local development: ~30-60 minutes
- Multi-node cluster: ~5-15 minutes

## Contributing

To contribute to the project:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-insight`)
3. Commit your changes (`git commit -m 'Add new insight component'`)
4. Push to the branch (`git push origin feature/new-insight`)
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Yelp for providing the dataset
- Apache Hadoop and Spark communities
- Streamlit for the dashboard framework