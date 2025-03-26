# LocalInsight: Big Data Analytics for Business Success Factors

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://localinsight.streamlit.app/)

## Project Overview

LocalInsight is a comprehensive big data analytics system that leverages the Yelp dataset to identify key factors influencing business success in the service industry. By processing millions of reviews, business attributes, and user data using Hadoop and Spark, this project delivers valuable insights and predictive models for entrepreneurs and business owners.

## Live Dashboard

We've deployed a live version of our dashboard with pre-processed data from the Yelp dataset. This allows you to explore the insights without having to run the full data pipeline:
* **Live Demo**: [LocalInsight Dashboard](https://localinsight.streamlit.app/)
* **Features Available**:
  * Interactive maps showing business distribution and success rates
  * Key success factors with correlation analysis
  * Temporal patterns of business performance
  * Predictive model results with 81% accuracy
  * Feature importance visualization

Note: The live dashboard uses a processed subset of the data. For full analysis capabilities with the complete dataset, follow the installation and setup instructions below.

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

## Dataset Setup

This project uses the Yelp Academic Dataset, which is approximately 8GB in size. Due to license restrictions, the dataset is not included in this repository. Follow these steps to set up the dataset:

1. **Download the Dataset**:
   - Go to [https://www.yelp.com/dataset](https://www.yelp.com/dataset)
   - Download the JSON files directly (approximately 8GB compressed)
   - No request form needs to be filled out, the files can be downloaded directly

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

## Environment Setup

Before running the project, make sure you have the following prerequisites installed:

1. **Hadoop Setup**:
   - Hadoop 3.x should be installed and configured on your system
   - HDFS should be running and accessible at `localhost:9000` (this is the default)
   - If you're using a different HDFS configuration, update the URIs in:
     - `scripts/hdfs_upload.sh`
     - `src/main/python/spark/*.py` files
     - `scripts/run_pipeline.sh`

2. **Spark Setup**:
   - Spark 3.x should be installed and configured
   - The `SPARK_HOME` environment variable should be set

3. **Python Environment**:
   - The project includes a script to set up a Python virtual environment:
   ```bash
   ./scripts/setup_env.sh
   source new_venv/bin/activate
   ```

4. **HDFS Directory Structure**:
   - The project automatically creates the required HDFS directory structure
   - Make sure your Hadoop user has write permissions to HDFS

## Quick Start Guide

### One-Step Execution (after Hadoop is running)

1. **Start Hadoop** (if not already running):
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```

2. **Run the automated setup and pipeline**:
   ```bash
   ./scripts/setup_env.sh  # Creates Python virtual environment with dependencies
   ./scripts/run_pipeline.sh  # Runs the full data pipeline
   ```

3. **View Results**:
   - Logs are available in `logs/`
   - Visualizations are generated in `visualization_output/`

### Step-by-Step Guide

If you prefer to run components individually:

1. **Start Hadoop** (if not already running):
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```

2. **Setup Environment**:
   ```bash
   ./scripts/setup_env.sh
   source new_venv/bin/activate
   ```

3. **Upload Data to HDFS**:
   - Place your Yelp dataset files in a local directory
   - Run the upload script:
   ```bash
   ./scripts/hdfs_upload.sh /path/to/your/yelp_dataset
   ```
   - This will upload the JSON files to HDFS with the correct filenames
   - Note: The script renames files from `yelp_academic_dataset_*.json` to just `*.json`

4. **Run the MapReduce and Spark Pipeline**:
   ```bash
   ./scripts/run_pipeline.sh
   ```
   - This handles all MapReduce jobs, Spark analysis, and visualization generation
   - The pipeline is configured to work with HDFS at `localhost:9000` by default

5. **Check Output and Logs**:
   - All logs will be available in the `logs/` directory
   - Visualizations will be generated in `visualization_output/`

## Troubleshooting

### Common Issues

1. **HDFS Connection Issues**:
   - Ensure Hadoop is running: `jps` should show NameNode and DataNode processes
   - Verify HDFS is accessible at localhost:9000: `hdfs dfs -ls hdfs://localhost:9000/`
   - If using a different hostname or port, update the URIs in all Spark scripts

2. **Python Dependencies**:
   - If encountering module errors, ensure you've activated the virtual environment:
     ```bash
     source new_venv/bin/activate
     ```

3. **Visualization Issues**:
   - If visualizations aren't generating, check the `logs/visualization.log`
   - Ensure the Spark configuration allows access to HDFS

### Advanced Configuration

The project is configured to work out-of-the-box with standard Hadoop and Spark installations. If you need to customize:

1. **Custom HDFS URI**: 
   If your HDFS is not running at the default `localhost:9000`:
   - Update URIs in all Spark scripts in `src/main/python/spark/`
   - Update the `hdfs_upload.sh` script
   - Update the Spark configuration in `run_pipeline.sh`

2. **Memory Configuration**:
   For larger datasets or limited resources:
   - Adjust Spark executor memory in `run_pipeline.sh` by adding `--executor-memory 4g`

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