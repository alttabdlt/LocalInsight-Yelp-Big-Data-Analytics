#!/bin/bash

# Script to create sample data for the Streamlit dashboard

set -e

# Get directory of script
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

# Create data directory for visualizations
DATA_DIR="${PROJECT_ROOT}/src/main/python/visualization/data_for_viz"
# Clear existing data directory to ensure clean results
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

echo "Creating sample data directory for dashboard: $DATA_DIR"

# Create sample business analysis data
cat > "${DATA_DIR}/business_analysis.json" << 'EOL'
{"business_id":"Apn5Q_b6Nz61Tq4XzPNOOA","name":"Minhas Micro Brewery","city":"Calgary","state":"AB","stars":4.0,"review_count":24,"is_open":1,"success":1}
{"business_id":"AjEbIBw6ZFfln7ePHha9PA","name":"Steveston Pizza","city":"Vancouver","state":"BC","stars":4.5,"review_count":156,"is_open":1,"success":1}
{"business_id":"cYwJA2A6N13pxYlX9eM18A","name":"Musashi Japanese Restaurant","city":"Vancouver","state":"BC","stars":4.0,"review_count":95,"is_open":1,"success":1}
{"business_id":"rq5dgoksPHkl2Nqo14kKnQ","name":"Meat & Bread","city":"Vancouver","state":"BC","stars":4.5,"review_count":309,"is_open":1,"success":1}
{"business_id":"iCQpiavjjPzJ5_3gPD5Ebg","name":"Nonna's Ristorante Italiano","city":"Phoenix","state":"AZ","stars":3.5,"review_count":248,"is_open":1,"success":1}
{"business_id":"RtUvSWO_UZ8V3Wpj0n077w","name":"Bootleggers Modern American Smokehouse","city":"Scottsdale","state":"AZ","stars":4.0,"review_count":768,"is_open":1,"success":1}
{"business_id":"O8OLvtAJXdGf-2wChm7acg","name":"Red Rock Beer Garden","city":"Boulder","state":"CO","stars":4.5,"review_count":82,"is_open":1,"success":1}
{"business_id":"G-WHz9U5MAFKsKzNGdCL7w","name":"Joe's Farm Grill","city":"Gilbert","state":"AZ","stars":4.0,"review_count":1203,"is_open":1,"success":1}
{"business_id":"f4x1YBxkLtqKiKK5m9w6Uw","name":"Fu-Fu Cafe","city":"Houston","state":"TX","stars":3.5,"review_count":304,"is_open":0,"success":0}
{"business_id":"7VA80Yd18iBh2cjfFNSTLQ","name":"Wentworth Cafe","city":"North Sydney","state":"NSW","stars":2.5,"review_count":47,"is_open":0,"success":0}
EOL

# Create sample geographic analysis data
cat > "${DATA_DIR}/geographic.json" << 'EOL'
{"business_id":"Apn5Q_b6Nz61Tq4XzPNOOA","latitude":51.037,"longitude":-114.032,"city":"Calgary","state":"AB","stars":4.0,"review_count":24}
{"business_id":"AjEbIBw6ZFfln7ePHha9PA","latitude":49.278,"longitude":-123.129,"city":"Vancouver","state":"BC","stars":4.5,"review_count":156}
{"business_id":"cYwJA2A6N13pxYlX9eM18A","latitude":49.282,"longitude":-123.118,"city":"Vancouver","state":"BC","stars":4.0,"review_count":95}
{"business_id":"rq5dgoksPHkl2Nqo14kKnQ","latitude":49.284,"longitude":-123.113,"city":"Vancouver","state":"BC","stars":4.5,"review_count":309}
{"business_id":"iCQpiavjjPzJ5_3gPD5Ebg","latitude":33.447,"longitude":-112.073,"city":"Phoenix","state":"AZ","stars":3.5,"review_count":248}
{"business_id":"RtUvSWO_UZ8V3Wpj0n077w","latitude":33.652,"longitude":-111.924,"city":"Scottsdale","state":"AZ","stars":4.0,"review_count":768}
{"business_id":"O8OLvtAJXdGf-2wChm7acg","latitude":40.017,"longitude":-105.277,"city":"Boulder","state":"CO","stars":4.5,"review_count":82}
{"business_id":"G-WHz9U5MAFKsKzNGdCL7w","latitude":33.343,"longitude":-111.798,"city":"Gilbert","state":"AZ","stars":4.0,"review_count":1203}
{"business_id":"f4x1YBxkLtqKiKK5m9w6Uw","latitude":29.738,"longitude":-95.529,"city":"Houston","state":"TX","stars":3.5,"review_count":304}
{"business_id":"7VA80Yd18iBh2cjfFNSTLQ","latitude":-33.844,"longitude":151.209,"city":"North Sydney","state":"NSW","stars":2.5,"review_count":47}
EOL

# Create sample temporal analysis data
cat > "${DATA_DIR}/temporal.json" << 'EOL'
{"business_id":"Apn5Q_b6Nz61Tq4XzPNOOA","avg_growth_rate":0.12,"avg_star_change":0.05}
{"business_id":"AjEbIBw6ZFfln7ePHha9PA","avg_growth_rate":0.25,"avg_star_change":0.1}
{"business_id":"cYwJA2A6N13pxYlX9eM18A","avg_growth_rate":0.08,"avg_star_change":0.02}
{"business_id":"rq5dgoksPHkl2Nqo14kKnQ","avg_growth_rate":0.42,"avg_star_change":0.15}
{"business_id":"iCQpiavjjPzJ5_3gPD5Ebg","avg_growth_rate":-0.05,"avg_star_change":-0.1}
{"business_id":"RtUvSWO_UZ8V3Wpj0n077w","avg_growth_rate":0.18,"avg_star_change":0.05}
{"business_id":"O8OLvtAJXdGf-2wChm7acg","avg_growth_rate":0.35,"avg_star_change":0.12}
{"business_id":"G-WHz9U5MAFKsKzNGdCL7w","avg_growth_rate":0.15,"avg_star_change":0.03}
{"business_id":"f4x1YBxkLtqKiKK5m9w6Uw","avg_growth_rate":-0.12,"avg_star_change":-0.08}
{"business_id":"7VA80Yd18iBh2cjfFNSTLQ","avg_growth_rate":-0.25,"avg_star_change":-0.15}
EOL

# Create sample model data
cat > "${DATA_DIR}/model_data.json" << 'EOL'
{"feature":"review_count","importance":0.35}
{"feature":"stars","importance":0.25}
{"feature":"compliments_count","importance":0.15}
{"feature":"photos_count","importance":0.12}
{"feature":"open_hours","importance":0.08}
{"feature":"category_count","importance":0.05}
{"success":1,"prediction":1}
{"success":1,"prediction":1}
{"success":1,"prediction":1}
{"success":1,"prediction":0}
{"success":0,"prediction":0}
{"success":0,"prediction":0}
{"success":0,"prediction":1}
{"success":0,"prediction":0}
EOL

# Set permissions
chmod -R 755 "$DATA_DIR"

echo "Sample data creation complete. You can now run the dashboard with:"
echo "streamlit run ${PROJECT_ROOT}/src/main/python/visualization/business_dashboard.py"
