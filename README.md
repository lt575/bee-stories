# README for `beesfinal.ipynb`

## Project Overview

This project explores the relationship between weather, honey production, pollination, and agricultural produce using a relational database (RDB). By integrating data from various reliable sources, the project aims to address critical environmental questions such as:

- How does weather affect honey production?
- How are bees' pollination activities impacted by weather?
- What is the correlation between pollination and agricultural produce?

The outputs of this project are particularly valuable for:
- Bee enthusiasts
- Environmental officials
- Agricultural market analysts

---

## Goals

1. **Data Integration**: Create a well-structured RDB linking weather, pollination, honey production, and produce data.
2. **Analysis and Insights**: Enable users to answer complex environmental and economic questions.
3. **Public Utility**: Provide accessible insights for environmental sustainability and market trends.

---

## Datasets Used

The project uses publicly available datasets:
1. **Monthly Temperatures**  
   Source: [Kaggle](https://www.kaggle.com/datasets/justinrwong/average-monthly-temperature-by-us-state)  
   Description: Historical monthly temperature data across U.S. states (1950–2022).

2. **Pollination Data**  
   Source: [Springer Nature](https://springernature.figshare.com/articles/dataset/PolLimCrop_dataset/24625299?backTo=%2Fcollections%2FPolLimCrop_-_A_global_database_of_Pollen_Limitation_in_Crops%2F6640595&file=43269153)  
   Description: Global dataset on pollen limitation in crops, including detailed experimental data.

3. **Honey Production**  
   Source: [Kaggle](https://www.kaggle.com/datasets/jessicali9530/honey-production/data)  
   Description: U.S. honey production data by state from 1998–2012.

4. **Fruit and Vegetable Prices**  
   Source: [Kaggle](https://www.kaggle.com/code/everydaycodings/complete-analysis-on-fruits-and-vegetables-prices/input)  
   Description: Data on the pricing trends of fruits and vegetables.

---

## Workflow: ETL (Extract, Transform, Load)

### 1. **Extraction**
- Datasets are read from cloud storage or URLs using `pandas`.
- Initial dataset statistics (e.g., row counts, missing values) are calculated for validation.

### 2. **Transformation**
- **Temperature Data**:  
  - Dropped unnecessary columns.
  - Standardized state names using abbreviations.
  - Combined `year` and `month` into a single datetime column.

- **Pollination Data**:  
  - Split complex column strings into individual columns.
  - Renamed columns to meaningful identifiers.
  - Dropped duplicate rows and irrelevant columns.

- **Honey Production**:  
  - Verified data integrity.
  - Ready for integration without significant transformation.

- **Product Prices**:  
  - Validated and retained for downstream analysis.

### 3. **Loading**
- Prepared datasets will be loaded into a relational database using `SQLAlchemy` for structured analysis and query execution.

---

## Code Overview

### Libraries Used
- **Pandas**: For data manipulation.
- **SQLAlchemy**: For database integration.
- **Colab**: Google Colab for interactive development.

### Key Functions
1. **Data Cleaning and Transformation**:
   - `drop()`, `replace()`, and column operations for standardizing data.
2. **Integration**:
   - Transform data formats to enable relationships in the RDB schema.
3. **Exploratory Data Analysis**:
   - Head samples and missing value checks.

---

## Potential Use Cases

1. **Environmental Analysis**:
   - Study how climate change impacts bees and agriculture.
2. **Economic Insights**:
   - Understand pricing trends in agricultural produce based on pollination efficacy.
3. **Policy Recommendations**:
   - Guide strategies for sustainable agriculture and bee conservation.

---

## Running the Notebook

1. Open `beesfinal.ipynb` in Google Colab.
2. Ensure the following libraries are installed:
   - `pandas`
   - `SQLAlchemy`
3. Run cells sequentially for extracting, transforming, and loading data.

---

## Contributions and Future Work

### Contributions
- Created a pipeline linking weather, pollination, honey production, and produce data.
- Provided a framework for relational analysis.

### Future Enhancements
1. Develop visualizations to illustrate trends and correlations.
2. Add machine learning models for predictive analytics.
3. Expand the dataset to include more geographic regions and time periods.

