# SAP Data Modeling and PySpark Coding

## Description

In this exercise, we will practice **data modeling** and **coding in PySpark** to build data products that provide insights into our supply chain.


### Exercise Overview
We will work with data from two SAP systems that have similar data sources. Our task is to implement and integrate this data to provide a unified view for supply chain insights.

The exercise involves:

- **Processing Local Material** data.
- **Processing Process Order** data.
- Ensuring both datasets have the **same schema** for **harmonization** across systems.
- Writing **modular, reusable code** with proper documentation.
- Following **best-in-class principles** for flexibility and maintainability.

**Technical-Case-Study.pdf** explains everything in detail about what we need to do the exercise.

![Technical Case Study](technical-case-study-solution/technical-case-study.drawio.png)

---

## Project Structure

In the `data/` folder, you will find CSV files for two SAP systems (System 1 and System 2) that need to be used and transformed.

In the `src/` folder, you will find the solution to this project:

1. **`input_check.py`**: This file is used to do the initial check on the data and examine what’s there.
2. **`local_material_s1.py`**: Used to process local material data for **System 1**.
3. **`local_material_s2.py`**: Used to process local material data for **System 2**.
4. **`process_order_s1.py`**: Used to process order data for **System 1**.
5. **`process_order_s2.py`**: Used to process order data for **System 2**.
6. **`main.py`**: This is the main script that runs both `local_material.py` and `process_order.py`.
7. **`output_check.py`**: This script checks the final output files (Parquet files for each system).

Additionally, the **`technical-case-study.drawio`** file provides the data modeling visualization.

---
## Setup Instructions