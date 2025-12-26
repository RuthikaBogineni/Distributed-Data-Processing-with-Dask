This project demonstrates a scalable ETL pipeline built using Dask DataFrame and Pandas to process large CSV datasets and compare their performance.
The objective is to highlight when and why to use Dask instead of Pandas, focusing on parallel execution, memory efficiency, and real-world trade-offs rather than assuming one tool is always faster.

Technologies Used:-
Python 3
Dask
Pandas
NumPy
VS Code

Project Overview:-
-Automatically generates a large CSV dataset if one is not present
-Implements an ETL pipeline with filtering, groupby, and aggregation
-Benchmarks execution time and memory usage for Pandas vs Dask
-Uses a single executable Python script with logging and CLI configuration
-Designed to be reproducible and production-style

Workflow:-
-Dataset is auto-generated (configurable size)
-Pandas pipeline processes the data sequentially
-Dask pipeline processes the data using partitioned, parallel execution
-Execution time and memory usage are recorded and compared
-Results are printed and optionally saved for analysis

Key Observations:-
-Pandas performs better for small datasets that fit in memory due to lower overhead
-Dask introduces scheduling overhead, but scales better as dataset size increases
-Tool selection should depend on data size and system constraints, not assumptions

How to Run:-
pip install pandas dask[complete] numpy psutil
python etl_benchmark.py



