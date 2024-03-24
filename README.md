
# c++ Apache Arrow Evaluation
## Setup
### for Evaluation and Testing:
* Apache Arrow v14.0
* cmake v3.16 
* (GNU GCC v11.4/make v4.3)
### for Evaluation:
* R v4.3.2
* R package jsonlite
### for Testing:
* Apache Arrow packages:
    * Parquet
    * Dataset
    * Gandiva
* Gandiva requires LLVM Project v17
## Usage:
* cd into this projects dir
* create makefile for testing/debugging arrow (`arrow_testing.cpp`):
```
cmake -G'Unix Makefiles' -S . -B ./build -DCMAKE_BUILD_TYPE=Debug
```
* create makefile for evaluating arrow (`arrow_eval.cpp`):
```
cmake -G'Unix Makefiles' -S . -B ./build -DBUILD_EVAL=ON -DCMAKE_BUILD_TYPE=Release
```
* execute makefile:
```
make -C ./build
```
### Evaluating:
execute `measure.sh`
* runs `arrow_eval`, which generates measurements in the `./build` folder
* visualizes the measurements generating boxplots with same name in the `./build` folder via executing `plot.r`
* measurements include execution times of different sum-functions:
    * using the built in arrow compute sum function(`b` or `builtin`)
    * using an user defined sum function (`c` or `custom`)
* sum will be calculated over a column which has a size of multiples of 16KB, ranging from 16KB to 522 MB
* 50 measurements for each setup 
## Sources:
`arrow_testing.cpp`: contains example code from the arrow documentation and some snippets on how to create user defined functions, including:
* creating arrow ds e.g. arrays and tables
* reading and writing from/to different file formats (arrow/csv/parquet)
* using the dataset,gandiva and compute api
* adding user defined element wise and aggregation compute functions

`arrow_eval.cpp`: contains code for evaluating arrow compute functions. The program takes arguments `size` (table size in KB) and `measurements`(amount of runs) and does the following:
1. creates arrow table of given size with a column containing random data
2. saves table as `./build/nums.arrow`
3. reads the table
4. registers two user defined functions which calculate the sum elementwise and as aggregation.
5. calculates the aggregated sum of the column using the 
6. calculates the sum of the column using the non vectorized user defined function.
7. calculates the sum of the column using the vectorized user defined function.
* all calculations will be measured and executed multiple times, theres a parameter for the amount of calculations.
9. creates file `./build/measurements_<size>_<measurements>.json` containing all measurements (filename contains arguments).

`arrow_custom_kernel.cpp`: contains code for user defined aggregation sum function, as well as some helper macros.




