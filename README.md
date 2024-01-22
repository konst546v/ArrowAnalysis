
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
* measurements include execution times of different sum aggregate functions:
    * using the built in arrow compute sum function(`b` or `builtin`)
    * using an user defined sum function (`c` or `custom`)
    * using a vectorized user defined sum function (`o` or `custom vectorized`)
* sum will be calculated over a column which has a size of multiples of 16KB, from 16KB to 8 MB
* 50 measurements for each setup 






