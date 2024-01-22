# R usage:
# - execute script:
#  $ Rscript ./test_plot.r
# - install stuff:
#  1. $ R
#  2. $ install.packages("<pkgName>")
#  3. $ q()
# ---
# script reads generated measurements and creates measurements.pdf as box plot in same dir as measurements
library(jsonlite)
# read measurements via first arg
args <- commandArgs(trailingOnly = TRUE)
if(length(args) < 3) {
    stop("missing file arg, usage e.g.:\nRscript ./plot.r ./build/measurements_16_50.json 16 50")
}
# Read JSON data from the file
plot_data <- fromJSON(args[1])
c_vs <- as.numeric(plot_data$c)
b_vs <- as.numeric(plot_data$b)
o_vs <- as.numeric(plot_data$o)
data <- list(builtin = b_vs, custom = c_vs, custom_o = o_vs)
# path without extension but dot
fb <- substring(args[1],first=1,last=nchar(args[1])-4)
# create boxplot
pdf(paste0(fb,"pdf"))
# the outline mess up the graphics
boxplot(data, outline=FALSE,col = c("red","green","blue"), names = c("builtin","custom","custom vectorized"),
        main = paste0("custom vs builtin execution time for ",args[2],"KB and ",args[3]," measurements"), ylab = "nanoseconds")
dev.off()
