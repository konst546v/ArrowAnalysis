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
asc_vs <- as.numeric(plot_data$asc)
asb_vs <- as.numeric(plot_data$asb)
aso_vs <- as.numeric(plot_data$aso)
esc_vs <- as.numeric(plot_data$esc)
esb_vs <- as.numeric(plot_data$esb)

# path without extension
fb <- substring(args[1],first=1,last=nchar(args[1])-5)

# -- aggregate sum --
data <- list(builtin = asb_vs, custom = asc_vs, custom_o = aso_vs)
# create boxplot, size scales textfont and so on
pdf(paste0(fb,"as.pdf"),width=10,height=6)
# the outline mess up the graphics
s<-2
xs<-seq(from = 1, by = s, length.out = 4)
print(xs)
bp<-boxplot(data, outline=FALSE,col = c("red","green","blue"), 
    xlim=c(0.5,xs[length(xs)]),at=xs[-length(xs)],
    names = c("builtin","custom","custom vectorized"),
    main = paste0("aggregation sum function: custom vs builtin execution time for ",args[2],"KB and ",args[3]," measurements"),
    ylab = "nanoseconds")
# visualize medians, calculate and visualize relative speedups
median_b <- bp$stats[3,1]
median_c <- bp$stats[3,2]
median_o <- bp$stats[3,3]
vis_point <- function(x,y){
    abline(h=c(y),lty=2)
    str<-paste0(y,"ns")
    if(y >= 1e3) str<-paste0(round(y/1e3,2),"µs")
    if(y >= 1e6) str<-paste0(round(y/1e6,2),"ms")
    if(y >= 1e9) str<-paste0(round(y/1e9,2),"s")
    legend(x, y, str,box.col=NA,bg="white")    
}
vis_point(xs[1],median_b)
vis_point(xs[2],median_c)
vis_point(xs[3],median_o)
vis_speedup <- function(from,to,x){
    s <- from/to
    arrows(x0 = x, y0 = from, x1 = x, y1 = to, 
        length = 0.1, angle = 30, code = 2)
    legend(x = x, y = mean(c(from, to)), 
        paste("s:", round(s, 2)), box.col=NA, text.col = "red",bg="white")
}
mid_point <- function(idx){
    mean(c(xs[idx],xs[idx+1]))
}
vis_speedup(median_c,median_b,mid_point(1))
vis_speedup(median_o,median_b,mid_point(2))
vis_speedup(median_c,median_o,mid_point(3))
dev.off()

# -- elemwise sum --
data <- list(builtin = esb_vs, custom = esc_vs)
pdf(paste0(fb,"es.pdf"),width=10,height=6)
s<-2
xs<-seq(from = 1.5, by = s, length.out = 3)
print(xs)
bp<-boxplot(data, outline=FALSE,col = c("red","green"), 
    xlim=c(0.5,xs[length(xs)]),at=xs[-length(xs)],
    names = c("builtin","custom"),
    main = paste0("elementwise sum function: custom vs builtin execution time for ",args[2],"KB and ",args[3]," measurements"),
    ylab = "nanoseconds")
median_b <- bp$stats[3,1]
median_c <- bp$stats[3,2]
vis_point(xs[1],median_b)
vis_point(xs[2],median_c)
vis_speedup(median_c,median_b,mid_point(1))
dev.off()

