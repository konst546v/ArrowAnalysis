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
esc_vs <- as.numeric(plot_data$esc)
esb_vs <- as.numeric(plot_data$esb)

# path without extension
fb <- substring(args[1],first=1,last=nchar(args[1])-5)

# -- aggregate sum
data <- list(asb = asb_vs, asc = asc_vs)
# create boxplot, size scales textfont and so on
pdf(paste0(fb,"as.pdf"),width=8,height=6)
# the outline mess up the graphics
columns <-2 #columns
s<-1.5 # width between columns
xs<-seq(from = 2, by = s, length.out = columns+1)
print(xs[-length(xs)])
bp<-boxplot(data, outline=FALSE,col = c("blue","darkblue"), 
    xlim=c(0.5,xs[length(xs)]),at=xs[-length(xs)],
    names = c("builtin","custom"),
    main = paste0("aggregate sum function execution times for ",args[2],"KB and ",args[3]," measurements"),
    ylab = "nanoseconds")
# visualize medians, calculate and visualize relative speedups
median_asb <- bp$stats[3,1]
median_asc <- bp$stats[3,2]

vis_point <- function(x,y){
    abline(h=c(y),lty=2)
    str<-paste0(y,"ns")
    if(y >= 1e3) str<-paste0(round(y/1e3,2),"µs")
    if(y >= 1e6) str<-paste0(round(y/1e6,2),"ms")
    if(y >= 1e9) str<-paste0(round(y/1e9,2),"s")
    #text(x,y,labels=str,col="red") # sucks 
    legend(x, y, str,box.col=NA,bg="white") #still sucks but not as much as before
}
vis_point(xs[1],median_asb)
vis_point(xs[2],median_asc)

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
vis_speedup(median_asc,median_asb,mid_point(1))
dev.off()

# -- elemwise sum
data <- list(esb = esb_vs, esc = esc_vs)
pdf(paste0(fb,"es.pdf"),width=8,height=6)
bp<-boxplot(data, outline=FALSE,col = c("green","darkgreen"), 
    xlim=c(0.5,xs[length(xs)]),at=xs[-length(xs)],
    names = c("builtin","custom"),
    main = paste0("elemwise sum function execution times for ",args[2],"KB and ",args[3]," measurements"),
    ylab = "nanoseconds")
median_esb <- bp$stats[3,1]
median_esc <- bp$stats[3,2]
vis_point(xs[1],median_esb)
vis_point(xs[2],median_esc)
vis_speedup(median_esc,median_esb,mid_point(1))
dev.off()

