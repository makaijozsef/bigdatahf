
library('devtools')

install_github('rmr2', 'RevolutionAnalytics', subdir='pkg')
library('rmr2')
rmr.options(backend="local")



# fajl beolvasas TODO: egyelore csak egy nap
d0 = read.csv('e:/munka/BME/BigData/siri.20121125.csv',col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop'))
#subset(d0[ ! duplicated( d0[ c("line_id","operator") ] ) , ], select=c("line_id","operator"))
#summary(d0)

# lenyomjuk az adatot "hadoop"-ba
hd0 <- to.dfs(d0)
# egyedi line, operator parok kinyerese trukkos aggregacioval TODO: lehetne szebb
hlines_ops <-  mapreduce(input = hd0, 
                         map = function(., v)
                           keyval(v[, c("line_id","operator")], 1),
                         reduce = function(k, vv) {
                           keyval(k, 1)
                         }
)
# operatorok szama vonalankent
hops_per_line <-  mapreduce(input = hlines_ops, 
                            map = function(k, .)
                              keyval(k[, c("line_id")], 1),
                            reduce = function(k, vv) {
                              keyval(k, length(vv))
                            }
)
# vonalak szama operatoronkent
hlines_per_op <-  mapreduce(input = hlines_ops, 
                            map = function(k, .)
                              keyval(k[, c("operator")], 1),
                            reduce = function(k, vv) {
                              keyval(k, length(vv))
                            }
)
# atlagos keses vonalankent
hdelay_per_line <-  mapreduce(input = hd0, 
                              map = function(., v)
                                keyval(v[, c("line_id")],v[, c("delay")]),
                              reduce = function(k, vv) {
                                keyval(k, mean(vv))
                              }
)

# eredmenyek kiolvasasa
from.dfs(hdelay_per_line)
result <- from.dfs(hdelay_per_line)
d1 = data.frame(result)
colnames(d1) <- c('line_id','mean_delay')

#from.dfs(hlines_per_op)
from.dfs(hops_per_line)
result <- from.dfs(hops_per_line)
d2 = data.frame(result)
colnames(d2) <- c('line_id','operator')

# plotolas
d3 = merge(x = d1, y = d2, by = "line_id", all = TRUE)
plot(d3$mean_delay, d3$operator)
