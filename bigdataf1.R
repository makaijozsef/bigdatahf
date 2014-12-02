
library('devtools')

install_github('rmr2', 'RevolutionAnalytics', subdir='pkg')
library('rmr2')
rmr.options(backend="local")



# fajl beolvasas TODO: egyelore csak egy nap
d0 = read.csv('e:/munka/BME/BigData/siri.20121125.csv',col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop'))
#subset(d0[ ! duplicated( d0[ c("line_id","operator") ] ) , ], select=c("line_id","operator"))
#summary(d0)
d0$id <- 1:nrow(d0)
# TODO order by first

# lenyomjuk az adatot "hadoop"-ba
hd0 <- to.dfs(d0)

#csinalunk egy pici mintat
td0 <- to.dfs(head(d0,5))

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

# atlagos keses vonalankent 2. szamu megoldas

hdelay_per_line <- mapreduce(hd0, 
    map = function(k, v)
        keyval(v$line_id, v$delay), 
    reduce = function(k, v) 
        cbind(line = k, mean = mean(v, na.rm = TRUE)))
result = from.dfs(hdelay_per_line)
head(result)

haversines <- mapreduce(td0, 
  map = function(k, v)
    keyval(c(v$id,v$id+1),cbind(v$lat,v$lon)),   
   reduce = function(k, v) {
     # kinyerjuk a koordinatakat a reduce set-bol
     lat = v[1]
     lon = v[3]
     plat = v[2]
     plon= v[4] 
     # radian konverzio
     rlat = lat*pi/180
     rlon = lon*pi/180
     rplat = plat*pi/180
     rplon= plon*pi/180
     
     # haversine tavolsag szamitasa
     R <- 6371 # Earth mean radius [km]
     delta.long <- (rplon - rlon)
     delta.lat <- (rplat - rlat)
     a <- sin(delta.lat/2)^2 + cos(rlat) * cos(rplat) * sin(delta.long/2)^2
     c <- 2 * asin(min(1,sqrt(a)))
     cbind(id = k, lat = lat, lon = lon, plat = plat, plon= plon, distdelta = R*c)})
   
result = from.dfs(haversines)$val
head(result)

#distHaversine(c(53.39514, -6.375028), c(53.376373, -6.587523))

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
