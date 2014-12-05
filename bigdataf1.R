
library('devtools')

install_github('rmr2', 'RevolutionAnalytics', subdir='pkg')
install_github('bigvis', 'hadley')
library('rmr2')
library('bigvis')
rmr.options(backend="local")

rread <- function(dset,tt = TRUE) {
  if (tt) result = from.dfs(dset)$val
  else result = from.dfs(dset)
  return(head(result))  
}

# CSV beolvaso es osszefuzo kod
# filenames <- list.files(path = "D:Downloads/bigdata_data", full.names = TRUE)
# filenames
# aggreg <- do.call("rbind", lapply(filenames[1:10], read.csv, col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop')))
# aggreg$id <- 1:nrow(aggreg)
# hd0 <- to.dfs(aggreg)

# fajl beolvasas TODO: egyelore csak egy nap
d0 = read.csv('e:/munka/BME/BigData/siri.20121125.csv',col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop'))
#subset(d0[ ! duplicated( d0[ c("line_id","operator") ] ) , ], select=c("line_id","operator"))
#summary(d0)
d0 = d0[order(d0$vehicle_journey_id, d0$ts, d0$journey_pattern_id),] 
d0$id <- 1:nrow(d0)
#d0$tst<-as.POSIXct(d0$ts/1e6, origin="1970-01-01")
#d0$tsd<-as.Date(d0$tst)

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
hops_per_line <- mapreduce(hlines_ops, 
                             map = function(k, v)
                               keyval(k$line_id, 1), 
                             reduce = function(k, v) 
                               cbind(line = k, no_ops = sum(v, na.rm = TRUE)))

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

# haversine tavolsag
# TODO: rendezes, vehicle valtasnal torles, beletenni az utolso ismert delay-t 

compareNA <- function(v1,v2) {
  # This function returns TRUE wherever elements are the same, including NA's,
  # and false everywhere else.
  same <- (v1 == v2)  |  (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}

# hordered <- mapreduce(td0, 
#                              map = function(k, v)
#                                keyval(v$vehicle_journey_id, v), 
#                              reduce = function(k, v) 
#                                keyval(k,v))
# rread(hordered)
# 
# result$id <- 1:nrow(result)

haversines <- mapreduce(hd0, 
                        map = function(k, v)
                          keyval(c(v$id,v$id+1),cbind(v$lat,v$lon,v$vehicle_journey_id,v$delay)),   
                        reduce = function(k, v) {
                          # kinyerjuk a koordinatakat a reduce set-bol
                          if (length(v) <= 4){
                          lat = v[1]
                          lon = v[2]  
                          plat = NA
                          plon= NA
                          vjid = v[3]
                          pvjid = NA
                          del = v[4]  
                          pdel = NA}  
                          if (length(v) > 4){                          
                          lat = v[1]
                          lon = v[3]  
                          plat = v[2]
                          plon= v[4]
                          vjid = v[5]
                          pvjid = v[6]
                          del = v[7]  
                          pdel = v[8]}  
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
                          
                          if (compareNA(vjid,pvjid)) distdelta = R*c
                          else distdelta = NA
                          
                          cbind(id=k,lat=lat,lon=lon,vjid=vjid,del=del,plat=plat,plon=plon,pvjid=pvjid,pdel=pdel,distdelta = distdelta)})


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
# szebben
p1 <- ggplot(d3, aes(x = operator, y = mean_delay))
p2 <- p1 + geom_point(color="blue")            #set one color for all points