############### IMPORTS #############################
library('devtools')
library('lattice')
library('ggmap')

library('rmr2')
rmr.options(backend="local")

############### SEGEDFUGGVENYEK ###################
# segedfuggveny hogy belenezhessunk az eredmenyekbe
rread <- function(dset,tt = TRUE) {
  if (tt) result = from.dfs(dset)$val
  else result = from.dfs(dset)
  return(head(result))  
}

# NA-val tuzdelt ertekek osszevetese
compareNA <- function(v1,v2) {
  # This function returns TRUE wherever elements are the same, including NA's,
  # and false everywhere else.
  same <- (v1 == v2)  |  (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}

################# FAJLBEOLVASAS ##################################
# Egy adott konyvtar 4-13. (csv) fajljait osszefuzzuk egy nagy dataframe-be
# es meg hozzaadunk sorfolytonos id es olyan attributumot timestamp alapjan, hogy mely oraban tortent megfigyeles
filenames <- list.files(path = "D:Downloads/bigdata_data", full.names = TRUE)
filenames
d0 <- do.call("rbind", lapply(filenames[4:13], read.csv, col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop')))

# fajl beolvasas TODO: egyelore csak egy nap
#d0 = read.csv('e:/munka/BME/BigData/siri.20121125.csv',col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop'))
#subset(d0[ ! duplicated( d0[ c("line_id","operator") ] ) , ], select=c("line_id","operator"))
#summary(d0)

# megfelelo modon sorrendezzuk a rekordokat
d0 = d0[order(d0$time_frame,d0$vehicle_journey_id, d0$ts, d0$journey_pattern_id),]

#sorazonosito generalasa
d0$id <- 1:nrow(d0)

#az idobeli aggregacio alapja, melyik nap melyik orajarol van szo
d0$tst <- paste(format(as.POSIXct(d0$ts/1e6, origin="1970-01-01"),"%Y-%b-%d %H"),'h', sep='')

########### HAVERSINE TAVOLSAG ##################
# haversine tavolsagok szamitasa mapreduce nelkul
# az MR11-es mapreduce kivaltasara
#################################################
# Agi javaslatara mapreduce nelkul is megcsinaltuk a tavolsagszamitast

# oszlopok eltolasa
d0 <- shift.column(data=d0, columns=c("lat","lon","vehicle_journey_id"),up=FALSE)

calc_haversine <- function(lat, lon, plat, plon, vjid, pvjid) {
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
  
  # csak ertelmes esetekben adjuk vissza a kalkulalt erteket
  if (compareNA(vjid,pvjid)) distdelta = R*c
  else distdelta = NA
  return(distdelta)
}

# tavolsag szamitasa minden sorra
d0$distdelta <- mapply(calc_haversine, d0$lat, d0$lon, d0$lat.Shifted, d0$lon.Shifted, d0$vehicle_journey_id, d0$vehicle_journey_id.Shifted) 
 
  
# tovabbiakban felesleges oszlopok torlese, takaritas
d0 <-subset(d0,,-c(lon.Shifted,lat.Shifted,vehicle_journey_id.Shifted))

# lenyomjuk az adatot "hadoop"-ba
hd0 <- to.dfs(d0)


#csinalunk egy pici mintat, csak teszteleshez
#td0 <- to.dfs(head(d0,500))

############# MAPREDUCE kodok###################
################################################

# MR1 utolso mert keses jaratra max timestamp alapjan
end_delay_per_journey <-  mapreduce(input = hd0, 
                                    map = function(., v){
                                      keyval(v[, c("vehicle_journey_id","time_frame")], v[, c("delay","ts")])
                                    },
                                    reduce = function(k, vv) {
                                      #kikeressuk max timestamp-hez a keses merteket
                                      max_place <- which.max(vv[, c("ts")])
                                      last_delay <- vv[max_place, c("delay")]
                                      # csak azt engedjuk tovabb, ami nem 0 keses, sanszos, hogy az rossz adat (sok jaratnal vegig 0)
                                      if(last_delay!=0)keyval(k, last_delay)
                                    }
)

# MR2 jaratok median kesese naponkent
avg_delay_per_day <-  mapreduce(input = end_delay_per_journey, 
                                map = function(k, v){
                                  # A nap lesz a kulcs
                                  day <- weekdays(as.Date(k$time_frame))
                                  keyval(day, v)
                                },
                                reduce = function(k, vv) {
                                  keyval(k, median(vv))
                                }
)

# csinalunk belole dataframe-et
daily <- data.frame(days=from.dfs(avg_delay_per_day)$key, delay=from.dfs(avg_delay_per_day)$val)
daily$days <- factor(daily$days, levels= c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
daily <- daily[order(daily$days), ]

# Ehhez tartozo plot (lattice kell hozza)
plt <- xyplot(daily$delay ~ daily$days, type='b', xlab="Days", ylab="Delay")
update(plt, par.settings = list(fontsize = list(text = 25, points = 20)))

# -----------------------------------------------------------------------------------

# MR3 egyedi line, operator parok kinyerese trukkos aggregacioval
hlines_ops <-  mapreduce(input = hd0, 
                         map = function(., v)
                           keyval(v[, c("line_id","operator")], 1),
                         reduce = function(k, vv) {
                           keyval(k, 1)
                         }
)
# MR4.1 operatorok szama vonalankent
hops_per_line <-  mapreduce(input = hlines_ops, 
                            map = function(k, .)
                              keyval(k[, c("line_id")], 1),
                            reduce = function(k, vv) {
                              keyval(k, length(vv))
                            }
)
# MR4.2 peratorok szama vonalankent alternativ megoldas
hops_per_line <- mapreduce(hlines_ops, 
                             map = function(k, v)
                               keyval(k$line_id, 1), 
                             reduce = function(k, v) 
                               cbind(line = k, no_ops = sum(v, na.rm = TRUE)))

# MR5 vonalak szama operatoronkent
hlines_per_op <-  mapreduce(input = hlines_ops, 
                            map = function(k, .)
                              keyval(k[, c("operator")], 1),
                            reduce = function(k, vv) {
                              keyval(k, length(vv))
                            }
)
# MR6.1 atlagos keses vonalankent
hdelay_per_line <-  mapreduce(input = hd0, 
                              map = function(., v)
                                keyval(v[, c("line_id")],v[, c("delay")]),
                              reduce = function(k, vv) {
                                keyval(k, mean(vv))
                              }
)

# MR6.2 atlagos keses vonalankent 2. szamu megoldas

hdelay_per_line <- mapreduce(hd0, 
    map = function(k, v)
        keyval(v$line_id, v$delay), 
    reduce = function(k, v) 
        cbind(line = k, mean = mean(v, na.rm = TRUE)))
rread(hdelay_per_line,FALSE)

# Eredm?nyek kinyer?se
result <- from.dfs(hdelay_per_line)
d1 = data.frame(result)
colnames(d1) <- c('line_id','mean_delay')

result <- from.dfs(hops_per_line)
d2 = data.frame(result)
colnames(d2) <- c('line_id','operator')

# tablak kombinalasa
d3 = merge(x = d1, y = d2, by = "line_id", all = TRUE)

# Ehhez plotok

# Operatorok es kesesek scatter plot
p1 <- ggplot(d3, aes(x = operator, y = mean_delay))
p1 <- p1 + geom_point(color="blue", size = 4)
p1 + theme(axis.title=element_text(face="bold",size="20"), axis.text = element_text(size = 20), legend.position="top") + labs(x = "Number of Operators", y = "Delay")

# Operatorszam es iranyitott vonalszam eloszlas
bar <- barchart(d2$operator, horizontal=FALSE, xlab="Number of Operators", ylab="Number of Lines")
update(bar, par.settings = list(fontsize = list(text = 20)))

# Lines per operators
barplot(height=from.dfs(hlines_per_op)$val, names.arg=from.dfs(hlines_per_op)$key, xlab="Operators", ylab="Lines")


# MR7 atlagos keses orankent
hhourly_meandelay <-  mapreduce(input = hd0, 
                                map = function(., v)
                                  keyval(v[, c("vehicle_journey_id","time_frame","tst")], v[, c("delay")]),
                                reduce = function(k, vv) 
                                  keyval(k, mean(vv)) 
                                
)

#rread(hhourly_meandelay)

# MR8 szumma atlagos keses orankent
hhourly_totaldelay <-  mapreduce(input = hhourly_meandelay, 
                                map = function(k, v)
                                  keyval(k[, c("tst")], v),
                                reduce = function(k, vv) 
                                  keyval(k, sum(vv)) 
)
#rread(hhourly_totaldelay,FALSE)

#ddelay = data.frame(from.dfs(hhourly_meandelay))
#colnames(ddelay) <- c("vehicle_journey_id","time_frame","hour","total_delay")
#head(ddelay,10)
#ddelay = ddelay[order(ddelay$hour),]
#ddelay$date = as.Date(ddelay$hour, format="%Y-%b-%d %H")

ddelay = data.frame(from.dfs(hhourly_totaldelay))
colnames(ddelay) <- c('hour','total_delay')
ddelay = ddelay[order(ddelay$hour),]

#write.csv(ddelay, file='c:/Users/gergo/Documents/bigdatahf/ddelay.csv', sep=',', row.names=FALSE, quote=FALSE)

# Ehhez plot
plot <- ggplot( data = ddelay, aes( strptime(hour, "%Y-%B-%d %Hh"), total_delay /3600 )) + geom_line()
plot + theme(axis.title=element_text(face="bold",size="20"), axis.text = element_text(size = 20), legend.position="top") + labs(x = "Time (Hours)", y = "Delay (Hours)")


# MR9 szumma km orankent

hhourly_totaldist <-  mapreduce(input = hd0, 
                                map = function(., v)
                                  keyval(v[, c("tst")], v[, c("distdelta")]),
                                reduce = function(k, vv) 
                                  keyval(k, sum(vv,na.rm = TRUE)) 
)
#rread(hhourly_totaldist,FALSE)

dkm = data.frame(from.dfs(hhourly_totaldist))
colnames(dkm) <- c('tst','distdelta')
dkm = dkm[order(dkm$tst),]

# Plot az orankenti osszes kilometerhez
plot <- ggplot( data = dkm, aes( strptime(tst, "%Y-%B-%d %Hh"), distdelta)) + geom_line()
plot + theme(axis.title=element_text(face="bold",size="20"), axis.text = element_text(size = 20), legend.position="top") + labs(x = "Time (Hours)", y = "Total distance (km)")

# MR10 szumma jarmuvek orankent
hhourly_count <-  mapreduce(input = hd0, 
                                map = function(., v)
                                  keyval(v[, c("tst")], v[, c("vehicle_id")]),
                                reduce = function(k, vv) 
                                  keyval(k, length(unique(vv))) 
)
#rread(hhourly_count,FALSE)

dcount = data.frame(from.dfs(hhourly_count))
colnames(dcount) <- c('tst','vehicle_count')
dcount = dcount[order(dcount$tst),]

# Plot orankenti jarmumennyiseghez
plot2 <- ggplot( data = dcount, aes( strptime(tst, "%Y-%B-%d %Hh"), vehicle_count)) + geom_line()
plot2 + theme(axis.title=element_text(face="bold",size="20"), axis.text = element_text(size = 20), legend.position="top") + labs(x = "Time (Hours)", y = "Number of Vehicles on Journey")



# MR11 haversine tavolsag trukkos mapreduce-szal
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

rread(haversines)




#################### TERKEPES plotok ################
#####################################################

# sima plot
DublinMap <- qmap('dublin', zoom = 11,color = 'bw', legend = 'topleft')
DublinMap +geom_point(aes(x = lon, y = lat), data = subset(d0, at_stop == 1) )

# map line id alapjan
DublinMap <- qmap('dublin', zoom = 11,color = 'bw', legend = 'topleft')
DublinMap +geom_point(aes(x = lon, y = lat), size = 4, data = subset(d0, line_id == "54A") )

# map line id alapjan, zoomolva
DublinMap <- qmap('dublin', zoom = 15,color = 'bw', legend = 'topleft')
DublinMap +geom_point(aes(x = lon, y = lat), size = 4, data = subset(d0, line_id == "54A") )

# map operator alapjan szinezve
DublinMap <- qmap('dublin', zoom = 11,color = 'bw', legend = 'topleft')
DublinMap +geom_point(aes(x = lon, y = lat, colour = operator), data = subset(d0, at_stop == 1) )