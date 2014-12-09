library('rmr2')
rmr.options(backend="local")

filenames <- list.files(path = "D:Downloads/bigdata_data", full.names = TRUE)
filenames
aggreg <- do.call("rbind", lapply(filenames[4:13], read.csv, col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop')))
aggreg$id <- 1:nrow(aggreg)
hd0 <- to.dfs(aggreg)
td0 <- to.dfs(head(aggreg,10000))

# mapreduce

end_delay_per_journey <-  mapreduce(input = hd0, 
                         map = function(., v){
                           #day <- weekdays(as.Date(as.POSIXct(v$ts/1e6, origin="1970-01-01")))
                           keyval(v[, c("vehicle_journey_id","time_frame")], v[, c("delay","ts")])
                         },
                         reduce = function(k, vv) {
                           max_place <- which.max(vv[, c("ts")])
                           last_delay <- vv[max_place, c("delay")]
                           # spéci combiner
                           if(last_delay!=0)keyval(k, last_delay) # csak azt engedjük tovább, ami nem 0, sanszos, hogy az rossz adat
                         }
)

# járatok átlagos késése naponként
avg_delay_per_day <-  mapreduce(input = end_delay_per_journey, 
                                map = function(k, v){
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


# plotok


# operator <--> járatok száma bar chart
barplot(height=from.dfs(hlines_per_op)$val, names.arg=from.dfs(hlines_per_op)$key, xlab="Operators", ylab="Lines")
# járatok átlagos késése naponként
barplot(height=daily$delay, names.arg=daily$days, xlab="Days", ylab="Delay")
# linechart
plt <- xyplot(daily$delay ~ daily$days, type='b', xlab="Days", ylab="Delay")
update(plt, par.settings = list(fontsize = list(text = 25, points = 20)))

DublinMap <- qmap('dublin', zoom = 11,color = 'bw', legend = 'topleft')
DublinMap +geom_point(aes(x = lon, y = lat), data = subset(aggreg, at_stop == 1) )