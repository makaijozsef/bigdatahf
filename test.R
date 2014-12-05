library('rmr2')
rmr.options(backend="local")

filenames <- list.files(path = "D:Downloads/bigdata_data", full.names = TRUE)
filenames
aggreg <- do.call("rbind", lapply(filenames[1:10], read.csv, col.names = c('ts','line_id','direction','journey_pattern_id','time_frame','vehicle_journey_id','operator','congestion','lon','lat','delay','block_id','vehicle_id','stop_id','at_stop')))
aggreg$id <- 1:nrow(aggreg)
hd0 <- to.dfs(aggreg)

