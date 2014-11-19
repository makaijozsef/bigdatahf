install.packages(c('Rcpp', 'RJSONIO', 'bitops', 'digest', 'functional', 'stringr', 
                   'plyr', 'reshape2','caTools'))
install.packages('C:/Users/Gergo/Downloads/rmr2_2.3.0.tar.gz')
library('rmr2')
library('devtools')

#install_github('rmr2', 'RevolutionAnalytics', subdir='pkg')
rmr.options(backend="local")

a <- mapreduce(input="http://www.gutenberg.org/cache/epub/4300/pg4300.txt", input.format="text")
a
a()

b <- from.dfs(a)
typeof(b)
names(b)
b[[1]][71:74]
b[[2]][71:74]

a <- mapreduce(input="http://www.gutenberg.org/cache/epub/4300/pg4300.txt", 
               input.format="text",
               
               map=function(k,v){
                 keyval(key=str_length(v),
                        val=1)},
               
               reduce=function(rowlen, counts){
                 keyval(key=rowlen,
                        val=sum(counts))}
)()


# send groups ID (randomly generated from a binomial) to Hadoop filesystem
groups = rbinom(32, n = 50, prob = 0.4)
groups = to.dfs(groups)
# run a mapreduce job
## map: key value is the group id, value is 1
## reduce: count the number of observations in each group
## then, retrieve it from Hadoop filesystem
output = from.dfs(mapreduce(input = groups, 
                            map = function(., v) keyval(v, 1), 
                            reduce = function(k, vv)
                              keyval(k, length(vv))))
# print results
## keys: group IDs
## values: results of reduce job (<em>i.e.</em>, frequency)
data.frame(key=keys(output),val=values(output))


small.ints = to.dfs(1:1000)
output = from.dfs(mapreduce(
  input = small.ints, 
  map = function(k, v) keyval(v, v^2)))

groups = rbinom(32, n = 50, prob = 0.4)
tapply(groups, groups, length)

groups = to.dfs(groups)
from.dfs(
  mapreduce(
    input = groups, 
    map = function(., v) keyval(v, 1), 
    reduce = 
      function(k, vv) 
        keyval(k, length(vv))))


wordcount = 
  function(
    input, 
    output = NULL, 
    pattern = " "){
    wc.map = 
      function(., lines) {
        keyval(
          unlist(
            strsplit(
              x = gsub("[^[:alnum:] ]", "", tolower(lines)),
              split = pattern)),
          1)}
    wc.reduce =
      function(word, counts ) {
        keyval(word, sum(counts))}
    
    mapreduce(
      input = input ,
      output = output,
      input.format = "text",
      map = wc.map,
      reduce = wc.reduce,
      combine = T)}

output = from.dfs(wordcount(input = 'e:/munka/BME/BigData/bogancs.txt'))
d0 = data.frame(key=keys(output),val=values(output))
d0 = d0[with(d0, order(-val)), ]
head(d0,100)

rmr.options(backend="local")
rmr.options(backend="hadoop") # With this option, the following script fails

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
# operatoror szama vonalankent
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
