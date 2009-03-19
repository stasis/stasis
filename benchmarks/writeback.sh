echo w1
time ./writeBack writeback 100 1 1000000

echo n1
time ./writeBack normal 100 1 1000000

echo w10
time ./writeBack writeback 100 10 100000

echo n10
time ./writeBack normal 100 10 100000

echo w100
time ./writeBack writeback 100 100 10000

echo n100
time ./writeBack normal 100 100 10000

echo w1000
time ./writeBack writeback 100 1000 1000

echo n1000
time ./writeBack normal 100 1000 1000

echo w10000
time ./writeBack writeback 100 10000 100

echo n10000
time ./writeBack normal 100 10000 100

#echo w100000
#time ./writeBack writeback 100 100000 10

#echo n100000
#time ./writeBack normal 100 100000 10

#echo w1000000
#time ./writeBack writeback 100 1000000 1

#echo n1000000
#time ./writeBack normal 100 1000000 1


