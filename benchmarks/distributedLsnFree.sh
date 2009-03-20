echo sw
time ./distributedLsnFree writeback  1000 10 1000
echo sn
time ./distributedLsnFree normal  1000 10 1000
echo swn
time ./distributedLsnFree writeback-net  1000 10 1000
echo swp
time ./distributedLsnFree writeback-pipeline  1000 10 1000
echo snn
time ./distributedLsnFree normal-net  1000 10 1000


echo w
time ./distributedLsnFree writeback  1000 10 100000
echo n
time ./distributedLsnFree normal  1000 10 100000
echo wn
time ./distributedLsnFree writeback-net  1000 10 100000
echo wp
time ./distributedLsnFree writeback-pipeline  1000 10 100000
date
echo nn
time ./distributedLsnFree normal-net  1000 10 100000
