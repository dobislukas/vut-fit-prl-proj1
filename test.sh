#!/bin/bash

#compile
mpic++ --prefix /usr/local/share/OpenMPI -o pms pms.cpp

#create "numbers" file
dd if=/dev/random bs=1 count=16 of=numbers status=none

#run
mpirun --prefix /usr/local/share/OpenMPI -np 5 pms

#clean
rm -f pms numbers
