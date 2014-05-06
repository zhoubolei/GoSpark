#!/bin/bash
# ssh-multi
# to ssh to multiple servers


hosts=(vision25 vision26 vision27 vision29 vision30 vision31 vision32 vision33 vision34 vision36 vision37 vision38)
UNAME=hduser
for i in "${hosts[@]}"
do
scp -r /scratch/bolei/hadoop $UNAME@$i:/scratch/bolei/
done
