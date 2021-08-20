#!/bin/bash
source /etc/profile
vardate=$(date +%c)
cd /data/table/safety
#/root/anaconda3/bin/python3 cesp_model.py && echo "${vardate}: runing success!"
spark-submit --master local[*] --num-executors 1 --executor-memory 16G --executor-cores 8 cesp_model_spark.py  && echo "${vardate}: runing succeed!"


