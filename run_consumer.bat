@echo off
title Consumer
color b
bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --from-beginning --topic bokie
