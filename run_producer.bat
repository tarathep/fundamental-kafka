@echo off
title Producer
color e
bin/windows/kafka-console-producer.bat --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic bokie
