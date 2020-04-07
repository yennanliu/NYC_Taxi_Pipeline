#!/bin/bash
# sample nyc event data from flink-streaming-demo repo :  https://github.com/dataArtisans/flink-streaming-demo

head -3000 flink-streaming-demo/data/nycTaxiData >> NYC_Taxi_Pipeline/data/nyc_event/event.txt