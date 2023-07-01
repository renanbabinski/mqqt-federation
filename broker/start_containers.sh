#!/bin/sh

docker pull eclipse-mosquitto:1.6

docker run -d --name mosquitto0 -p 1880:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6
docker run -d --name mosquitto1 -p 1881:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6
docker run -d --name mosquitto2 -p 1882:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6
docker run -d --name mosquitto3 -p 1883:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6
docker run -d --name mosquitto4 -p 1884:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6
docker run -d --name mosquitto5 -p 1885:1883 -v ./mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:1.6

