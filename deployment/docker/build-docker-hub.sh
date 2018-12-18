#!/bin/bash
export DOCKERHUB_IMAGE=emigateway
export DOCKERHUB_TAG=0.0.6

rm -rf deployment/docker/emi-gateway/
cp -R $API_SHELL_PATH/emi-gateway deployment/docker/emi-gateway

docker build -m "300M" -t $DOCKERHUB_NAMESPACE/$DOCKERHUB_IMAGE:$DOCKERHUB_TAG -t $DOCKERHUB_NAMESPACE/$DOCKERHUB_IMAGE:latest deployment/docker/
docker login -u $DOCKERHUB_USER -p $DOCKERHUB_PASS
docker push $DOCKERHUB_NAMESPACE/$DOCKERHUB_IMAGE:$DOCKERHUB_TAG
docker push $DOCKERHUB_NAMESPACE/$DOCKERHUB_IMAGE:latest