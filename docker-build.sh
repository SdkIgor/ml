#!/usr/bin/env bash
source .env
echo ${IMAGE_NAME}
docker build -t ${IMAGE_NAME} .
# docker push ${IMAGE_NAME}
