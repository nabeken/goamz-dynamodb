#!/bin/bash
set -e
export TEST_DIR=$(cd `dirname $0`; pwd)
DYNAMODB_LOCAL_VERSION=2014-04-24
DYNAMODB_LOCAL_DIR="${TEST_DIR}/dynamodb_local"

cd "${TEST_DIR}"

if [ ! -f "dynamodb_local_${DYNAMODB_LOCAL_VERSION}.tar.gz" ]; then
  curl -O "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_${DYNAMODB_LOCAL_VERSION}.tar.gz"
fi

if [ ! -d "${DYNAMODB_LOCAL_DIR}" ]; then
  mkdir "${DYNAMODB_LOCAL_DIR}" && \
  tar -C "${DYNAMODB_LOCAL_DIR}" -zxf dynamodb_local_${DYNAMODB_LOCAL_VERSION}.tar.gz
fi
