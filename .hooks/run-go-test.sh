#!/usr/bin/env bash

go test $(go list ./...)

returncode=$?
if [ $returncode -ne 0 ]; then
  echo "go test failed"
  exit 1
fi
