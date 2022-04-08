.PHONY: all thrift-build

all: thrift-build

thrift-build:
	thrift -r --gen py -out . thrift/logging_service.thrift
	@echo "Done"
