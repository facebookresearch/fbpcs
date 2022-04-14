.PHONY: all image-build thrift-build

DOCKER_REPOSITORY?=

IMAGE_VERSION?=0.0.1
IMAGE_NAME?=pc-logging_service-server

SERVER_VERSION?=0.0.1

LOCAL_IMAGE=$(IMAGE_NAME):$(IMAGE_VERSION)
ifndef $(DOCKER_REPOSITORY)
CONTAINER_IMAGE:=$(LOCAL_IMAGE)
else
CONTAINER_IMAGE:=$(DOCKER_REPOSITORY)/$(LOCAL_IMAGE)
endif

all: thrift-build image-build

thrift-build:
	thrift -r --gen py -out . thrift/logging_service.thrift
	@echo "Done"

image-build:
	docker build -t $(CONTAINER_IMAGE) .
	docker tag $(CONTAINER_IMAGE) $(LOCAL_IMAGE)
	@echo "Done"

image-run: image-build
	docker run -it --rm -p 9090:9090 $(CONTAINER_IMAGE)

distclean:
	@echo "Remaining images:"
	docker image ls
	@echo "Deleting image: $(CONTAINER_IMAGE)"
	docker rmi $(CONTAINER_IMAGE)
