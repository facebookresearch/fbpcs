.PHONY: all distclean image-build image-run

DOCKER_REPOSITORY?=

IMAGE_VERSION?=0.0.1
IMAGE_NAME?=pl-coordinator-env-client-test

SERVER_VERSION?=0.0.1

LOCAL_IMAGE=$(IMAGE_NAME):$(IMAGE_VERSION)
ifndef $(DOCKER_REPOSITORY)
CONTAINER_IMAGE:=$(LOCAL_IMAGE)
else
CONTAINER_IMAGE:=$(DOCKER_REPOSITORY)/$(LOCAL_IMAGE)
endif

TMP_DIR=make_tmp
FBPCS_DIR=../../../../../fbpcs

all: image-build

image-build: copy_fbpcs
	docker build -t $(CONTAINER_IMAGE) "$(FBPCS_DIR)/../$(TMP_DIR)/"
	docker tag $(CONTAINER_IMAGE) $(LOCAL_IMAGE)

	# clean up the temporary files copied from other directories
	rm -fr "$(FBPCS_DIR)/../$(TMP_DIR)/"
	@echo "Done"

image-run: image-build
	docker run -it --rm $(CONTAINER_IMAGE)

distclean:
	@echo "Remaining images:"
	docker image ls
	@echo "Deleting image: $(CONTAINER_IMAGE)"
	docker rmi $(CONTAINER_IMAGE)

copy_fbpcs:
	rm -fr "$(FBPCS_DIR)/../$(TMP_DIR)/"
	mkdir -p "$(FBPCS_DIR)/../$(TMP_DIR)/"
	cp -r "$(FBPCS_DIR)" "$(FBPCS_DIR)/../$(TMP_DIR)/fbpcs"
	cp "$(FBPCS_DIR)/Dockerfile" "$(FBPCS_DIR)/../$(TMP_DIR)"
