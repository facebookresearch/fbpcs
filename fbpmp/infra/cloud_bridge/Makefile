.PHONY: all server-build local-run external_deps image-build image-run image-package

DOCKER_REPOSITORY?=

IMAGE_VERSION?=0.0.1
IMAGE_NAME?=cloudbridge-private_lift-server

SERVER_VERSION?=0.0.1
SERVER_JAR=server/build/libs/server-$(SERVER_VERSION).jar

SOURCE_FILES=$(wildcard server/src/main/java/com/facebook/business/cloudbridge/pl/server/*.java)

LOCAL_IMAGE=$(IMAGE_NAME):$(IMAGE_VERSION)
ifndef $(DOCKER_REPOSITORY)
CONTAINER_IMAGE:=$(LOCAL_IMAGE)
else
CONTAINER_IMAGE:=$(DOCKER_REPOSITORY)/$(LOCAL_IMAGE)
endif

all: image-build



$(SERVER_JAR): $(SOURCE_FILES)

server-build $(SERVER_JAR):
	server/gradlew -p server build

local-run:
	server/gradlew -p server bootRun



image-build: $(SERVER_JAR) external_deps
	docker build -t $(CONTAINER_IMAGE) .
	docker tag $(CONTAINER_IMAGE) $(LOCAL_IMAGE)

	# Cleanup copied resources
	@echo "\nCleaning up dependencies..."
	$(RM) -r aws_terraform_template
	$(RM) config.yml
	@echo "Done"

image-run: image-build
	docker run -it --rm -p 10080:8080 $(CONTAINER_IMAGE)


clean:
	server/gradlew -p server clean
	$(RM) -r aws_terraform_template
	$(RM) config.yml

distclean: clean
	docker rmi $(CONTAINER_IMAGE)


# Dockerfile will not accept these resources as links, so they need to be copied in
external_deps: config.yml aws_terraform_template
	@echo "Dependencies Copied\n"

config.yml:
	cp ../../pl_coordinator/config.yml .

aws_terraform_template:
	cp -r ../pce/aws_terraform_template .
