.PHONY: all server-build local-run external_deps image-build image-run image-package

DOCKER_REPOSITORY?=

IMAGE_VERSION?=0.0.1
IMAGE_NAME?=cloudbridge-private_lift-server

SERVER_VERSION?=0.0.1
SERVER_JAR=server/build/libs/server-$(SERVER_VERSION).jar
SERVER_JAR_VERSION=`grep 'version = ' server/build.gradle | sed s"/^version = //" | sed s"/'*//g"`
JAR_FILE_PATH="server/build/libs/server-${SERVER_JAR_VERSION}.jar"

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
	docker build --build-arg JAR_FILE_PATH=$(JAR_FILE_PATH) -t $(CONTAINER_IMAGE) .
	docker tag $(CONTAINER_IMAGE) $(LOCAL_IMAGE)

	# Cleanup copied resources
	@echo "\nCleaning up dependencies..."
	$(RM) -r aws_terraform_template
	$(RM) -r key_injection_agent/kia_source_code
	$(RM) -r measurement_validation_agent/mva_source_code
	$(RM) -r clean_up_agent/clean_up_agent_source_code
	$(RM) config.yml
	@echo "Done"

image-run: image-build
	docker run -it --rm -p 10080:8080 $(CONTAINER_IMAGE)


clean:
	server/gradlew -p server clean
	$(RM) -r aws_terraform_template
	$(RM) -r key_injection_agent/kia_source_code
	$(RM) -r clean_up_agent/clean_up_agent_source_code
	$(RM) config.yml

distclean: clean
	docker rmi $(CONTAINER_IMAGE)


# Dockerfile will not accept these resources as links, so they need to be copied in
external_deps: mva_source_code kia_source_code clean_up_agent_source_code config.yml aws_terraform_template
	@echo "Dependencies Copied\n"

kia_source_code:
	mkdir -p key_injection_agent/kia_source_code
	mkdir -p key_injection_agent/kia_source_code/private_computation
	mkdir -p key_injection_agent/kia_source_code/private_computation/tee_lift
	mkdir -p key_injection_agent/kia_source_code/smart/
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/audit_log_service
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs/repository
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs/entity
	mkdir -p key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs/entity/measurement
	chmod +x key_injection_agent/kia_source_code
	cp -r ../../../private_computation/tee_lift/key_injection_agent/kia_runner.py key_injection_agent/kia_source_code/
	cp -r ../../../private_computation/tee_lift/key_injection_agent key_injection_agent/kia_source_code/private_computation/tee_lift
	cp -r ../../../private_computation/tee_lift/pc_crypto key_injection_agent/kia_source_code/private_computation/tee_lift
	cp -r ../../../private_computation/tee_lift/utils key_injection_agent/kia_source_code/private_computation/tee_lift
	cp -r ../../../smart/private_computation/audit_log_service/srcs/repository key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs
	cp -r ../../../smart/private_computation/audit_log_service/srcs/entity key_injection_agent/kia_source_code/smart/private_computation/audit_log_service/srcs


mva_source_code:
	mkdir -p measurement_validation_agent/mva_source_code
	mkdir -p measurement_validation_agent/mva_source_code/private_computation
	mkdir -p measurement_validation_agent/mva_source_code/private_computation/tee_lift
	mkdir -p measurement_validation_agent/mva_source_code/smart/
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs/repository
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs/entity
	mkdir -p measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs/entity/measurement
	chmod +x measurement_validation_agent/mva_source_code
	cp -r ../../../private_computation/tee_lift/measurement_validation_agent/measurement_validation_runner.py measurement_validation_agent/mva_source_code/
	cp -r ../../../private_computation/tee_lift/measurement_validation_agent measurement_validation_agent/mva_source_code/private_computation/tee_lift
	cp -r ../../../private_computation/tee_lift/pc_crypto measurement_validation_agent/mva_source_code/private_computation/tee_lift
	cp -r ../../../private_computation/tee_lift/utils measurement_validation_agent/mva_source_code/private_computation/tee_lift
	cp -r ../../../smart/private_computation/audit_log_service/srcs/repository measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs
	cp -r ../../../smart/private_computation/audit_log_service/srcs/entity measurement_validation_agent/mva_source_code/smart/private_computation/audit_log_service/srcs

clean_up_agent_source_code:
	mkdir -p clean_up_agent/clean_up_agent_source_code
	mkdir -p clean_up_agent/clean_up_agent_source_code/private_computation
	mkdir -p clean_up_agent/clean_up_agent_source_code/private_computation/tee_lift
	mkdir -p clean_up_agent/clean_up_agent_source_code/private_computation/tee_lift/pl_clean_up
	chmod +x clean_up_agent/clean_up_agent_source_code
	cp -r ../../../private_computation/tee_lift/pl_clean_up/pl_run_clean_up.py clean_up_agent/clean_up_agent_source_code/
	cp -r ../../../private_computation/tee_lift/pl_clean_up/pl_clean_up_input.py clean_up_agent/clean_up_agent_source_code/private_computation/tee_lift/pl_clean_up/
	cp -r ../../../private_computation/tee_lift/utils clean_up_agent/clean_up_agent_source_code/private_computation/tee_lift

config.yml:
	cp ../../private_computation_cli/config.yml .

aws_terraform_template:
	cp -r ../pce/aws_terraform_template .
