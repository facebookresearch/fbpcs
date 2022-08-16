.PHONY: all external_deps image-build image-run

DOCKER_REPOSITORY?=

IMAGE_VERSION?=0.0.1
IMAGE_NAME?=mrpid-publisher-side


LOCAL_IMAGE=$(IMAGE_NAME):$(IMAGE_VERSION)
ifndef $(DOCKER_REPOSITORY)
CONTAINER_IMAGE:=$(LOCAL_IMAGE)
else
CONTAINER_IMAGE:=$(DOCKER_REPOSITORY)/$(LOCAL_IMAGE)
endif

all: image-build


image-build: external_deps
	docker build -t $(CONTAINER_IMAGE) .
	docker tag $(CONTAINER_IMAGE) $(LOCAL_IMAGE)

	# Cleanup copied resources
	@echo "\nCleaning up dependencies..."
	$(RM) -r aws_terraform_template_initial
	$(RM) -r aws_terraform_template_final
	$(RM) config.yml
	@echo "Done"


image-run: image-build
	docker run -it --rm -p 10080:8080 $(CONTAINER_IMAGE)


clean:
	$(RM) -r aws_terraform_template_initial
	$(RM) -r aws_terraform_template_final
	$(RM) config.yml


distclean: clean
	docker rmi $(CONTAINER_IMAGE)


# Dockerfile will not accept these resources as links, so they need to be copied in
external_deps: config.yml aws_terraform_template_initial aws_terraform_template_final
	@echo "Dependencies Copied\n"

config.yml:
	cp ../../../private_computation_cli/config.yml .

aws_terraform_template_initial:
	cp -r ../../pid/aws_terraform_template/publisher/initial_script .

aws_terraform_template_final:
	cp -r ../../pid/aws_terraform_template/publisher/final_script .
