dockerpushamd:
	docker buildx build  \
--tag unifactmanufacturinghub/mqttkafkabridge:linuxamd0.3  \
--platform "linux/amd64"  \
--builder benthos-container  --push .