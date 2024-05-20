dockerpushamd:
	docker buildx build  \
--tag unifactmanufacturinghub/mqttkafkabridge:linuxamd  \
--platform "linux/amd64"  \
--builder benthos-container  --push .