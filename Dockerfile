FROM apache/beam_python3.8_sdk:2.28.0

WORKDIR /pipeline

COPY ./ /pipeline/

