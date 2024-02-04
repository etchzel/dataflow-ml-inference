FROM apache/apache/beam_python3.11_sdk:2.53.0

RUN pip install opencv-python-headless==4.9.0.80
RUN apt-get update && apt-get dist-upgrade

ENTRYPOINT [ "/opt/apache/beam/boot" ]