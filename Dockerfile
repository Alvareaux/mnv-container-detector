FROM python:3.11 AS build
WORKDIR /

ARG gar_cred
ENV extra_index_url europe-west3-python.pkg.dev/stopfakenews/mnv-pypi/simple/

# Upgrade pip
RUN /usr/local/bin/python -m pip install --upgrade pip

# Python dependencies
COPY requirements.txt  ./requirements.txt
RUN pip3 install --extra-index-url=https://_json_key_base64:${gar_cred}@${extra_index_url} -r requirements.txt

FROM build

# Copy all container files
COPY service_detector/  ./service_detector/

ENTRYPOINT ["python", "service_detector/container_detector/container_detector.py"]
