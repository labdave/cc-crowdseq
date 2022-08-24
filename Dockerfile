# Base Image
FROM python:3.8

# Metadata
LABEL base.image="cc-crowdseq:v1.0.0"
LABEL version="1"
LABEL software="cc-crowdseq:v1.0.0"
LABEL software.version="1.0.0"
LABEL author="labdave"
LABEL maintainer="lab.dave@gmail.com"
LABEL description="Davelab scripts for CloudConductor and Crowdseq integration"

# Install gcloud
# RUN curl -sSL https://sdk.cloud.google.com > /tmp/gcl &&\
#     bash /tmp/gcl --disable-prompts &&\
#     echo "if [ -f '/root/google-cloud-sdk/path.bash.inc' ]; then source '/root/google-cloud-sdk/path.bash.inc'; fi" >> /root/.bashrc &&\
#     echo "if [ -f '/root/google-cloud-sdk/completion.bash.inc' ]; then source '/root/google-cloud-sdk/completion.bash.inc'; fi" >> /root/.bashrc
# ENV PATH /root/google-cloud-sdk/bin:$PATH

COPY . /code
WORKDIR /code

RUN pip install -r requirements.txt
