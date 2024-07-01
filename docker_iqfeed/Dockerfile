FROM python:3.10.13-bookworm

# As of Jan 2024, Debian makes the proper Wine packages for running on the 
# ARM64 chipset that is used by Apple. To build this image on Mac M1/M2 chips:
# docker build --platform linux/amd64 -t fractalgambit/iqfeed:6.2 ./dtn/docker_iqfeed
# This prebuilt image is loaded to Docker Hub and used in the deployment pipeline.
ARG DTN_SOURCE=https://www.iqfeed.net/downloads/download_iqfeed.cfm?version=6.2
ARG PYTHON_VERSION=https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tgz

ENV DEBIAN_FRONTEND noninteractive

WORKDIR /root/

# Install libraries and 64-bit Wine with i386 as a 32-bit foreign architecture.
RUN dpkg --add-architecture i386 && \
    apt-get update -qq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --install-recommends \
      wget \
      wine \
      wine32 \
      wine64 \
      libwine \
      libwine:i386 \
      fonts-wine \
      cabextract \
      xvfb

# Download and install IQFeed.
# Running xvfb creates a headless fake X-server to run .exe files without a GUI.
# Running winecfg to initialize a new wine prefix can sometimes resolve issues.
ENV WINEDEBUG -all
ARG IQFEED_PATH=/root/.wine/drive_c/iqfeed_install.exe
RUN rm -rf ~/.wine && \
    WINEARCH=win64 WINEPREFIX=~/.wine winecfg && \
    echo $(wine --version) && \
    wget -nv $DTN_SOURCE -O $IQFEED_PATH && \
    chmod +x $IQFEED_PATH

ENV DISPLAY :99
RUN Xvfb $DISPLAY -nolisten tcp & wine $IQFEED_PATH /S

# Install pip3 for installing libraries.
RUN apt-get install -yqq --install-recommends python3-pip && \
    pip3 install --upgrade pip

# Use a separate Dockerfile that loads this prebuilt image, launches iqconnect,
# and the Python API wrapper. This will accelerate the deployment time.