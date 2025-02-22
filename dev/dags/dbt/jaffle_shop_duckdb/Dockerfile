FROM mcr.microsoft.com/vscode/devcontainers/python:3.9

ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN if [ "$USER_GID" != "1000" ] || [ "$USER_UID" != "1000" ]; then groupmod --gid $USER_GID vscode && usermod --uid $USER_UID --gid $USER_GID vscode; fi

RUN pwd
RUN ls
COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip
RUN pip3 install --requirement /tmp/requirements.txt
