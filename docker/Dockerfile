FROM python:3.7

# Use UTF-8 as the default locale
ENV LANG=C.UTF-8

# Better movement with [Ctrl + →] and [Ctrl + ←]
ENV INPUTRC=/etc/inputrc

RUN set -ex \
  && echo '"\e[1;5C": forward-word' >> /etc/inputrc \
  && echo '"\e[1;5D": backward-word' >> /etc/inputrc

# Set working directory.
# Mount your project under the same path.
WORKDIR /usr/src

# Install useful programs
RUN apt-get update -y && apt-get install -y \
      less \
      git

# Install DVC with your specified files
RUN pip install "dvc[all]"

# Enable autocompletion scripts for DVC
RUN wget \
    -O /etc/bash_completion.d/dvc \
    https://raw.githubusercontent.com/iterative/dvc/master/scripts/completion/dvc.bash

# Add a user to run DVC
ARG USER_ID=1000
RUN useradd --create-home -u ${USER_ID} dvc
USER dvc
