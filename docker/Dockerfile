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
      zsh \
      less \
      git

# Set Zsh as the default shell
SHELL ["/usr/bin/zsh", "-c"]

# Install DVC with your specified files
RUN pip install "dvc[all]"

# Enable autocompletion scripts for DVC
RUN wget \
      -O /usr/local/share/zsh/site-functions/_dvc \
      https://raw.githubusercontent.com/iterative/dvc/master/scripts/completion/dvc.zsh

# Add a user to run DVC
ARG USER_ID=1000
RUN useradd --create-home -u ${USER_ID} dvc
USER dvc

# Load and rice the completion menu with `zstyle`:
# http://zsh.sourceforge.net/Doc/Release/Zsh-Modules.html#The-zsh_002fzutil-Module
RUN echo "autoload -U compinit && compinit" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:*:*:*:*' menu select" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:matches' group 'yes'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:options' description 'yes'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:options' auto-description '%d'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:corrections' format ' %F{green}-- %d (errors: %e) --%f'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:descriptions' format ' %F{yellow}-- %d --%f'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:messages' format ' %F{purple} -- %d --%f'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:warnings' format ' %F{red}-- no matches found --%f'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*:default' list-prompt '%S%M matches%s'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*' format ' %F{yellow}-- %d --%f'" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*' group-name ''" >> $HOME/.zshrc \
      && echo "zstyle ':completion:*' verbose yes" >> $HOME/.zshrc

CMD ["zsh"]

# vim: ft=dockerfile
