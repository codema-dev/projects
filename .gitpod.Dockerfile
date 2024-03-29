FROM gitpod/workspace-full

RUN  wget -qO- https://micromamba.snakepit.net/api/micromamba/linux-64/latest \
    | tar -xvj bin/micromamba
    
RUN ./bin/micromamba shell init -s bash -p ~/micromamba \
    && echo "alias conda=micromamba" >> ~/.bashrc