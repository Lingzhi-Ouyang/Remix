FROM openjdk:11
WORKDIR /usr/src/Project
COPY . .
RUN cp /etc/apt/sources.list /etc/apt/sources.list.bk && \
  cp sources.list /etc/apt/sources.list
RUN apt update -y && apt install -y --no-install-recommends tzdata && \
  ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
  dpkg-reconfigure -f noninteractive tzdata && \
  apt-get clean && rm -rf /var/lib/apt/lists/*
RUN apt update -y && apt install -y --no-install-recommends build-essential vim curl && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

