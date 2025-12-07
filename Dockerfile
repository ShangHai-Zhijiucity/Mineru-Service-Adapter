FROM nvidia/cuda:12.9.1-cudnn-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    git \
    wget \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3.10 /usr/bin/python

WORKDIR /app

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip -i https://mirrors.aliyun.com/pypi/simple

RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple
RUN pip install --no-cache-dir "mineru[core]" -i https://mirrors.aliyun.com/pypi/simple

COPY . .

EXPOSE 58001

CMD ["python", "main.py"]

