FROM nvidia/cuda:12.9.1-cudnn-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# 配置国内镜像源（清华大学镜像）
RUN if [ -f /etc/apt/sources.list ]; then \
        sed -i 's@//.*archive.ubuntu.com@//mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list && \
        sed -i 's@//.*security.ubuntu.com@//mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list; \
    fi && \
    if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
        sed -i 's@https://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list.d/ubuntu.sources && \
        sed -i 's@https://.*security.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list.d/ubuntu.sources; \
    fi && \
    if ls /etc/apt/sources.list.d/cuda-*.list >/dev/null 2>&1; then \
        sed -i 's@https://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g' /etc/apt/sources.list.d/cuda-*.list; \
    fi

RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    git \
    wget \
    libgl1 \
    libglib2.0-0 \
    curl \
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
