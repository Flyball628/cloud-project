
FROM pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime

# 安装 torchvision
RUN pip install torchvision -i https://pypi.tuna.tsinghua.edu.cn/simple

# 创建目录
RUN mkdir -p /app/data /app/code

# 复制训练脚本
COPY train_mnist_ddp.py /app/code/

# 设置工作目录
WORKDIR /app

# 预下载 MNIST 数据（使用可访问的镜像源）
RUN python -c "\
import torchvision.datasets as datasets; \
from torchvision import transforms; \
import ssl; \
ssl._create_default_https_context = ssl._create_unverified_context; \
datasets.MNIST.resources = [ \
    ('https://ossci-datasets.s3.amazonaws.com/mnist/train-images-idx3-ubyte.gz', 'f68b3c2dcbeaaa9fbdd348bbdeb94873'), \
    ('https://ossci-datasets.s3.amazonaws.com/mnist/train-labels-idx1-ubyte.gz', 'd53e105ee54ea40749a09fcbcd1e9432'), \
    ('https://ossci-datasets.s3.amazonaws.com/mnist/t10k-images-idx3-ubyte.gz', '9fb629c4189551a2d022fa330f9573f3'), \
    ('https://ossci-datasets.s3.amazonaws.com/mnist/t10k-labels-idx1-ubyte.gz', 'ec29112dd5afa0611ce80d1b7f02629c') \
]; \
print('Downloading MNIST dataset from mirror...'); \
datasets.MNIST('./data', train=True, download=True, transform=transforms.ToTensor()); \
print('MNIST downloaded successfully to /app/data'); \
"

# 验证数据下载成功
RUN ls -la /app/data/

# 设置环境变量
ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/code/train_mnist_ddp.py"]
