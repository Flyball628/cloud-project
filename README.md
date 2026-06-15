# 云计算课程设计 - 云原生微服务应用

> **组员**：倪浩程 (2023112684)、税宏鸣 (2023114708)
>
> **华为云账号**：共用同一账号，Region: `cn-east-3`
>
> **说明**：本项目分两部分。第一部分（50分）已完成云平台搭建、监控和 CI/CD；第二部分（40分）基于已有环境继续并行编程实战。

---

## 一、项目架构

```
                    华为云 CCE (K8s 集群)
┌───────────────────────────────────────────────────────────┐
│                                                            │
│   Frontend (Nginx)  ──反向代理──>  Backend (Flask ×2)     │
│   ELB 对外暴露                       │                    │
│                                      ▼                    │
│                                 Redis (带 PVC 持久化)      │
│                                                            │
│   ┌──────────────────────────────────────────────────┐    │
│   │  监控: Prometheus + Grafana + Alertmanager       │    │
│   │  (kube-prometheus-stack Helm 部署)               │    │
│   │  Grafana 自定义 Dashboard: CPU 折线图 + 内存柱状图│    │
│   └──────────────────────────────────────────────────┘    │
│                                                            │
│   Spark Operator (PySpark) / PyTorchJob (DDP) / MPI Operator (mpi4py) │
│                                                            │
│   HPA: Backend 1-4 副本 (CPU > 60% 自动扩容)               │
└───────────────────────────────────────────────────────────┘
         ▲                              │
         │  CI/CD 自动构建推送镜像       │  ELB 对外访问
         │                              ▼
   GitHub Actions              互联网用户/浏览器
   → 华为云 SWR 镜像仓库
```

---

## 二、目录结构

```
cloud-project/
├── .github/workflows/
│   └── ci-cd.yml                    # CI/CD 流水线（已完成）
├── docker-compose.yml               # 本地开发 Docker Compose
├── README.md                        # 本文档
├── backend/
│   ├── app.py                       # Flask 后端 API
│   ├── Dockerfile                   # 后端镜像
│   └── requirements.txt             # Python 依赖（flask, redis, pandas）
├── frontend/
│   ├── Dockerfile                   # 前端 Nginx 镜像
│   ├── nginx.conf                   # Nginx 反向代理配置
│   └── static/
│       └── index.html               # 前端页面（含学号姓名）
├── k8s/
│   ├── backend-deployment.yaml      # 后端 Deployment（2 副本）
│   ├── backend-service.yaml         # 后端 LoadBalancer Service
│   ├── frontend-deployment.yaml     # 前端 Deployment + Service
│   ├── configmap.yaml               # 后端环境变量 ConfigMap
│   ├── secret.yaml                  # Redis 密码 Secret
│   ├── redis-deployment-with-pvc.yaml  # Redis + PVC 持久化
│   ├── redis-deployment.yaml        # Redis 基础版（备用）
│   ├── redis-service.yaml           # Redis ClusterIP Service
│   ├── hpa.yaml                     # 水平自动扩缩容
│   ├── nginx-configmap.yaml         # Nginx ConfigMap（Volume 挂载）
│   ├── pvc.yaml                     # 持久卷声明（华为云 EVS）
│   ├── prometheus-deployment.yaml   # Prometheus 轻量部署（备用）
│   ├── prometheus-configmap.yaml    # Prometheus 抓取配置（备用）
│   └── grafana-deployment.yaml      # Grafana 轻量部署（备用）
├── spark/
│   ├── sparkapplication.yaml        # Spark 作业模板（已更新）
│   ├── wordcount.py                 # PySpark WordCount 示例
│   ├── douban_cleaning.py           # 数据清洗 + Spark SQL 分析（A-1 & A-2）
│   └── comparison.py                # PySpark 性能基准测试（A-3）
├── mpi/
│   ├── mpijob.yaml                  # MPI 作业模板
│   └── pi_mpi.py                    # 蒙特卡洛求 π 示例
├── ml/
│   ├── Dockerfile                   # PyTorch 训练镜像
│   ├── pytorchjob.yaml             # PyTorchJob DDP 分布式训练（C-0~C-3）
│   ├── train_mnist_ddp.py          # CNN + DDP 训练脚本
│   └── data/                        # MNIST 数据目录
├── performance_cmp/
│   ├── pandas_test.py               # Pandas 单机性能测试
│   ├── pyspark_test.py              # PySpark 性能测试
│   ├── plot_results.py              # 绘制对比图表 + Amdahl 分析
│   ├── douban_movies.csv            # 豆瓣电影数据集
│   └── query1_performance_comparison.png  # 性能对比图
└── 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/
    ├── 课程设计任务书.docx
    ├── 课设问题合集.pdf
    └── 离线包/
        ├── monitoring/              # kube-prometheus-stack + 镜像
        ├── spark/                   # Spark Operator + PySpark 镜像
        └── mpi/                     # MPI Operator + mpi4py 镜像
```

---

## 三、第一部分：云平台搭建（已完成 ✅，50分）

| 任务 | 分值 | 状态 | 关键产出 |
|------|------|------|---------|
| 任务1 应用容器化 | 10分 | ✅ | Dockerfile（多阶段构建 + pandas）；Frontend 含学号姓名；镜像已推 SWR |
| 任务2 CCE 集群搭建 | 8分 | ✅ | 2 Worker 节点 Ready，版本 ≥1.27 |
| 任务3 应用部署 | 12分 | ✅ | Backend 2副本 + Redis；ELB 公网可访问 `/api/ping` |
| 任务4 持久化存储 | 10分 | ✅ | Redis PVC（csi-disk 10Gi）；Pod 重建数据不丢失 |
| 任务5 ConfigMap 挂载 | 5分 | ✅ | Nginx 配置以 Volume 形式挂载 |
| 任务6 HPA 弹性伸缩 | 5分 | ✅ | HPA 1-4 副本，CPU 60% 触发 |

### 附加题（已完成 ✅，+10分）

| 附加题 | 分值 | 状态 | 关键产出 |
|--------|------|------|---------|
| 附加题1 监控系统 | +5分 | ✅ | kube-prometheus-stack；Grafana Dashboard（CPU折线图 + 内存柱状图） |
| 附加题2 CI/CD | +5分 | ✅ | GitHub Actions → SWR；镜像更新验证通过 |

---

## 四、第二部分：并行编程实战（已完成 ✅，40分）

### 4.1 方向 A：Spark 大数据分析（已完成 ✅）

#### A-0 环境部署（10分）
```bash
# 1. 导入离线镜像
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/spark/pyspark-v9.tar
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/spark/spark-operator-2.5.0.tar

# 2. 安装 Spark Operator
helm install spark-op 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/spark/spark-operator/ \
  -n spark-operator --create-namespace

# 3. 创建 ServiceAccount
kubectl create serviceaccount spark -n default
kubectl create clusterrolebinding spark-edit --clusterrole=edit --serviceaccount=default:spark

# 4. 提交分析作业
kubectl apply -f spark/sparkapplication.yaml
kubectl get pods -n default -w  # 观察 Driver 和 Executor Pod
```

#### A-1 数据清洗（10分）→ `spark/douban_cleaning.py`
- 加载豆瓣电影 CSV 数据（OBS `s3a://` 路径）
- 打印 Schema → 统计缺失值 → dropna/fillna 处理 → 输出清洗前后统计对比

#### A-2 Spark SQL 统计分析（15分）→ `spark/douban_cleaning.py`
- Query 1: GROUP BY 按电影类型统计数量和平均评分
- Query 2: ORDER BY Top-N 最高评分电影
- Query 3: 时间维度趋势（按年份统计平均评分）
- Query 4: 窗口函数 ROW_NUMBER() 每类型最高评分电影

#### A-3 性能对比与 Amdahl 分析（5分）→ `performance_cmp/`
- Pandas（单机）vs PySpark（1/2 Executor）对比
- 对比柱状图 + Amdahl 定律分析
- 文件：`pandas_test.py`, `pyspark_test.py`, `plot_results.py`

### 4.2 方向 C：PyTorch DDP 分布式训练（已完成 ✅）

#### C-0 环境部署（10分）
```bash
# 1. 构建 PyTorch 镜像
docker build -t pytorch-mnist:latest ml/

# 2. 推送镜像到 SWR
docker tag pytorch-mnist:latest swr.cn-east-3.myhuaweicloud.com/cloudcourse/pytorch-mnist:latest
docker push swr.cn-east-3.myhuaweicloud.com/cloudcourse/pytorch-mnist:latest

# 3. 创建 ConfigMap（训练代码）
kubectl create configmap ml-code --from-file=ml/train_mnist_ddp.py -n default

# 4. 提交 PyTorchJob
kubectl apply -f ml/pytorchjob.yaml
kubectl get pods -n default -w  # 观察 Master + Worker Pod
```

#### C-1 CNN 模型实现（10分）→ `ml/train_mnist_ddp.py`
- 自定义 CNN 网络（Conv2d → ReLU → MaxPool → Dropout → FC）
- 支持单 GPU 训练 和 DDP 分布式训练两种模式
- 使用 MNIST 手写数字数据集

#### C-2 DDP 分布式训练（10分）→ `ml/train_mnist_ddp.py`
- 使用 PyTorch DistributedDataParallel (DDP)
- DistributedSampler 数据分区 + gloo 后端通信
- 1 Master + 2 Workers 共 3 进程并行训练
- 日志输出每个 Epoch 的 Loss 和时间

#### C-3 性能对比分析（5分）→ `ml/train_mnist_ddp.py`
- 单 GPU vs DDP (3 进程) 训练时间对比
- 分析加速比和并行效率

---

## 五、常用命令速查

### 查看集群状态
```bash
kubectl get nodes -o wide          # 节点状态
kubectl get pods -n default        # 所有 Pod
kubectl get svc -n default         # 所有 Service（含 ELB 公网 IP）
kubectl get pvc -n default         # PVC 状态
kubectl get hpa -n default         # HPA 状态
kubectl describe deployment backend -n default | grep "Image:"   # 查看镜像版本
```

### 监控相关
```bash
# Grafana（admin / admin123456）
kubectl port-forward svc/monitoring-grafana 3000:80 -n default
# 浏览器打开 http://localhost:3000

# Prometheus
kubectl port-forward svc/monitoring-prometheus 9090:9090 -n default
# 浏览器打开 http://localhost:9090
```

### 镜像更新（CI/CD 推送新镜像后）
```bash
kubectl rollout restart deployment/backend -n default
kubectl rollout restart deployment/frontend -n default
```

---

## 六、关键账号信息

| 项目 | 信息 |
|------|------|
| 华为云 Region | `cn-east-3` |
| SWR 镜像仓库 | `swr.cn-east-3.myhuaweicloud.com/cloudcourse/` |
| SWR 组织 | `cloudcourse` |
| Grafana 账号 | `admin` / `admin123456` |
| Redis 密码 | `redis123` |
| GitHub 仓库 | 找第一部分同学获取 |
| 华为云账号密码 | 找第一部分同学获取 |

---

## 七、技术栈

| 层级 | 技术 |
|------|------|
| 容器化 | Docker, Docker Compose |
| 编排 | Kubernetes（华为云 CCE） |
| 后端 | Python 3.11, Flask 3.0, Redis 7 |
| 前端 | Nginx 1.25, HTML/CSS |
| 大数据 | Apache Spark (PySpark), Spark Operator |
| 高性能计算 | MPI (mpi4py), MPI Operator (Kubeflow) |
| 分布式训练 | PyTorch DDP, PyTorchJob (Kubeflow Training Operator) |
| 监控 | Prometheus, Grafana, Alertmanager |
| 存储 | 华为云 EVS (CSI Disk), PVC |
| 网络 | 华为云 ELB (LoadBalancer) |
| CI/CD | GitHub Actions, 华为云 SWR |
| 自动扩缩 | HPA v2 (CPU) |

---

## 八、注意事项

1. **同一华为云账号**：第二部分同学不需要重新创建 CCE 集群、SWR 仓库、ELB，环境已就绪
2. **离线资源**：Spark/MPI/Monitoring 大镜像在 `离线包/` 目录中，需先 `docker load` 导入
3. **CCE 节点资源**：共 2 个 Worker 节点，如资源不足可临时扩容 1 个节点
4. **实验结束后**：释放额外创建的 ECS 节点和 ELB，避免代金券超额
5. **Git 协作**：第二部分同学修改代码后同样 push 到 main 分支，CI/CD 会自动构建推送新镜像
