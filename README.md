# 云计算课程设计 - 云原生微服务应用

> **组员**：倪浩程 (2023112684)、税宏鸣 (2023114708)

---

## 项目概述

本项目是一个完整的**云原生微服务应用**，部署在**华为云 CCE (Cloud Container Engine)** Kubernetes 集群上。涵盖 Web 应用、大数据计算（Spark）、高性能计算（MPI）、Prometheus+Grafana 监控以及 GitHub Actions CI/CD 全链路。

---

## 项目架构

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
│   Spark Operator (PySpark WordCount)                      │
│   MPI Operator (mpi4py 蒙特卡洛求 π)                       │
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

## 目录结构

```
cloud-project/
├── .github/workflows/
│   └── ci-cd.yml                    # CI/CD 流水线配置
├── docker-compose.yml               # 本地开发 Docker Compose
├── backend/
│   ├── app.py                       # Flask 后端 (API 服务)
│   ├── Dockerfile                   # 后端镜像
│   └── requirements.txt             # Python 依赖
├── frontend/
│   ├── Dockerfile                   # 前端 Nginx 镜像
│   ├── nginx.conf                   # Nginx 反向代理配置
│   └── static/
│       └── index.html               # 前端页面
├── k8s/
│   ├── backend-deployment.yaml      # 后端 Deployment (2 副本)
│   ├── backend-service.yaml         # 后端 LoadBalancer Service
│   ├── frontend-deployment.yaml     # 前端 Deployment + Service
│   ├── configmap.yaml               # 后端环境变量
│   ├── secret.yaml                  # Redis 密码
│   ├── redis-deployment-with-pvc.yaml  # Redis + PVC 持久化
│   ├── redis-service.yaml           # Redis Service
│   ├── hpa.yaml                     # 水平自动扩缩容
│   ├── nginx-configmap.yaml         # Nginx ConfigMap
│   ├── pvc.yaml                     # 持久卷声明 (华为云 EVS)
│   ├── prometheus-deployment.yaml   # Prometheus 轻量部署
│   ├── prometheus-configmap.yaml    # Prometheus 抓取配置
│   └── grafana-deployment.yaml      # Grafana 轻量部署
├── spark/
│   ├── sparkapplication.yaml        # Spark 作业定义
│   └── wordcount.py                 # PySpark WordCount
├── mpi/
│   ├── mpijob.yaml                  # MPI 作业定义
│   └── pi_mpi.py                    # 蒙特卡洛求 π
└── 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/
    └── 离线包/                       # Spark/MPI/Monitoring 离线镜像
```

---

## 已完成的工作

### 1. 基础部署（必做）
- ✅ **Backend**：Flask API 服务，双副本部署，提供 `/api/ping` 和 `/api/hello` 接口
- ✅ **Frontend**：Nginx 静态页面 + 反向代理，展示学号姓名和技术栈
- ✅ **Redis**：带 PVC 持久化，Backend 通过 ConfigMap/Secret 连接 Redis
- ✅ **HPA**：基于 CPU 利用率的自动扩缩容（1-4 副本，阈值 60%）
- ✅ **ELB 对外暴露**：Frontend 和 Backend 均通过华为云 LoadBalancer 对外访问

### 2. 监控（附加题1）✅
- ✅ 使用 `kube-prometheus-stack` Helm Chart 部署完整监控栈
- ✅ Prometheus 采集集群指标（Node Exporter、kube-state-metrics）
- ✅ Grafana 可视化（admin / admin123456）
- ✅ **自定义 Dashboard "CCE Monitoring"**：
  - CPU 使用率折线图（`node_cpu_seconds_total`）
  - Pod 内存使用柱状图（`container_memory_usage_bytes`）
- ✅ Alertmanager 告警管理

### 3. CI/CD（附加题2）✅
- ✅ GitHub Actions 自动流水线，Push 到 main 分支自动触发
- ✅ 自动构建 Backend 和 Frontend Docker 镜像
- ✅ 自动推送到华为云 SWR 镜像仓库（`swr.cn-east-3.myhuaweicloud.com/cloudcourse/`）
- ✅ 镜像 Tag：`latest`、`v1`、`commit SHA`
- ✅ K8s Deployment 使用 CI/CD 构建的镜像（镜像更新验证通过）

### 4. 大数据计算（Spark）
- ✅ Spark Operator 部署
- ✅ PySpark WordCount 作业（`wordcount.py`）
- 离线资源包已准备好

### 5. 高性能计算（MPI）
- ✅ MPI Operator 部署
- ✅ 蒙特卡洛方法估算圆周率（`pi_mpi.py`，2 Worker × 2 Slot = 4 进程并行）
- 离线资源包已准备好

---

## 部署指南

### 前提条件
- 华为云账号（已实名认证）
- 已创建 CCE 集群（Kubernetes）
- kubectl 已配置连接集群
- Helm 3 已安装

---

### 0. 华为云控制台操作（准备工作）

以下操作需要在 [华为云控制台](https://console.huaweicloud.com) 完成：

#### 0.1 创建 CCE 集群
1. 登录华为云控制台 → 搜索 **CCE（云容器引擎）**
2. 点击 **购买集群** → 选择 **标准集群**
3. 关键配置：
   - **集群版本**：v1.27 或 v1.29
   - **网络模型**：容器隧道网络（或 VPC 网络）
   - **节点规格**：至少 2 个节点，建议 4 vCPU / 8 GiB 内存
   - **弹性 IP**：节点绑定 EIP（用于拉取镜像）
4. 等待集群创建完成（约 5-10 分钟）

#### 0.2 获取 kubectl 配置
1. CCE 控制台 → 点击集群名称进入详情
2. 点击 **kubectl** 标签页
3. 下载 kubectl 配置文件（kubeconfig.json）
4. 将配置文件放到本地 `~/.kube/config`（Linux/Mac）或 `%USERPROFILE%\.kube\config`（Windows）
5. 验证连接：
   ```bash
   kubectl get nodes
   ```

#### 0.3 创建 SWR 镜像仓库
1. 华为云控制台 → 搜索 **SWR（容器镜像服务）**
2. 点击 **组织管理** → **创建组织**，名称：`cloudcourse`
3. 点击 **我的镜像** → **创建镜像仓库**：
   - 仓库名称：`backend`
   - 所属组织：`cloudcourse`
4. 再创建一个 `frontend` 仓库
5. 记录 SWR 登录信息（用于 CI/CD）：
   - 点击 **总览** → 获取 **登录指令**（包含用户名和密码）
   - 或者点击 **我的凭证** → 创建 **长期有效的登录指令**

#### 0.4 创建 ELB 负载均衡器
1. 华为云控制台 → 搜索 **ELB（弹性负载均衡）**
2. 点击 **购买弹性负载均衡** → 选择 **共享型**
3. 网络类型：公网（用于对外暴露服务）
4. 创建完成后，K8s LoadBalancer Service 会自动绑定此 ELB

#### 0.5 配置 CCE 节点安全组
1. 华为云控制台 → 搜索 **VPC（虚拟私有云）**
2. 找到 CCE 集群所在 VPC → 进入 **安全组**
3. 添加入方向规则，开放以下端口：
   - `80`（Frontend/Backend ELB 访问）
   - `30000-32767`（NodePort 范围，可选）
   - `9090`（Prometheus，可选）
   - `3000`（Grafana，可选）

---

### 1. 部署 K8s 基础资源
```bash
# 创建命名空间（如需要）
kubectl create ns default

# 部署 ConfigMap 和 Secret
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# 部署 PVC
kubectl apply -f k8s/pvc.yaml

# 部署 Redis
kubectl apply -f k8s/redis-deployment-with-pvc.yaml
kubectl apply -f k8s/redis-service.yaml

# 部署 Backend 和 Frontend
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/backend-service.yaml
kubectl apply -f k8s/nginx-configmap.yaml
kubectl apply -f k8s/frontend-deployment.yaml

# 部署 HPA
kubectl apply -f k8s/hpa.yaml
```

### 2. 部署监控（kube-prometheus-stack）
```bash
# 加载离线镜像
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/monitoring/monitoring-all.tar

# 安装 Helm Chart
helm install monitoring \
  云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/monitoring/kube-prometheus-stack-83.7.0.tgz \
  -f 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/monitoring/monitoring-values.yaml \
  --server-side=false --skip-crds
```

#### 2.1 创建 Grafana 自定义 Dashboard
1. **获取 Grafana 访问地址**：
   - 华为云 CCE 控制台 → **工作负载** → **无状态负载** → 找到 `monitoring-grafana`
   - 查看 ELB 公网 IP 和端口（默认 80）
2. **登录 Grafana**：浏览器打开 `http://<ELB-IP>`，用户名 `admin`，密码 `admin123456`
3. **创建 Dashboard**：
   - 左侧菜单 → **Dashboards** → **New** → **New Dashboard** → **Add visualization**
   - 数据源选择 `Prometheus`
4. **添加 CPU 折线图**：
   - Query: `100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) by (instance) * 100)`
   - 可视化类型：**Time series**
   - 单位：**Percent (0-100)**
   - Panel 标题：`CPU 使用率`
5. **添加内存柱状图**：
   - 点击 **Add** → **Visualization**
   - Query: `sum(container_memory_usage_bytes{namespace="default", container!=""}) by (pod)`
   - 可视化类型：**Bar chart**
   - 单位：**Bytes (IEC)**
   - Panel 标题：`Pod 内存使用`
6. 点击 **Save dashboard**，名称：`CCE Monitoring`

### 3. 部署 Spark Operator
```bash
# 导入镜像
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/spark/pyspark-v9.tar
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/spark/spark-operator-2.5.0.tar

# 安装 Spark Operator（Helm Chart 在离线包中）
# 然后提交作业
kubectl apply -f spark/sparkapplication.yaml
```

### 4. 部署 MPI Operator
```bash
# 导入镜像
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/mpi/mpi4py-latest.tar

# 部署 MPI Operator
kubectl apply -f 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/mpi/mpi-operator.yaml

# 提交 MPI 作业
kubectl apply -f mpi/mpijob.yaml
```

---

## CI/CD 配置说明

### 华为云 SWR 获取登录凭证
1. 华为云控制台 → **SWR（容器镜像服务）** → **总览**
2. 点击右上角 **生成登录指令**
3. 复制弹出的命令，从中提取用户名和密码：
   - 用户名格式：`cn-east-3@XXXXXXXXXX`
   - 密码：`docker login` 命令中 `-p` 后面的长字符串

### GitHub Secrets 配置
进入 GitHub 仓库 → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**：

| Secret 名称 | 值 |
|------------|------|
| `SWR_USERNAME` | 华为云 SWR 用户名（格式：`区域@AK`，例如 `cn-east-3@HST3WK7PLHQZ4WCV04FS`） |
| `SWR_PASSWORD` | 华为云 SWR 密码（`docker login` 中的密码） |

### 流水线流程
1. Push 代码到 `main` 分支
2. GitHub Actions 自动触发
3. 登录华为云 SWR
4. 构建 Backend Docker 镜像并推送
5. 构建 Frontend Docker 镜像并推送
6. 镜像 Tag：`latest`、`v1`、`commit SHA`
7. 手动执行 `kubectl rollout restart deployment/backend deployment/frontend -n default` 更新集群中的 Pod

---

## 验证命令

```bash
# 查看所有 Pod 状态
kubectl get pods -n default

# 查看 Service（获取 ELB 公网 IP）
kubectl get svc -n default

# 验证 CI/CD 镜像
kubectl describe deployment backend -n default | grep "Image:"
kubectl describe deployment frontend -n default | grep "Image:"

# 查看 HPA 状态
kubectl get hpa -n default

# 查看 Prometheus Targets
kubectl port-forward svc/monitoring-prometheus 9090:9090 -n default

# 查看 Grafana（admin / admin123456）
kubectl port-forward svc/monitoring-grafana 3000:80 -n default
```

### 华为云控制台验证
1. **CCE 控制台 → 工作负载 → 无状态负载**：确认所有 Deployment Pod 状态为 **运行中**
2. **CCE 控制台 → 工作负载 → 有状态负载**：确认 Redis 运行正常
3. **CCE 控制台 → 服务**：确认各 Service 已绑定 ELB 公网 IP
4. **ELB 控制台**：确认负载均衡器已绑定后端服务器，健康检查通过
5. **SWR 控制台 → 我的镜像**：确认 `backend` 和 `frontend` 镜像已推送成功
6. **浏览器访问 Frontend ELB IP**：确认页面正常显示（学号、姓名、技术栈）

---

## 技术栈

| 层级 | 技术 |
|------|------|
| 容器化 | Docker, Docker Compose |
| 编排 | Kubernetes (华为云 CCE) |
| 后端 | Python 3.11, Flask 3.0, Redis 7 |
| 前端 | Nginx 1.25, 静态 HTML/CSS |
| 大数据 | Apache Spark (PySpark), Spark Operator 2.5.0 |
| 高性能计算 | MPI (mpi4py), MPI Operator (Kubeflow) |
| 监控 | Prometheus, Grafana, Alertmanager |
| 存储 | 华为云 EVS (CSI Disk), PVC |
| 网络 | 华为云 ELB (LoadBalancer) |
| CI/CD | GitHub Actions, 华为云 SWR |
| 自动扩缩 | HPA v2 (CPU) |

---

## 注意事项

1. **镜像仓库**：所有镜像托管在华为云 SWR `swr.cn-east-3.myhuaweicloud.com/cloudcourse/`
2. **离线资源**：大型镜像（Spark、MPI、Monitoring）在离线资源包中，需先 `docker load` 导入
3. **监控 Grafana 密码**：`admin123456`
4. **Redis 密码**：`redis123`（存储在 K8s Secret 中）
5. **CI/CD**：仅覆盖 Backend 和 Frontend 镜像，Spark/MPI 镜像由离线包提供
6. **ELB 公网 IP**：部署后通过 `kubectl get svc` 获取
