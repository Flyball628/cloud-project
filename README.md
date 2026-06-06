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
│   Spark Operator (PySpark) / MPI Operator (mpi4py)        │
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
│   ├── sparkapplication.yaml        # Spark 作业模板
│   └── wordcount.py                 # PySpark WordCount 示例
├── mpi/
│   ├── mpijob.yaml                  # MPI 作业模板
│   └── pi_mpi.py                    # 蒙特卡洛求 π 示例
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

## 四、第二部分：并行编程实战（待完成 🔲，40分）

> **说明**：第二部分基于第一部分已有的 CCE 集群、SWR 仓库、GitHub 仓库继续开发。选择一个方向（Spark 或 MPI），完成对应任务。

### 当前华为云环境（已就绪，无需重新创建）

| 资源 | 状态 | 关键信息 |
|------|------|---------|
| CCE 集群 | ✅ 运行中 | 2 个 Worker 节点，Region: `cn-east-3` |
| SWR 镜像仓库 | ✅ 已创建 | 组织: `cloudcourse`，已有 `backend`/`frontend` 仓库 |
| ELB 负载均衡 | ✅ 已绑定 | Backend 和 Frontend 的 Service 已绑定公网 IP |
| Redis | ✅ 运行中 | PVC 持久化，密码: `redis123` |
| Prometheus + Grafana | ✅ 运行中 | Grafana: `admin` / `admin123456` |
| GitHub Actions CI/CD | ✅ 已配置 | Push 到 main 自动构建推送镜像 |

### 4.1 第二部分同学环境接入指南

由于共用一个华为云账号，第二部分同学**无需**重新创建集群、SWR 仓库等。只需完成以下接入步骤：

#### Step 1：拉取代码仓库
```bash
git clone <GitHub 仓库地址>
cd cloud-project
```

#### Step 2：配置 kubectl 连接 CCE 集群
1. 登录 [华为云控制台](https://console.huaweicloud.com)（账号密码找第一部分同学获取）
2. 进入 **CCE（云容器引擎）** → 点击已有集群名称
3. 点击 **kubectl** 标签页 → 下载 kubeconfig 文件
4. 放置配置文件：
   - Linux/Mac: `~/.kube/config`
   - Windows: `%USERPROFILE%\.kube\config`
   - 或 WSL 中: `~/.kube/config`
5. 验证连接：
   ```bash
   kubectl get nodes
   # 应看到 2 个 Ready 的 Worker 节点
   kubectl get pods -n default
   # 应看到 backend、frontend、redis、monitoring-* 等 Pod 都在 Running
   ```

#### Step 3：配置 Docker 登录 SWR（如需本地构建镜像）
```bash
# 获取登录密码：华为云控制台 → SWR → 总览 → 生成登录指令
docker login -u cn-east-3@<AK> -p <密码> swr.cn-east-3.myhuaweicloud.com
```

#### Step 4：配置 GitHub（如需使用 CI/CD）
- GitHub Secrets 已在第一部分配置好，无需重复配置
- 如果换了新的 GitHub 仓库，需要重新配置 Secrets：
  - 进入仓库 Settings → Secrets → Actions → New secret
  - `SWR_USERNAME`：`cn-east-3@HST3WK7PLHQZ4WCV04FS`
  - `SWR_PASSWORD`：找第一部分同学获取

---

### 4.2 方向 A：Spark 大数据分析（选做）

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

# 4. 修改 spark/sparkapplication.yaml 中的镜像地址（替换为教师提供的 SWR PySpark 镜像）
# 5. 提交 WordCount 作业验证
kubectl apply -f spark/sparkapplication.yaml
kubectl get pods -n default -w  # 观察 Driver 和 Executor Pod
```

**验收**：Driver Pod 状态 Completed，`kubectl logs <driver-pod>` 输出 Top 10 单词。

#### A-1 数据清洗（10分）
- 数据集路径见课程群公告（OBS `s3a://` 地址）
- 加载数据 → 打印 Schema → 统计缺失值 → 2 种处理策略（dropna/fillna）→ 输出清洗前后对比

#### A-2 Spark SQL 统计分析（15分）
- 至少 4 个查询：GROUP BY 聚合、ORDER BY Top-N、时间维度趋势、JOIN 或窗口函数
- 每个查询附截图 + ≥50 字分析

#### A-3 性能对比与 Amdahl 分析（5分）
- Pandas（单机）vs PySpark（1/2 Executor）对比
- 绘制对比图 + Amdahl 定律分析

---

### 4.3 方向 B：MPI 并行科学计算（选做）

#### B-0 环境部署（10分）
```bash
# 1. 导入离线镜像
docker load -i 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/mpi/mpi4py-latest.tar

# 2. 部署 MPI Operator
kubectl apply -f 云计算课程设计_离线资源包_SparkOperator+MPI+Monitoring/离线包/mpi/mpi-operator.yaml

# 3. 修改 mpi/mpijob.yaml 中的镜像地址（替换为教师提供的 SWR mpi4py 镜像）
# 4. 提交 π 估算作业验证
kubectl apply -f mpi/mpijob.yaml
kubectl logs -f <launcher-pod>  # 查看 π 估算结果
```

**验收**：Launcher Pod 日志输出 `π ≈ 3.1415xx`。

#### B-1 并行算法实现（10分）
三选一：并行矩阵乘法 / 数值积分（梯形法） / 并行排序（奇偶换序）
- 实现串行版 + MPI 并行版，结果一致
- 通信原语加注释，附通信模式示意图

#### B-2 性能测试与 Amdahl 分析（15分）
- 1/2/4 进程各运行 3 次取平均
- 填写时间表格 + 绘制实测 vs Amdahl 理论加速比双折线图
- 分析差距原因

#### B-3 非阻塞通信优化（5分）
- 将一处关键通信改为 `Isend`/`Irecv`
- 对比阻塞版 vs 非阻塞版执行时间

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
