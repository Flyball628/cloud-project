"""
pi_mpi.py - 蒙特卡洛方法估算 π，MPI 入门示例
"""
from mpi4py import MPI
import random

comm = MPI.COMM_WORLD
rank = comm.Get_rank()    # 当前进程编号
size = comm.Get_size()    # 总进程数

N = 10_000_000            # 每个进程各采样 1000 万次
local_count = 0
for _ in range(N):
    x, y = random.random(), random.random()
    if x*x + y*y <= 1.0:
        local_count += 1

# Reduce：将所有进程的 local_count 汇总到 rank=0
total = comm.reduce(local_count, op=MPI.SUM, root=0)

if rank == 0:
    pi = 4.0 * total / (N * size)
    print(f"[{size} processes] pi ≈ {pi:.6f}")
