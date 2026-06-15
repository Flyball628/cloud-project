import os
import time
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from torchvision import datasets, transforms

class CNN(nn.Module):
    def __init__(self):
        super(CNN, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = nn.functional.relu(x)
        x = self.conv2(x)
        x = nn.functional.relu(x)
        x = nn.functional.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = nn.functional.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        return nn.functional.log_softmax(x, dim=1)


def get_data_loader(batch_size=64, is_distributed=False, rank=0, world_size=1):
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    
    # 直接使用本地数据，不下载（因为数据已经在集群中）
    dataset = datasets.MNIST('./data', train=True, download=False, transform=transform)

    if is_distributed:
        sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank, shuffle=True)
        shuffle = False
    else:
        sampler = None
        shuffle = True

    dataloader = DataLoader(dataset, batch_size=batch_size, sampler=sampler, shuffle=shuffle)
    return dataloader, sampler


def train_single_gpu(epochs=5, batch_size=64, lr=0.01):
    print("=" * 60)
    print("Single GPU Training")
    print("=" * 60)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    model = CNN().to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr)
    dataloader, _ = get_data_loader(batch_size, is_distributed=False)

    total_start = time.time()

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        epoch_start = time.time()

        for batch_idx, (data, target) in enumerate(dataloader):
            data, target = data.to(device), target.to(device)
            optimizer.zero_grad()
            output = model(data)
            loss = nn.functional.nll_loss(output, target)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()

        epoch_time = time.time() - epoch_start
        avg_loss = epoch_loss / len(dataloader)
        print(f"Epoch {epoch + 1}/{epochs} | Loss: {avg_loss:.4f} | Time: {epoch_time:.2f}s")

    total_time = time.time() - total_start
    print(f"\nTotal Training Time: {total_time:.2f}s")
    return total_time


def train_ddp(epochs=5, batch_size=64, lr=0.01):
    dist.init_process_group(backend="gloo")

    rank = dist.get_rank()
    world_size = dist.get_world_size()

    if rank == 0:
        print("=" * 60)
        print(f"Distributed Training (DDP) - {world_size} Workers")
        print("=" * 60)

    device = torch.device("cpu")
    if rank == 0:
        print(f"Device: {device}, Rank: {rank}/{world_size}")

    model = CNN().to(device)
    model = DDP(model)
    optimizer = optim.Adam(model.parameters(), lr=lr)

    dataloader, sampler = get_data_loader(batch_size, is_distributed=True, rank=rank, world_size=world_size)

    total_start = time.time()

    for epoch in range(epochs):
        model.train()
        sampler.set_epoch(epoch)
        epoch_loss = 0.0
        epoch_start = time.time()

        for batch_idx, (data, target) in enumerate(dataloader):
            data, target = data.to(device), target.to(device)
            optimizer.zero_grad()
            output = model(data)
            loss = nn.functional.nll_loss(output, target)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()

        if rank == 0:
            epoch_time = time.time() - epoch_start
            avg_loss = epoch_loss / len(dataloader)
            print(f"Epoch {epoch + 1}/{epochs} | Loss: {avg_loss:.4f} | Time: {epoch_time:.2f}s")

    total_time = time.time() - total_start

    if rank == 0:
        print(f"\nTotal Training Time: {total_time:.2f}s")

    dist.destroy_process_group()
    return total_time


def main():
    parser = argparse.ArgumentParser(description="MNIST CNN Training")
    parser.add_argument("--mode", type=str, default="single", choices=["single", "ddp"])
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=0.01)
    args = parser.parse_args()

    if args.mode == "single":
        train_single_gpu(epochs=args.epochs, batch_size=args.batch_size, lr=args.lr)
    elif args.mode == "ddp":
        train_ddp(epochs=args.epochs, batch_size=args.batch_size, lr=args.lr)


if __name__ == "__main__":
    main()
