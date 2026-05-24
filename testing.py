import torch
import time

print("CUDA Available:", torch.cuda.is_available())

device = torch.device("cuda")

x = torch.rand(8000, 8000).to(device)

start = time.time()
y = torch.matmul(x, x)

print("Time:", time.time() - start)