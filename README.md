# 分布式键值存储系统

该项目主要包含以下核心模块:

1. **管理服务器** — `python -m serve.main`
2. **存储服务器** — `python -m storege.main`
3. **客户端** — `python -m client.main`
   
该键值存储系统提供了基于本地磁盘记录键值的功能，并且拥有在内存进行缓存、锁管理、心跳检测以及多节点协作能力

---

## 🚀 1. 创建环境

该项目使用[uv](https://docs.astral.sh/uv/)作为包管理器管理项目环境

先创建一个虚拟环境:

```
uv venv --python 3.12 --seed
```

同步项目依赖:

```
uv sync
```


## 🧪 2. 激活虚拟环境

不同的操作系统对应的命令有所不同:

### macOS / Linux
```bash
source .venv/bin/activate
```

### Windows PowerShell
```powershell
.venv\Scripts\Activate.ps1
```

### Windows CMD
```cmd
.venv\Scripts\activate.bat
```

**VSCode** 能自动激活虚拟环境


## ▶️ 3. 运行项目 (多终端)

至少需要打开**三个终端**

### **终端 1: 管理节点**
```
python -m serve.main
```

### **终端 2: 存储节点**
```
python -m storege.main
```
存储节点可以打开多个，模仿分布式存储环境

### **终端 3: 客户端**
```
python -m client.main
```
客户端同样可以有多个，模仿多用户使用

客户端支持以下命令:
- `get key`
- `put key value`
- `del key`
- `change [api]`
- `exit`
- `help`
  
详细作用可在客户端输入`help`查看

---

## 📁 项目结构

```
project/
├─ server/
│   └─ main.py
├─ storege/
│   └─ main.py
├─ client/
│   └─ main.py
├─ protos/
│   ├─ mapb.proto
│   ├─ stpb.proto
│   ├─ mapb_pb2.py
│   ├─ mapb_pb2_grpc.py
│   ├─ stpb_pb2.py
│   └─ stpb_pb2_grpc.py
├─ params/
│   └─ params.py
├─ pyproject.toml
└─ README.md
```

---

## 🔧 grpc协议生成 (如果需要)

```
python -m grpc_tools.protoc -I protos --python_out=protos --grpc_python_out=protos protos/*.proto
```
注：因为自动生成的proto对应的python文件可能存在路径问题，需手动修改`import`路径
```
# mapb_pb2_grpc.py
#import mapb_pb2 as mapb__pb2(origin)
from protos import mapb_pb2 as mapb__pb2
```
```
# stpb_pb2_grpc.py
#import stpb_pb2 as stpb__pb2(origin)
from protos import stpb_pb2 as stpb__pb2
```
---

## 📝 其他
- 项目基于 Python 3.12
- gRPC 默认使用`insecure channels`
- *存储服务器*会创建类似 `storage_<serverId>/`的文件夹保存键值数据

