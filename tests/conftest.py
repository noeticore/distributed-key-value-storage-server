import pytest
import grpc
import logging
import os

from concurrent import futures


from protos import mapb_pb2_grpc as mapb_grpc
from protos import mapb_pb2 as mapb
from protos import stpb_pb2_grpc as stpb_grpc
from protos import stpb_pb2 as stpb


from server.main import ManageService
from storage.main import StoreService


# ===========================
# In‑process Manager gRPC server
# ===========================
@pytest.fixture(scope="function")
def manager_server():
    datapath = "tests/manage/"
    os.makedirs(datapath, exist_ok=True)
    logger = logging.getLogger("manager")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"{datapath}manage.log", mode='w', encoding='utf-8')
    fh.setFormatter(logging.Formatter(f"[%(levelname)s] - %(message)s"))
    logger.addHandler(fh) 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    manage_service = ManageService(logger=logger)


    mapb_grpc.add_manageServiceServicer_to_server(manage_service, server)
    port = ":"+str(server.add_insecure_port("localhost:0"))
    server.start()


    channel = grpc.insecure_channel(f"localhost{port}")
    manager_stub = mapb_grpc.manageServiceStub(channel)

    # 返回 stub（用于 RPC）和管理服务对象（用于测试中查看内部）
    yield manager_stub, manage_service, f"localhost{port}"
    # teardown
    try:
        channel.close()
        server.stop(None).wait()
        for handler in logger.handlers[:]:
            handler.close()            # 关闭 handler，释放文件句柄
            logger.removeHandler(handler)
        import shutil
        shutil.rmtree(datapath)
    except Exception:
        pass
    




@pytest.fixture(scope="function")
def storage_server(manager_server):
    """
    启动单个 Storage Server 的 in-process gRPC 服务并注册到 manager。
    返回 (storage_stub, storage_service, sid, storage_api) 供测试使用。
    """
    datapath = "tests/storage/"
    os.makedirs(datapath, exist_ok=True)
    logger = logging.getLogger("store")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"{datapath}storage.log", mode='w', encoding='utf-8')
    fh.setFormatter(logging.Formatter(f"[%(levelname)s] - %(message)s"))
    logger.addHandler(fh)
    # unpack manager fixture
    manager_stub, _, manager_addr = manager_server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    port = ":"+str(server.add_insecure_port("localhost:0"))
    
    info = manager_stub.online(mapb.SerRequest(ip="localhost", port=str(port)))
    sid = info.server_id
    # 1) 先创建 storage 的 service（id 先占位为 0），并把它注册到 gRPC server
    
    os.makedirs(f"{datapath}", exist_ok=True)
    storage_service = StoreService(server_id=sid, datapath=datapath, logger=logger, cache_num=5, manager_addr=manager_addr)
    stpb_grpc.add_storagementServiceServicer_to_server(storage_service, server)
    server.start()

    store_channel = grpc.insecure_channel(f"localhost{port}")
    store_stub = stpb_grpc.storagementServiceStub(store_channel)

    # 返回 stub、service、sid、api，测试可以根据需要使用
    yield store_stub, storage_service
    #os.rmdir(datapath)
    # teardown：先让 manager 注销该 storage，再停止 server
    try:
        manager_stub.offline(mapb.SerInfo(server_id=sid))
        store_channel.close()
        server.stop(None).wait()
        for handler in logger.handlers[:]:
            handler.close()            # 关闭 handler，释放文件句柄
            logger.removeHandler(handler)
        import shutil
        shutil.rmtree(datapath)
    except Exception as e:
        print(e, flush=True)