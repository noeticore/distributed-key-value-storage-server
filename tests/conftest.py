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

    yield manager_stub, manage_service, f"localhost{port}"

    try:
        channel.close()
        server.stop(None).wait()
        for handler in logger.handlers[:]:
            handler.close()           
            logger.removeHandler(handler)
        import shutil
        shutil.rmtree(datapath)
    except Exception:
        pass
    




@pytest.fixture(scope="function")
def storage_server(manager_server):
    manager_stub, _, manager_addr = manager_server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    port = ":"+str(server.add_insecure_port("localhost:0"))
    
    info = manager_stub.online(mapb.SerRequest(ip="localhost", port=str(port)))
    sid = info.server_id

    datapath = "tests/storage/"
    os.makedirs(f"{datapath}{sid}/", exist_ok=True)
    logger = logging.getLogger("store")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"{datapath}{sid}/storage.log", mode='w', encoding='utf-8')
    fh.setFormatter(logging.Formatter(f"[%(levelname)s] - %(message)s"))
    logger.addHandler(fh)

    storage_service = StoreService(server_id=sid, datapath=f"{datapath}{sid}/", logger=logger, cache_num=5, manager_addr=manager_addr)
    stpb_grpc.add_storagementServiceServicer_to_server(storage_service, server)
    server.start()

    store_channel = grpc.insecure_channel(f"localhost{port}")
    store_stub = stpb_grpc.storagementServiceStub(store_channel)


    yield store_stub, storage_service, sid, f"localhost{port}"
    try:
        manager_stub.offline(mapb.SerInfo(server_id=sid))
        store_channel.close()
        server.stop(None).wait()
        for handler in logger.handlers[:]:
            handler.close()           
            logger.removeHandler(handler)
        import shutil
        shutil.rmtree(datapath)
    except Exception as e:
        print(e, flush=True)