import logging
import grpc

from concurrent import futures
from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from server.main import ManageService
from storage.main import StoreService

def _start_storage(manager_stub):
    fakelogger = logging.getLogger("storage")
    fakelogger.handlers.clear()   # 移除所有 handler    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    port = server.add_insecure_port("localhost:0")
    info = manager_stub.online(mapb.SerRequest(ip="localhost", port=str(port)))
    sid = info.server_id
    storage_service = StoreService(server_id=sid, datapath="tests/storage/", logger=fakelogger, cache_num=5, manager_addr=f"localhost:{port}")
    stpb_grpc.add_storagementServiceServicer_to_server(storage_service, server)
    
    server.start()
    

def test_node_register_and_unregister():
    logger = logging.getLogger("manage")
    logger.handlers.clear()   # 移除所有 handler
    service = ManageService(logger, interval_seconds=1)
    dummy_storage_info = mapb.SerRequest(ip="localhost", port="50051")
    server_info = service.online(dummy_storage_info, None)
    sid = server_info.server_id
    assert sid in service.servermap
    service.offline(mapb.SerInfo(server_id = sid),None)
    assert sid not in service.servermap
