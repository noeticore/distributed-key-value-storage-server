from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from storage.main import Cache

def test_cache_basic_operations():
    c = Cache(maxnum=3)

    # 添加并获取
    c.add("a", "apple")
    val, ok = c.get("a")
    assert ok
    assert val == "apple"

    # 添加多个键
    c.add("b", "banana")
    c.add("c", "cherry")
    val, ok = c.get("b")
    assert ok and val == "banana"
    val, ok = c.get("c")
    assert ok and val == "cherry"

    # 更新已有 key
    c.add("a", "apricot")
    val, ok = c.get("a")
    assert ok and val == "apricot"

    # 超出 maxnum，淘汰最老的 key
    c.add("d", "date")
    # 根据 age 计数，b 或 c 会被淘汰（最老）
    keys = list(c.m.keys())
    assert "d" in keys
    assert len(keys) <= 3

    # 删除 key
    c.del_key("c")
    val, ok = c.get("c")
    assert not ok

def test_heartbeat_handling(storage_server):
    storage_stub, _, _, _ = storage_server
    # 发送心跳
    response = storage_stub.live(stpb.StEmpty())
    assert response.errno

def test_base_operations(storage_server):
    storage_stub, _, _, _ = storage_server
    # 测试基本的 PUT, GET, DEL 操作
    key = "testkey"
    value = "testvalue"

    # PUT
    put_resp = storage_stub.putdata(stpb.StKV(cli_id=0, key=key, value=value))
    assert put_resp.errno

    # GET
    get_resp = storage_stub.getdata(stpb.StRequest(cli_id=0, key=key))
    assert get_resp.errno
    assert get_resp.value == value

    # DEL
    del_resp = storage_stub.deldata(stpb.StRequest(cli_id=0, key=key))
    assert del_resp.errno

    # GET after DEL
    get_resp_after_del = storage_stub.getdata(stpb.StRequest(cli_id=0, key=key))
    assert not get_resp_after_del.errno and get_resp_after_del.errmes == "未找到键值"