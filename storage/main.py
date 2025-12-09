import argparse
import logging
import os
import random
import signal
import sys
import time
from threading import Lock
from concurrent import futures
import grpc

from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from params import params


class Cache:
    def __init__(self, maxnum: int):
        self.maxnum = maxnum
        self.m = {}  # key -> value
        self.timemap = {}  # key -> age counter
        self.mu = Lock()

    def del_key(self, key: str):
        with self.mu:
            self.timemap.pop(key, None)
            self.m.pop(key, None)

    def add(self, key: str, value: str):
        with self.mu:
            # increase age for all keys
            for k in list(self.timemap.keys()):
                self.timemap[k] += 1
            if key in self.timemap:
                self.timemap[key] = 0
                self.m[key] = value
                return
            if len(self.m) < self.maxnum:
                self.m[key] = value
                self.timemap[key] = 0
                return
            # evict oldest
            maxt = -1
            maxk = None
            for k, t in self.timemap.items():
                if maxt == -1 or t > maxt:
                    maxt = t
                    maxk = k
            if maxk is not None:
                self.timemap.pop(maxk, None)
                self.m.pop(maxk, None)
            self.timemap[key] = 0
            self.m[key] = value

    def get(self, key: str):
        with self.mu:
            for k in list(self.timemap.keys()):
                self.timemap[k] += 1
            if key in self.m:
                return self.m[key], True
            return "", False


class RWLock:
    """A simple reader-writer lock with try-acquire for readers.
    Not fully featured but sufficient for this port.
    """

    def __init__(self):
        self._readers = 0
        self._rlock = Lock()  # protects readers count
        self._wlock = Lock()  # exclusive writer lock

    def acquire_read(self):
        # block until we can acquire read
        with self._rlock:
            self._readers += 1
            if self._readers == 1:
                # first reader acquires writer lock to block writers
                self._wlock.acquire()

    def release_read(self):
        with self._rlock:
            self._readers -= 1
            if self._readers == 0:
                self._wlock.release()

    def try_acquire_read(self) -> bool:
        # try to obtain read lock without blocking writers
        with self._rlock:
            if self._readers > 0:
                # already have readers, can acquire
                self._readers += 1
                return True
            acquired = self._wlock.acquire(blocking=False)
            if acquired:
                self._readers += 1
                return True
            else:
                # writer active; cannot acquire read
                return False

    def acquire_write(self):
        self._wlock.acquire()

    def release_write(self):
        self._wlock.release()


class StoreService(stpb_grpc.storagementServiceServicer):
    def __init__(self, server_id: int, datapath: str, logger: logging.Logger, cache_num: int, manager_addr: str):
        self.id = server_id
        self.mumap = {}  # key -> RWLock
        self.tmpvalue = None  # bytes
        self.logger = logger
        self.datapath = datapath
        self.KVmap = {}  # key -> bool
        self.cache = Cache(cache_num)
        self.manager = manager_addr

    # ----- RPC methods -----
    def getdata(self, request, context):
        cli_id = request.cli_id
        key = request.key
        self.logger.info(f"客户端{cli_id} 请求键值{key}")

        value, ok = self.cache.get(key)
        if ok:
            self.logger.info(f"缓存存在键值{key}")
            self.logger.info(f"返回键值{key}")
            return stpb.StResponse(value=value, errno=True)

        self.logger.info("缓存中未找到键值 %s" % key)
        if key in self.KVmap:
            lock = self.mumap.get(key)
            if not lock or not lock.try_acquire_read():
                # TryRLock equivalent: if cannot get read lock return busy
                self.logger.info(f"客户端{cli_id} 尝试获取 {key}共享锁, 但目前该锁被独占")
                return stpb.StResponse(errno=False, errmes="该值被另一进程占有")
            self.logger.info(f"客户端{cli_id} 获取了 {key}共享锁")
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'rb') as f:
                    content = f.read()
            except Exception as e:
                self.logger.info(f"读取键值{key} 时发生错误{e},客户端{cli_id} 释放 {key}共享锁")
                lock.release_read()
                return stpb.StResponse(errno=False, errmes=str(e))
            self.logger.info(f"成功读取键值{key}")
            self.logger.info(f"缓存记录键值{key}")
            self.cache.add(key, content.decode())
            self.logger.info(f"返回键值{key} ,客户端{cli_id} 释放 {key}共享锁")
            lock.release_read()
            return stpb.StResponse(value=content.decode(), errno=True)
        else:
            self.logger.info(f"无键值{key} ,向其他服务器请求")
            # contact manage server to get value from other storages
            try:
                with grpc.insecure_channel(self.manager) as ch:
                    client = mapb_grpc.manageServiceStub(ch)
                    resp = client.Get(mapb.Request(key=key, server_id=self.id))
            except Exception as e:
                self.logger.error(f"连接管理服务器错误 {e}")
                raise
            if not resp.errno:
                self.logger.info(f"无法从其他服务器取得键值{key} {resp.errmes},告知客户端{cli_id}")
                return stpb.StResponse(errno=False, errmes="未找到键值")
            self.logger.info(f"成功从其他服务器请求键值{key}")
            self.logger.info(f"缓存记录键值{key}")
            self.cache.add(key, resp.value)
            # create lock and write local file
            self.mumap[key] = RWLock()
            self.logger.info(f"准备写入键值{key}")
            self.logger.info(f"为客户端{cli_id} 申请 {key}独占锁")
            self.mumap[key].acquire_write()
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'wb') as f:
                    f.write(resp.value.encode())
                self.logger.info(f"写入键值{key} 成功")
            except Exception as e:
                self.logger.info(f"写入键值{key} 失败: {e}")
            finally:
                self.logger.info(f"返回键值{key} ,客户端{cli_id} 释放 {key}独占锁")
                self.mumap[key].release_write()
            return stpb.StResponse(value=resp.value, errno=True)

    def maGetdata(self, request, context):
        key = request.key
        self.logger.info(f"管理服务器 请求键值{key}")
        value, ok = self.cache.get(key)
        if ok:
            self.logger.info(f"缓存存在键值{key}")
            self.logger.info(f"返回键值{key}")
            return stpb.StResponse(value=value, errno=True)
        self.logger.info("缓存中未找到键值 %s" % key)
        if key in self.KVmap:
            lock = self.mumap.get(key)
            if not lock or not lock.try_acquire_read():
                self.logger.info(f"管理服务器尝试获取 {key}共享锁, 但目前该锁被独占")
                return stpb.StResponse(errno=False, errmes="无法获取锁")
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'rb') as f:
                    content = f.read()
            except Exception as e:
                self.logger.info(f"读取键值{key} 时发生错误{e},管理服务器释放 {key}共享锁")
                lock.release_read()
                return stpb.StResponse(errno=False, errmes=str(e))
            self.logger.info(f"成功读取键值{key} ,管理服务器释放 {key}共享锁")
            lock.release_read()
            return stpb.StResponse(value=content.decode(), errno=True)
        self.logger.info(f"无键值{key} ,告知管理服务器")
        return stpb.StResponse(errno=False, errmes="服务器中无键值")

    def maPutdata(self, request, context):
        key = request.key
        value = request.value
        self.cache.del_key(key)
        if key not in self.KVmap:
            self.KVmap[key] = True
            self.mumap[key] = RWLock()
            self.tmpvalue = None
            self.logger.info(f"管理服务器正在申请 {key}独占锁")
            self.mumap[key].acquire_write()
            self.logger.info(f"管理服务器获取了 {key}独占锁")
        else:
            self.logger.info(f"管理服务器正在申请 {key}独占锁")
            self.mumap[key].acquire_write()
            self.logger.info(f"管理服务器获取了 {key}独占锁")
            # record original
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'rb') as f:
                    content = f.read()
                self.logger.info(f"记录原有键值{key} 成功")
                self.tmpvalue = content
            except Exception:
                self.logger.info(f"记录原有键值{key} 失败")
                self.tmpvalue = None
            # release exclusive lock (in Go they RUnlock after reading; here we release and re-lock later)
            self.mumap[key].release_write()
        self.logger.info(f"准备写入键值{key}")
        try:
            with open(os.path.join(self.datapath, f"{key}"), 'wb') as f:
                f.write(value.encode())
        except Exception as e:
            self.logger.info(f"写入键值{key} 失败,告知管理服务器: {e}")
            return stpb.StEmpty(errno=False, errmes=str(e))
        self.logger.info(f"写入键值{key} 成功,告知管理服务器")
        self.logger.info("等待管理服务器告知本次写入结果...")
        return stpb.StEmpty(errno=True)

    def maDeldata(self, request, context):
        key = request.key
        self.cache.del_key(key)
        self.logger.info(f"准备删除键值{key}")
        if key not in self.KVmap:
            self.tmpvalue = None
            self.logger.info(f"管理服务器正在申请 {key}独占锁")
            self.mumap[key] = RWLock()
            self.mumap[key].acquire_write()
            self.logger.info(f"管理服务器获取了 {key}独占锁")
            return stpb.StEmpty(errno=True)
        else:
            self.logger.info(f"管理服务器正在申请 {key}独占锁")
            self.mumap[key].acquire_write()
            self.logger.info(f"管理服务器获取了 {key}独占锁")
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'rb') as f:
                    content = f.read()
                self.tmpvalue = content
                self.logger.info(f"记录原有键值{key} 成功")
            except Exception:
                self.tmpvalue = None
                self.logger.info(f"记录原有键值{key} 失败")
        # delete from KV map
        self.KVmap.pop(key, None)
        self.logger.info(f"删除键值{key} 成功,告知管理服务器")
        self.logger.info("等待管理服务器告知本次删除结果...")
        return stpb.StEmpty(errno=True)

    def putdata(self, request, context):
        cli_id = request.cli_id
        key = request.key
        value = request.value
        self.logger.info(f"客户端{cli_id} 正在申请提交键值{key}")
        try:
            with grpc.insecure_channel(self.manager) as ch:
                client = mapb_grpc.manageServiceStub(ch)
                resp = client.Put(mapb.KV(key=key, server_id=self.id, value=value))
        except Exception as e:
            self.logger.error(f"连接管理服务器错误 {e}")
            raise
        if not resp.errno:
            self.logger.info(f"向其他服务器提交键值{key} 时发生错误 {resp.errmes}")
            return stpb.StEmpty(errno=False, errmes="提交失败")
        return stpb.StEmpty(errno=True)

    def deldata(self, request, context):
        cli_id = request.cli_id
        key = request.key
        self.logger.info(f"客户端{cli_id} 正在申请删除键值{key}")
        try:
            with grpc.insecure_channel(self.manager) as ch:
                client = mapb_grpc.manageServiceStub(ch)
                resp = client.Del(mapb.Request(key=key, server_id=self.id))
        except Exception as e:
            self.logger.error(f"连接管理服务器错误 {e}")
            raise
        if not resp.errno:
            self.logger.info(f"向其他服务器删除键值{key} 时发生错误 {resp.errmes}")
            return stpb.StEmpty(errno=False, errmes="删除失败")
        return stpb.StEmpty(errno=True)

    def abort(self, request, context):
        key = request.key
        self.logger.info("抛弃本次结果")
        self.logger.info("准备恢复原有记录")
        if self.tmpvalue is not None:
            self.KVmap[key] = True
            try:
                with open(os.path.join(self.datapath, f"{key}"), 'wb') as f:
                    f.write(self.tmpvalue)
                self.logger.info(f"重写入键值{key} 成功")
                self.logger.info(f"{key}独占锁释放")
                self.mumap[key].release_write()
            except Exception:
                self.logger.error("恢复原有记录失败")
                self.logger.info(f"{key}独占锁释放")
                self.mumap[key].release_write()
            return stpb.StEmpty(errno=True)
        else:
            self.KVmap.pop(key, None)
            try:
                os.remove(os.path.join(self.datapath, f"{key}"))
            except Exception:
                self.logger.info(f"{key}删除失败")
            self.logger.info(f"{key}独占锁释放")
            self.mumap[key].release_write()
            self.mumap.pop(key, None)
        self.logger.info("恢复原有记录完成")
        return stpb.StEmpty(errno=True)

    def commit(self, request, context):
        key = request.key
        self.logger.info("提交本次结果")
        self.logger.info(f"{key}独占锁释放")
        # release exclusive lock
        if key in self.mumap:
            try:
                self.mumap[key].release_write()
            except Exception:
                pass
            if request.delete:
                try:
                    os.remove(os.path.join(self.datapath, f"{key}"))
                except Exception:
                    self.logger.info(f"{key}删除失败")
                self.mumap.pop(key, None)
        return stpb.StEmpty(errno=True)

    def live(self, request, context):
        self.logger.info("响应心跳请求,返回存活状态")
        return stpb.StEmpty(errno=True)
    
    def offline(self):
        try:
            with grpc.insecure_channel(self.manager) as ch:
                client = mapb_grpc.manageServiceStub(ch)
                client.offline(mapb.SerInfo(server_id=self.id))
            self.logger.info("注销完毕")
        except Exception as e:
            self.logger.error(f"发生错误{e},注销失败")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="localhost")
    parser.add_argument("--port", default=str(random.randint(20000, 65535)))
    parser.add_argument("--clear", action="store_true", help="结束是否清除数据")
    parser.add_argument("--cache", type=int, default=5)
    parser.add_argument("--savepath", type=str, default="storage/")
    args = parser.parse_args()

    ip = args.ip
    port = f":{args.port}" if not args.port.startswith(":") else args.port

    target = params.MANAGER_IP + params.MANAGER_PORT
    try:
        with grpc.insecure_channel(target) as ch:
            client = mapb_grpc.manageServiceStub(ch)
            info = client.online(mapb.SerRequest(ip=ip, port=port))
    except Exception as e:
        print(e)
        raise SystemExit("无法连接管理服务器")

    if not info.errno:
        print(info.errmes)

    server_id = info.server_id
    datapath = f"{args.savepath}/storage_{server_id}/"

    os.makedirs(f"{datapath}", exist_ok=True)
    logger = logging.getLogger("store")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"{datapath}storage.log", mode='w', encoding='utf-8')
    fh.setFormatter(logging.Formatter(f"[%(levelname)s] - %(message)s"))
    logger.addHandler(fh)

    service = StoreService(server_id, datapath, logger, args.cache, target)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    stpb_grpc.add_storagementServiceServicer_to_server(service, server)
    server.add_insecure_port(ip + port)

    def handle_sig(signum, frame):
        logger.info("接收到中断信号, 正在注销...")
        if args.clear:
            try:
                for handler in logger.handlers[:]:
                    handler.close()            # 关闭 handler，释放文件句柄
                    logger.removeHandler(handler)
                import shutil
                shutil.rmtree(datapath)
            except Exception:
                pass
        service.offline()
        server.stop(0)
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    logger.info("开始进行服务")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    main()