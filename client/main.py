# client.py — synchronous Python client converted from your Go client
# Requires generated protobuf modules in `protos/` and a params.py in `params/`.

import grpc
import signal
import sys
import time

from protos import mapb_pb2 as mapb
from protos import mapb_pb2_grpc as mapb_grpc
from protos import stpb_pb2 as stpb
from protos import stpb_pb2_grpc as stpb_grpc
from params import params


def reconnect(client_id: int):
    """Try to get a new storage server from the management service and return a new channel.
    Retries up to 10 times like the Go version."""
    for i in range(10):
        try:
            manage_target = params.MANAGER_IP
            ch = grpc.insecure_channel(manage_target)
            mac = mapb_grpc.manageServiceStub(ch)
            resp = mac.changeServerRandom(mapb.CliId(cli_id=client_id))
            ch.close()
            if getattr(resp, 'errno', getattr(resp, 'Errno', False)):
                api = getattr(resp, 'api', getattr(resp, 'Api', None))
                if not api:
                    continue
                # api expected to be like 'host:port'
                return grpc.insecure_channel(api)
        except Exception:
            # ignore and retry
            time.sleep(0.2)
            continue
    raise RuntimeError("无法连接至服务器")


def main():
    # connect to management server and register as a client
    manage_target = params.MANAGER_IP + params.MANAGER_PORT
    manage_ch = grpc.insecure_channel(manage_target)
    mac = mapb_grpc.manageServiceStub(manage_ch)
    try:
        info = mac.connect(mapb.Empty())
    except Exception as e:
        print('连接管理服务器时发生错误:', e)
        return
    finally:
        manage_ch.close()

    if not info.errno:
        print(info.errmes)
        return

    client_id = info.cli_id
    print(f"已连接至管理服务器, 客户端ID为 {client_id}")
    ip = info.ip
    port = info.port

    storage_target = ip + port
    print(f"连接至存储服务器 {storage_target}")
    storage_ch = grpc.insecure_channel(storage_target)
    st = stpb_grpc.storagementServiceStub(storage_ch)

    def handle_sig(signum, frame):
        print('接收到中断信号，正在退出...')
        try:
            # notify manage server that this client disconnects
            manage_ch2 = grpc.insecure_channel(manage_target)
            mac2 = mapb_grpc.manageServiceStub(manage_ch2)
            mac2.disconnect(mapb.CliId(cli_id=client_id))
            manage_ch2.close()
        except Exception:
            pass
        try:
            storage_ch.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    print("开始输入命令")
    while True:
        try:
            line = input('>>> ').strip()
        except EOFError:
            break
        if not line:
            continue
        args = line.split()
        cmd = args[0].upper()

        if cmd == 'EXIT':
            break
        if cmd == 'HELP':
            print('使用 get [key] 来获取key对应的键值')
            print('使用 put [key] [value] 来上传键值对')
            print('使用 del [key] 来删除key对应的键值')
            print('使用 change <api> 更改存储服务器, 不指定api时随机分配')
            print('使用 exit 结束运行')
            continue

        # helper to attempt RPC and reconnect on channel errors
        def call_with_reconnect(rpc_func, request):
            nonlocal storage_ch, st
            try:
                return rpc_func(request)
            except Exception as e:
                # try reconnect once
                try:
                    try:
                        storage_ch.close()
                    except Exception:
                        pass
                    storage_ch = reconnect(client_id)
                    st = stpb_grpc.storagementServiceStub(storage_ch)
                except Exception as re:
                    raise re
            # if still failed, raise
            try:
                return rpc_func(request)
            except Exception:
                raise RuntimeError('RPC failed after reconnect')

        try:
            if cmd == 'GET':
                if len(args) != 2:
                    print('不正确的参数个数')
                    continue
                key = args[1]
                resp = call_with_reconnect(lambda r: st.getdata(r), stpb.StRequest(cli_id=client_id, key=key))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print(resp.value)

            elif cmd == 'PUT':
                if len(args) != 3:
                    print('不正确的参数个数')
                    continue
                key, value = args[1], args[2]
                resp = call_with_reconnect(lambda r: st.putdata(r), stpb.StKV(cli_id=client_id, key=key, value=value))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print('上传成功')

            elif cmd == 'DEL':
                if len(args) != 2:
                    print('不正确的参数个数')
                    continue
                key = args[1]
                resp = call_with_reconnect(lambda r: st.deldata(r), stpb.StRequest(cli_id=client_id, key=key))
                if not resp.errno:
                    print(resp.errmes)
                else:
                    print('删除成功')

            elif cmd == 'CHANGE':
                if len(args) == 1:
                    # random change
                    manage_ch2 = grpc.insecure_channel(manage_target)
                    mac2 = mapb_grpc.manageServiceStub(manage_ch2)
                    resp = mac2.changeServerRandom(mapb.CliId(cli_id=client_id))
                    manage_ch2.close()
                    if not resp.errno:
                        print(resp.errmes)
                    else:
                        api = resp.api
                        storage_ch = grpc.insecure_channel(api)
                        st = stpb_grpc.storagementServiceStub(storage_ch)
                        print('切换成功')
                elif len(args) == 2:
                    api = args[1]
                    manage_ch2 = grpc.insecure_channel(manage_target)
                    mac2 = mapb_grpc.manageServiceStub(manage_ch2)
                    resp = mac2.changeServer(mapb.CliChange(cli_id=client_id, api=api))
                    manage_ch2.close()
                    if not resp.errno:
                        print(resp.errmes)
                    else:
                        storage_ch = grpc.insecure_channel(api)
                        st = stpb_grpc.storagementServiceStub(storage_ch)
                        print('切换成功')
                else:
                    print('不正确的参数个数')

            else:
                print('无效命令')

        except Exception as e:
            print('发生错误:', e)

    # exit: notify management server
    try:
        manage_ch3 = grpc.insecure_channel(manage_target)
        mac3 = mapb_grpc.manageServiceStub(manage_ch3)
        mac3.disconnect(mapb.CliId(cli_id=client_id))
        manage_ch3.close()
    except Exception:
        pass

    try:
        storage_ch.close()
    except Exception:
        pass

    print('结束')


if __name__ == '__main__':
    main()
