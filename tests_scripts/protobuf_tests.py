import os
import grpc_tools.protoc as protoc

protoc.main([
    "user.proto",
    "--proto_path=/tmp/pipeline/protobuf/",
    "--python_out=/tmp/pipeline/protobuf/",
    "user.proto",
])

try:
    import os
    import sys
    # sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append("/tmp/pipeline/protobuf")

    # import protobuf.user_pb2 as user_pb2
    import user_pb2
    print(user_pb2)
except Exception as error:
    raise error
