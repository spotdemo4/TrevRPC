"""TrevRPC for Python: a client for TrevRPC services over native QUIC and QMux over TLS.

Generate a client for your service with `protoc-gen-py` (protobuf-py's messages) and
`protoc-gen-trevrpc-py`, then call it on a `Channel`::

    import trevrpc
    from gen.greeter_trevrpc import GreeterClient
    from gen.greeter_pb import HelloRequest

    async with trevrpc.Channel("example.com:443") as channel:
        greeter = GreeterClient(channel)
        reply = await greeter.say_hello(HelloRequest(name="Ada"), timeout=5)
"""

from ._call import (
    BidiStreamingCall,
    CallOptions,
    ClientStreamingCall,
    ResponseStream,
    Status,
)
from ._channel import Channel, ChannelOptions, Transport, TransportPolicy, connect
from ._compression import Compression, Dictionary
from ._frames import DEFAULT_MAX_FRAME_SIZE
from ._metadata import Metadata, MetadataInput
from ._method import Method, MethodCompression, MethodKind
from ._qmux import QMUX_ALPN
from ._quic import ALPN
from ._status import Code, StatusDetail, TrevRpcError

__all__ = [
    "ALPN",
    "DEFAULT_MAX_FRAME_SIZE",
    "QMUX_ALPN",
    "BidiStreamingCall",
    "CallOptions",
    "Channel",
    "ChannelOptions",
    "ClientStreamingCall",
    "Code",
    "Compression",
    "Dictionary",
    "Metadata",
    "MetadataInput",
    "Method",
    "MethodCompression",
    "MethodKind",
    "ResponseStream",
    "Status",
    "StatusDetail",
    "Transport",
    "TransportPolicy",
    "TrevRpcError",
    "connect",
]
