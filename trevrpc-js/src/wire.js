import protobuf from "protobufjs";

export { protobuf };

export const WireVersion = 1;

export const RpcKind = Object.freeze({
  Unary: 0,
  ClientStreaming: 1,
  ServerStreaming: 2,
  BidirectionalStreaming: 3,
});

export const RpcStreamFrameKind = Object.freeze({
  Message: 0,
  Status: 1,
});

export const wireRoot = protobuf.Root.fromJSON({
  nested: {
    trevrpc: {
      nested: {
        RpcKind: {
          values: {
            Unary: 0,
            ClientStreaming: 1,
            ServerStreaming: 2,
            BidirectionalStreaming: 3,
          },
        },
        RpcStreamFrameKind: {
          values: {
            Message: 0,
            Status: 1,
          },
        },
        RpcRequest: {
          fields: {
            service: { type: "string", id: 1 },
            method: { type: "string", id: 2 },
            body: { type: "bytes", id: 3 },
            metadata: { keyType: "string", type: "bytes", id: 4 },
            kind: { type: "RpcKind", id: 5 },
            version: { type: "uint32", id: 6 },
            deadlineUnixNanos: { type: "uint64", id: 7 },
          },
        },
        RpcResponse: {
          fields: {
            status: { type: "uint32", id: 1 },
            message: { type: "string", id: 2 },
            body: { type: "bytes", id: 3 },
            metadata: { keyType: "string", type: "bytes", id: 4 },
          },
        },
        RpcStreamFrame: {
          fields: {
            kind: { type: "RpcStreamFrameKind", id: 1 },
            status: { type: "uint32", id: 2 },
            message: { type: "string", id: 3 },
            body: { type: "bytes", id: 4 },
            metadata: { keyType: "string", type: "bytes", id: 5 },
          },
        },
      },
    },
  },
});

export const RpcRequest = wireRoot.lookupType("trevrpc.RpcRequest");
export const RpcResponse = wireRoot.lookupType("trevrpc.RpcResponse");
export const RpcStreamFrame = wireRoot.lookupType("trevrpc.RpcStreamFrame");

export function createRoot(json) {
  return protobuf.Root.fromJSON(json);
}

export function messageFrame(body) {
  return {
    kind: RpcStreamFrameKind.Message,
    status: 0,
    message: "",
    body,
    metadata: {},
  };
}
