import type { Root, Type } from "protobufjs";

import type { RpcMethodKind, RpcServiceDescriptor } from "./index.js";
import type { NodeServer, NodeServerCall } from "./node.js";

export type { StreamingServerResponse, UnaryServerResponse } from "./index.js";
export type { NodeServer, NodeServerCall } from "./node.js";

/** Registers typed generated handlers on a Node server. */
export function registerTypedService(
  server: NodeServer,
  root: Root,
  service: RpcServiceDescriptor,
  handlers: Record<string, unknown>,
): NodeServer;

/** Dispatches one typed generated Node handler. */
export function dispatchTypedNodeHandler(
  call: NodeServerCall,
  handler: (...args: unknown[]) => unknown,
  kind: RpcMethodKind,
  requestType: Type,
  responseType: Type,
): Promise<void>;
