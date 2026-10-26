// Connection metadata is request metadata a client sets once for all its calls, such as a bearer
// token, so that it crosses each connection once instead of in every REQUEST. A call offers the
// set (RpcRequest.connection_metadata with entries) until a HEADERS confirms that the server
// stored it for the session, then refers to it by ID. Calls on different streams reach the server
// in any order, so a reference only follows a confirmation. The server merges the set into each
// call's metadata, the call's own entries winning, so the client just sends both; see the wiki's
// Connection Metadata section. This module holds the channel's set and what each session
// confirmed; call.js puts them on the REQUEST and learns the confirmation from the HEADERS.
import { normalizeMetadata } from "./metadata.js";

/** The property of a transport that holds its ConnectionMetadataSource. */
export const ConnectionMetadataKey = Symbol("trevrpc.connectionMetadata");

/** The connection metadata of a transport, or undefined without a source. */
export function transportConnectionMetadata(transport) {
  return transport[ConnectionMetadataKey];
}

/** A request that carries no connection metadata. */
const NoConnectionMetadata = Object.freeze({ field: undefined, offered: 0 });

/**
 * A channel's connection metadata, shared by its sessions: the current set, whose ID changes with
 * it, and the state of each session.
 */
export class ConnectionMetadataSource {
  /** The current set, `{ id, metadata }`, or undefined without connection metadata. */
  #current;
  #ids = 0;
  #sessions = new WeakMap();

  /** Throws InvalidArgument if `metadata` is invalid, like call metadata. */
  constructor(metadata) {
    this.set(metadata);
  }

  /** The current set, or undefined. */
  get current() {
    return this.#current;
  }

  /**
   * Replaces the connection metadata, with keys normalized like a call's; empty or missing
   * metadata removes it. The new set has a new ID, so each session offers it again.
   */
  set(metadata) {
    const normalized = metadata == null ? {} : normalizeMetadata(metadata);
    const keys = Object.keys(normalized);
    if (keys.length === 0) {
      this.#current = undefined;
      return;
    }
    // Copied, so a caller changing its bytes later does not change what is sent.
    for (const key of keys) {
      normalized[key] = normalized[key].slice();
    }
    this.#ids += 1;
    this.#current = { id: this.#ids, metadata: normalized };
  }

  /**
   * The state of one session, keyed like the compression state: what the server confirmed on the
   * connection that `key` identifies, dropped with it.
   */
  session(key) {
    let state = this.#sessions.get(key);
    if (state === undefined) {
      state = new SessionConnectionMetadata(this);
      this.#sessions.set(key, state);
    }
    return state;
  }
}

class SessionConnectionMetadata {
  #source;
  /** The ID of the set the server confirmed on this session, or zero. */
  #confirmed = 0;

  constructor(source) {
    this.#source = source;
  }

  /**
   * The connection metadata of one REQUEST as `{ field, offered }`: `field` is the value of
   * `connectionMetadata`, a reference to the current set once the session confirmed it and an
   * offer of it otherwise, or undefined without a set; `offered` is the ID the REQUEST offers,
   * which `learn` checks the HEADERS against, or zero.
   */
  prepare() {
    const set = this.#source.current;
    if (set === undefined) {
      return NoConnectionMetadata;
    }
    if (this.#confirmed === set.id) {
      return { field: { id: set.id }, offered: 0 };
    }
    return { field: { id: set.id, metadata: set.metadata }, offered: set.id };
  }

  /** Records that the server stored the `offered` set, once the call's HEADERS confirm it. */
  learn(offered, headers) {
    if (offered !== 0 && headers.connectionMetadata === offered) {
      this.#confirmed = offered;
    }
  }
}
