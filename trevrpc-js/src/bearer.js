import { invalidArgument } from "./status.js";

// RFC 6750's b64token, as the servers' bearer token parsers accept it:
// 1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"=".
const b64Token = /^[A-Za-z0-9\-._~+/]+=*$/;

/**
 * Returns the request metadata that carries `token` as bearer credentials: the `authorization` key
 * with the value `Bearer <token>`. It accepts the tokens that the servers' bearer token parsers do,
 * the RFC 6750 `b64token` syntax, and throws an InvalidArgument TrevRpcError for any other, such as
 * an empty one or one with whitespace, which a server would reject as Unauthenticated, and which a
 * client could not tell from an expired session.
 */
export function bearerMetadata(token) {
  if (typeof token !== "string" || !b64Token.test(token)) {
    throw invalidArgument("bearer token is not a b64token");
  }
  return { authorization: `Bearer ${token}` };
}
