import { create } from "@bufbuild/protobuf";
import { createClient, type MessageCodec, type Transport } from "@trevrpc/trevrpc-js";

import {
  Codecs,
  ScalarsSchema,
  SubsetSchema,
  type Scalars,
} from "../../build/test/gen/codec_pb.js";
import { CodecsCodecs, ScalarsCodec } from "../../build/test/gen/codec_trevrpc.js";

declare const transport: Transport;

ScalarsCodec satisfies MessageCodec<typeof ScalarsSchema>;
CodecsCodecs satisfies Iterable<MessageCodec>;

const client = createClient(Codecs, transport, { codecs: CodecsCodecs });
(await client.echo({ i32: 1 })) satisfies Scalars;
createClient(Codecs, transport, { codecs: [ScalarsCodec] });
// @ts-expect-error codecs are message codecs
createClient(Codecs, transport, { codecs: [ScalarsSchema] });

const bytes: Uint8Array<ArrayBuffer> = ScalarsCodec.encode(create(ScalarsSchema, { s: "x" }));
ScalarsCodec.decode(bytes) satisfies Scalars;
// @ts-expect-error a codec encodes messages of its own type
ScalarsCodec.encode(create(SubsetSchema));
