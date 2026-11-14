// Limits shared by the framing layer and the transports, in a module of their own so that a
// transport can read them without importing the framing codecs.

/** The default largest frame, header excluded, that a call sends or accepts. */
export const DefaultMaxFrameSize = 4 * 1024 * 1024;
