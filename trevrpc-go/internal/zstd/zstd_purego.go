//go:build !cgo || trevrpc_purego

package zstd

// The pure-Go codec: github.com/klauspost/compress/zstd. It is built without
// cgo, or with the trevrpc_purego tag in place of libzstd. klauspost has no
// magicless format, so the codec strips each frame's magic number after
// encoding and puts it back before decoding.

import (
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"slices"
	"sync"

	kzstd "github.com/klauspost/compress/zstd"
)

// libzstd reports that the codec is libzstd, whose output the golden vectors
// hold byte for byte.
const libzstd = false

// frameMagic is the Magic_Number that starts every zstd frame klauspost
// writes and reads, and that ZSTD_MESSAGE payloads leave out.
var frameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// Dict is a dictionary digested for both directions, or, from NewPlain, the
// absence of one. It is safe for concurrent use.
type Dict struct {
	ID          uint32
	Fingerprint [8]byte
	// data is the dictionary, nil for none.
	data  []byte
	level int
	enc   *kzstd.Encoder
	dec   *kzstd.Decoder
	// streamEncoders and streamDecoders pool the stream contexts that start
	// from this dictionary, since creating one digests it again.
	streamEncoders sync.Pool
	streamDecoders sync.Pool
}

// NewDict digests a zstd-format dictionary for compression at level and for
// decompression.
func NewDict(data []byte, level int) (*Dict, error) {
	id, err := DictID(data)
	if err != nil {
		return nil, err
	}
	data = slices.Clone(data)
	d := &Dict{ID: id, Fingerprint: Fingerprint(data), data: data, level: level}
	if err := d.init(); err != nil {
		return nil, err
	}
	return d, nil
}

// NewPlain returns the Dict of messages compressed at level without a
// dictionary: its frames carry no Dictionary_ID, and its ID is zero.
func NewPlain(level int) *Dict {
	d := &Dict{level: level}
	if err := d.init(); err != nil {
		panic(err)
	}
	return d
}

func (d *Dict) init() error {
	var err error
	opts := append(d.encoderOptions(),
		kzstd.WithSingleSegment(true), kzstd.WithZeroFrames(true),
		kzstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if d.enc, err = kzstd.NewWriter(nil, opts...); err != nil {
		return errors.New("zstd: cannot load dictionary")
	}
	dopts := []kzstd.DOption{kzstd.WithDecoderConcurrency(0)}
	if d.data != nil {
		dopts = append(dopts, kzstd.WithDecoderDicts(d.data))
	}
	if d.dec, err = kzstd.NewReader(nil, dopts...); err != nil {
		return errors.New("zstd: cannot load dictionary")
	}
	return nil
}

// encoderOptions are the options every encoder of d shares: frames without a
// checksum, at d's level, from d's dictionary.
func (d *Dict) encoderOptions() []kzstd.EOption {
	opts := []kzstd.EOption{
		kzstd.WithEncoderLevel(kzstd.EncoderLevelFromZstd(d.level)),
		kzstd.WithEncoderCRC(false),
	}
	if d.data != nil {
		opts = append(opts, kzstd.WithEncoderDict(d.data))
	}
	return opts
}

// Compress appends src, compressed with d into a ZSTD_MESSAGE payload, to dst.
func (d *Dict) Compress(dst, src []byte) ([]byte, error) {
	if len(src) == 0 && d.data != nil {
		// klauspost leaves the Dictionary_ID out of empty frames.
		return appendEmptyFrame(dst, d.ID), nil
	}
	n := len(dst)
	dst = d.enc.EncodeAll(src, dst)
	return append(dst[:n], dst[n+len(frameMagic):]...), nil
}

// appendEmptyFrame appends a single-segment frame of no content that names
// dictionary id.
func appendEmptyFrame(dst []byte, id uint32) []byte {
	var didFlag byte
	var did []byte
	switch {
	case id < 1<<8:
		didFlag, did = 1, []byte{byte(id)}
	case id < 1<<16:
		didFlag, did = 2, binary.LittleEndian.AppendUint16(nil, uint16(id))
	default:
		didFlag, did = 3, binary.LittleEndian.AppendUint32(nil, id)
	}
	dst = append(dst, 0x20|didFlag)
	dst = append(dst, did...)
	// A zero Frame_Content_Size, then an empty Raw_Block that is the last.
	return append(dst, 0, 1, 0, 0)
}

// Decompress appends the message of payload, whose header h came from
// ParseHeader, to dst. The caller has checked h.ContentSize against its limits
// and that h.DictID is d.ID.
func (d *Dict) Decompress(dst, payload []byte, h Header) ([]byte, error) {
	size := int(h.ContentSize)
	dst = slices.Grow(dst, size)
	framed := framedPool.Get().(*[]byte)
	defer framedPool.Put(framed)
	*framed = append(append((*framed)[:0], frameMagic...), payload...)
	out, err := d.dec.DecodeAll(*framed, dst)
	if err != nil || len(out)-len(dst) != size {
		return dst, ErrInvalid
	}
	return out, nil
}

// framedPool holds the buffers in which Decompress puts the magic number back
// in front of a payload.
var framedPool = sync.Pool{New: func() any { return new([]byte) }}

// frameLimit is how many bytes of messages a frame takes before the encoder
// ends it and starts the next, as libzstd's codec does.
const frameLimit = 60 << 10

// errClosed reports a stream context used after Close.
var errClosed = errors.New("zstd: stream context is closed")

// plainStreams holds the stream contexts without a dictionary, per level.
var plainStreams sync.Map // int -> *Dict

func streamDict(dict *Dict, level int) *Dict {
	if dict != nil {
		return dict
	}
	if d, ok := plainStreams.Load(level); ok {
		return d.(*Dict)
	}
	d, _ := plainStreams.LoadOrStore(level, &Dict{level: level})
	return d.(*Dict)
}

// appendWriter appends what is written to b, after skipping skip bytes.
type appendWriter struct {
	b    []byte
	skip int
}

func (w *appendWriter) Write(p []byte) (int, error) {
	k := min(w.skip, len(p))
	w.skip -= k
	w.b = append(w.b, p[k:]...)
	return len(p), nil
}

// StreamEncoder compresses the messages of one direction of a call into
// ZSTD_STREAM_MESSAGE chunks: magicless frames without checksum or content
// size, flushed after every message and ended every frameLimit bytes. It is
// used by one goroutine at a time, and Close may be called from any.
type StreamEncoder struct {
	mu   sync.Mutex
	enc  *kzstd.Encoder
	from *Dict
	out  appendWriter
	// open reports a frame begun, and written that its header is out.
	open, written bool
	// inFrame counts the bytes of the messages in the current frame.
	inFrame int
}

// NewStreamEncoder creates an encoder at level with a window of
// 2^windowLog bytes, whose frame starts from dict if it is not nil.
func NewStreamEncoder(dict *Dict, level, windowLog int) (*StreamEncoder, error) {
	if windowLog != StreamWindowLog || (dict != nil && dict.level != level) {
		return nil, errors.New("zstd: invalid stream compression parameters")
	}
	from := streamDict(dict, level)
	enc, _ := from.streamEncoders.Get().(*kzstd.Encoder)
	if enc == nil {
		var err error
		opts := append(from.encoderOptions(), kzstd.WithWindowSize(1<<windowLog), kzstd.WithEncoderConcurrency(1))
		if enc, err = kzstd.NewWriter(nil, opts...); err != nil {
			return nil, errors.New("zstd: invalid stream compression parameters")
		}
	}
	return &StreamEncoder{enc: enc, from: from}, nil
}

// Encode appends the chunk that src, one message, compresses to.
func (e *StreamEncoder) Encode(dst, src []byte) ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.enc == nil {
		return dst, errClosed
	}
	e.out.b = dst
	defer func() { e.out.b = nil }()
	if !e.open {
		e.enc.Reset(&e.out)
		e.out.skip = len(frameMagic)
		e.open, e.written = true, false
	}
	if _, err := e.enc.Write(src); err != nil {
		return dst, errors.New("zstd: compression failed")
	}
	// An empty message never ends a frame, so it stays an empty chunk.
	if e.inFrame+len(src) < frameLimit {
		e.inFrame += len(src)
		if err := e.enc.Flush(); err != nil {
			return dst, errors.New("zstd: compression failed")
		}
		e.written = e.written || len(src) > 0
		return e.out.b, nil
	}
	// Closing a frame whose header is not out writes it as one block with a
	// content size, which stream frames must not have, so flush it first.
	if !e.written {
		if err := e.enc.Flush(); err != nil {
			return dst, errors.New("zstd: compression failed")
		}
	}
	if err := e.enc.Close(); err != nil {
		return dst, errors.New("zstd: compression failed")
	}
	e.open, e.inFrame = false, 0
	return e.out.b, nil
}

// Close returns the encoder's context to its pool. Encode fails afterwards.
func (e *StreamEncoder) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.enc != nil {
		e.from.streamEncoders.Put(e.enc)
		e.enc = nil
	}
}

// chunkReader serves pre, then one chunk, to a decoder, then io.EOF.
type chunkReader struct{ pre, b []byte }

func (r *chunkReader) Read(p []byte) (int, error) {
	src := &r.pre
	if len(r.pre) == 0 {
		src = &r.b
	}
	if len(*src) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *src)
	*src = (*src)[n:]
	return n, nil
}

// StreamDecoder decompresses the ZSTD_STREAM_MESSAGE chunks of one direction
// of a call, in order. It is used by one goroutine at a time, and Close may be
// called from any.
type StreamDecoder struct {
	mu     sync.Mutex
	closed bool
	dec    *kzstd.Decoder
	// from is the Dict whose pool dec came from.
	from *Dict
	in   chunkReader
}

// NewStreamDecoder creates a decoder that accepts windows of at most
// 2^maxWindowLog bytes.
func NewStreamDecoder(maxWindowLog int) (*StreamDecoder, error) {
	if maxWindowLog != StreamWindowLog {
		return nil, errors.New("zstd: invalid stream decompression parameters")
	}
	return &StreamDecoder{}, nil
}

// Decode appends the message of chunk to dst: exactly size bytes. info is
// what ParseStreamChunk returned for chunk, and dict the dictionary named by
// the frame it starts, nil for none. A chunk that does not decompress to
// exactly size bytes is ErrInvalid, and the decoder cannot be used after it.
func (d *StreamDecoder) Decode(dst, chunk []byte, info StreamChunk, dict *Dict, size int) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return dst, errClosed
	}
	if info.StartsFrame {
		if err := d.startFrame(dict); err != nil {
			return dst, err
		}
	}
	if len(chunk) == 0 {
		if size != 0 {
			return dst, ErrInvalid
		}
		return dst, nil
	}
	if d.dec == nil {
		return dst, ErrInvalid
	}
	// The decoder reads a block only when it needs output, and Read decodes
	// at most one block once it has some, so it never reads past the chunk
	// while blocks are left in it. One spare byte shows a chunk holding more
	// than size bytes.
	d.in.b = chunk
	dst = slices.Grow(dst, size+1)
	out := dst[len(dst) : len(dst)+size+1]
	got := 0
	for len(d.in.b) > 0 {
		n, err := d.dec.Read(out[got:])
		got += n
		if got > size {
			return dst, ErrInvalid
		}
		if err != nil {
			// Blocks without output at the end of the chunk are read past,
			// which is only the end of the stream when the frame ended there.
			if err != io.EOF || len(d.in.b) > 0 || !info.EndsFrame {
				return dst, ErrInvalid
			}
			break
		}
	}
	d.in.pre, d.in.b = nil, nil
	if got != size {
		return dst, ErrInvalid
	}
	return dst[:len(dst)+size], nil
}

// startFrame readies the decoder for a frame that starts from dict, nil for
// none. Decoders are pooled by the dictionary they hold, so a frame without a
// dictionary does not inherit the previous one's.
func (d *StreamDecoder) startFrame(dict *Dict) error {
	from := dict
	if from == nil {
		from = &plainDecoders
	}
	if d.from != from {
		d.release()
		d.dec, _ = from.streamDecoders.Get().(*kzstd.Decoder)
		if d.dec == nil {
			opts := []kzstd.DOption{
				kzstd.WithDecoderConcurrency(1),
				kzstd.WithDecoderMaxWindow(1 << StreamWindowLog),
			}
			if from.data != nil {
				opts = append(opts, kzstd.WithDecoderDicts(from.data))
			}
			var err error
			if d.dec, err = kzstd.NewReader(nil, opts...); err != nil {
				return ErrInvalid
			}
		}
		d.from = from
	}
	if err := d.dec.Reset(&d.in); err != nil {
		return ErrInvalid
	}
	d.in.pre = frameMagic
	return nil
}

// plainDecoders pools the stream decoders without a dictionary.
var plainDecoders Dict

func (d *StreamDecoder) release() {
	if d.dec != nil {
		_ = d.dec.Reset(nil)
		d.from.streamDecoders.Put(d.dec)
		d.dec, d.from = nil, nil
	}
}

// Close returns the decoder's context to its pool. Decode fails afterwards.
func (d *StreamDecoder) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.release()
}

// StreamEncoderCost is an estimate of the memory of a StreamEncoder at level
// with a window of 2^windowLog bytes: about what klauspost's fastest dictionary
// encoder allocates.
func StreamEncoderCost(int, int) int64 { return 600 << 10 }

// StreamDecoderCost is an estimate of the memory of a StreamDecoder for
// windows of at most 2^windowLog bytes.
func StreamDecoderCost(windowLog int) int64 { return 3<<windowLog + 128<<10 }
