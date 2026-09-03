package ubiq

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type TCPConfig struct {
	Name       string
	PeerAddr   string
	ListenAddr string
	Dialer     *net.Dialer
	MaxFrame   uint32
}

type TCPTransport struct {
	cfg      TCPConfig
	listener net.Listener
	mu       sync.RWMutex
	handlers map[string][]Handler
	cancel   context.CancelFunc
}

func NewTCPTransport(cfg TCPConfig) (*TCPTransport, error) {
	if cfg.Name == "" {
		return nil, errors.New("tcp name required")
	}
	if cfg.MaxFrame == 0 {
		cfg.MaxFrame = 16 << 20
	}
	t := &TCPTransport{cfg: cfg, handlers: make(map[string][]Handler)}
	if cfg.ListenAddr != "" {
		ln, err := net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			return nil, err
		}
		t.listener = ln
		ctx, cancel := context.WithCancel(context.Background())
		t.cancel = cancel
		go t.acceptLoop(ctx)
	}
	return t, nil
}

func (t *TCPTransport) Name() string { return t.cfg.Name }

func (t *TCPTransport) Capabilities() Capabilities {
	return Capabilities{Reliable: true, Ordered: true, Bidirectional: true, Streaming: true}
}

func (t *TCPTransport) Publish(ctx context.Context, env Envelope) error {
	if t.cfg.PeerAddr == "" {
		return errors.New("tcp peer address not configured")
	}
	raw, err := encodeEnvelope(env)
	if err != nil {
		return err
	}
	if uint32(len(raw)) > t.cfg.MaxFrame {
		return errors.New("tcp frame too large")
	}
	d := t.cfg.Dialer
	if d == nil {
		d = &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	}
	conn, err := d.DialContext(ctx, "tcp", t.cfg.PeerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err := conn.Write(header[:]); err != nil {
		return err
	}
	_, err = conn.Write(raw)
	return err
}

func (t *TCPTransport) Subscribe(_ context.Context, event string, h Handler) (Subscription, error) {
	if t.listener == nil {
		return nil, errors.New("tcp listener not configured")
	}
	if h == nil {
		return nil, errors.New("handler required")
	}
	t.mu.Lock()
	t.handlers[event] = append(t.handlers[event], h)
	idx := len(t.handlers[event]) - 1
	t.mu.Unlock()
	return &funcSubscription{close: func() error {
		t.mu.Lock()
		defer t.mu.Unlock()
		if idx < len(t.handlers[event]) {
			t.handlers[event][idx] = nil
		}
		return nil
	}}, nil
}

func (t *TCPTransport) acceptLoop(ctx context.Context) {
	for {
		c, err := t.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
		go t.handleConn(ctx, c)
	}
}

func (t *TCPTransport) handleConn(ctx context.Context, c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		var header [4]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[:])
		if n == 0 || n > t.cfg.MaxFrame {
			return
		}
		raw := make([]byte, n)
		if _, err := io.ReadFull(r, raw); err != nil {
			return
		}
		env, err := decodeEnvelope(raw)
		if err != nil {
			continue
		}
		t.dispatch(ctx, env)
	}
}

func (t *TCPTransport) dispatch(ctx context.Context, env Envelope) {
	t.mu.RLock()
	exact := append([]Handler(nil), t.handlers[env.Event]...)
	all := append([]Handler(nil), t.handlers["*"]...)
	t.mu.RUnlock()
	for _, h := range append(exact, all...) {
		if h != nil {
			_ = h(ctx, env)
		}
	}
}

func (t *TCPTransport) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}
