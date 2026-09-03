package ubiq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"sync"

	quic "github.com/quic-go/quic-go"
)

type QUICConfig struct {
	Name       string
	PeerAddr   string
	ListenAddr string
	ClientTLS  *tls.Config
	ServerTLS  *tls.Config
}

type QUICTransport struct {
	cfg      QUICConfig
	mu       sync.Mutex
	listener *quic.Listener
	handlers map[string][]Handler
	cancel   context.CancelFunc
}

func NewQUICTransport(cfg QUICConfig) (*QUICTransport, error) {
	if cfg.Name == "" {
		return nil, errors.New("quic name required")
	}
	if cfg.ClientTLS == nil && cfg.PeerAddr != "" {
		return nil, errors.New("QUIC client TLS required")
	}
	q := &QUICTransport{cfg: cfg, handlers: make(map[string][]Handler)}
	if cfg.ListenAddr != "" {
		if cfg.ServerTLS == nil {
			return nil, errors.New("QUIC server TLS required")
		}
		ln, err := quic.ListenAddr(cfg.ListenAddr, cfg.ServerTLS, &quic.Config{EnableDatagrams: false})
		if err != nil {
			return nil, err
		}
		q.listener = ln
		ctx, cancel := context.WithCancel(context.Background())
		q.cancel = cancel
		go q.acceptLoop(ctx)
	}
	return q, nil
}

func (q *QUICTransport) Name() string { return q.cfg.Name }

func (q *QUICTransport) Capabilities() Capabilities {
	return Capabilities{Reliable: true, Ordered: true, Bidirectional: true, Streaming: true}
}

func (q *QUICTransport) Publish(ctx context.Context, env Envelope) error {
	if q.cfg.PeerAddr == "" {
		return errors.New("QUIC peer address not configured")
	}
	if err := env.Validate(); err != nil {
		return err
	}
	conn, err := quic.DialAddr(ctx, q.cfg.PeerAddr, q.cfg.ClientTLS, &quic.Config{EnableDatagrams: false})
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "published")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stream).Encode(env); err != nil {
		stream.CancelWrite(1)
		return err
	}
	return stream.Close()
}

func (q *QUICTransport) Subscribe(_ context.Context, event string, h Handler) (Subscription, error) {
	if q.listener == nil {
		return nil, errors.New("QUIC listener not configured")
	}
	q.mu.Lock()
	q.handlers[event] = append(q.handlers[event], h)
	idx := len(q.handlers[event]) - 1
	q.mu.Unlock()
	return &funcSubscription{close: func() error {
		q.mu.Lock()
		defer q.mu.Unlock()
		hs := q.handlers[event]
		if idx < len(hs) {
			hs[idx] = nil
			q.handlers[event] = hs
		}
		return nil
	}}, nil
}

func (q *QUICTransport) acceptLoop(ctx context.Context) {
	for {
		conn, err := q.listener.Accept(ctx)
		if err != nil {
			return
		}
		go q.handleConn(ctx, conn)
	}
}

func (q *QUICTransport) handleConn(ctx context.Context, conn *quic.Conn) {
	defer conn.CloseWithError(0, "done")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			var env Envelope
			if err := json.NewDecoder(stream).Decode(&env); err != nil {
				return
			}
			if err := env.Validate(); err != nil {
				return
			}
			q.dispatch(ctx, env)
		}()
	}
}

func (q *QUICTransport) dispatch(ctx context.Context, env Envelope) {
	q.mu.Lock()
	specific := append([]Handler(nil), q.handlers[env.Event]...)
	all := append([]Handler(nil), q.handlers["*"]...)
	q.mu.Unlock()
	for _, h := range append(specific, all...) {
		if h != nil {
			_ = h(ctx, env)
		}
	}
}

func (q *QUICTransport) Close() error {
	if q.cancel != nil {
		q.cancel()
	}
	if q.listener != nil {
		return q.listener.Close()
	}
	return nil
}
