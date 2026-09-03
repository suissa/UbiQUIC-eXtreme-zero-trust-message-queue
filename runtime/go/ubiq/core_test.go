package ubiq

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func testEnvelope(id, idem string) Envelope {
	return Envelope{ID: id, Event: "Financial.createInvoice.request", CorrelationID: "corr-1", CausationID: "cause-1", IdempotencyKey: idem, SchemaID: "Financial.createInvoice.request@1", Payload: json.RawMessage(`{"invoice":"A-1"}`), Guarantee: GuaranteeProcessed, CreatedAtMS: time.Now().UnixMilli()}
}

func TestDurableSettlementAndIdempotency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	ctx := context.Background()
	s, err := OpenControlStore(path)
	if err != nil { t.Fatal(err) }
	env := testEnvelope("evt-1", "idem-1")
	if _, created, err := s.Publish(ctx, env); err != nil || !created { t.Fatalf("publish created=%v err=%v", created, err) }
	if _, created, err := s.Publish(ctx, testEnvelope("evt-duplicate", "idem-1")); err != nil || created { t.Fatalf("dedupe created=%v err=%v", created, err) }
	claimed, err := s.Claim(ctx, "worker-a", time.Second); if err != nil { t.Fatal(err) }
	if claimed.Lease.FencingToken == 0 { t.Fatal("missing fencing token") }
	if err := s.MarkReceived(ctx, ControlFrame{Kind: ControlReceived, EventID: env.ID, WorkerID: "worker-a", FencingToken: claimed.Lease.FencingToken, AtMS: time.Now().UnixMilli()}); err != nil { t.Fatal(err) }
	if err := s.Settle(ctx, ControlFrame{Kind: ControlSettledOK, EventID: env.ID, WorkerID: "worker-a", FencingToken: claimed.Lease.FencingToken, AtMS: time.Now().UnixMilli()}); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	s, err = OpenControlStore(path); if err != nil { t.Fatal(err) }; defer s.Close()
	state, err := s.State(ctx, env.ID); if err != nil { t.Fatal(err) }
	if state != DeliverySettledOK { t.Fatalf("state=%s", state) }
}

func TestLeaseExpirationReassignsWithNewFence(t *testing.T) {
	ctx := context.Background(); s, err := OpenControlStore(filepath.Join(t.TempDir(), "control.db")); if err != nil { t.Fatal(err) }; defer s.Close()
	env := testEnvelope("evt-2", "idem-2"); if _, _, err := s.Publish(ctx, env); err != nil { t.Fatal(err) }
	a, err := s.Claim(ctx, "worker-a", 10*time.Millisecond); if err != nil { t.Fatal(err) }
	time.Sleep(15 * time.Millisecond); if _, err := s.RequeueExpired(ctx, time.Now()); err != nil { t.Fatal(err) }
	b, err := s.Claim(ctx, "worker-b", time.Second); if err != nil { t.Fatal(err) }
	if b.Lease.FencingToken <= a.Lease.FencingToken { t.Fatal("fencing token did not advance") }
	err = s.Settle(ctx, ControlFrame{Kind: ControlSettledOK, EventID: env.ID, WorkerID: "worker-a", FencingToken: a.Lease.FencingToken, AtMS: time.Now().UnixMilli()})
	if !errors.Is(err, ErrStaleLease) { t.Fatalf("old worker should be stale: %v", err) }
}

func TestHybridPQCSessionAndPoP(t *testing.T) {
	alice, err := NewCryptoProvider("alice"); if err != nil { t.Fatal(err) }
	bob, err := NewCryptoProvider("bob"); if err != nil { t.Fatal(err) }
	offer, a, err := alice.Establish(bob.Public()); if err != nil { t.Fatal(err) }; defer a.Destroy()
	b, err := bob.Accept(offer); if err != nil { t.Fatal(err) }; defer b.Destroy()
	aad := []byte("Financial.createInvoice.request")
	cipher, err := Seal(a, []byte("secret-payload"), aad); if err != nil { t.Fatal(err) }
	plain, err := Open(b, cipher, aad); if err != nil { t.Fatal(err) }
	if string(plain) != "secret-payload" { t.Fatalf("plain=%q", plain) }
	env := testEnvelope("evt-crypto", "idem-crypto"); now := time.Now()
	proof, err := alice.NewPoP("PUBLISH", "nats://ubiq", env, now); if err != nil { t.Fatal(err) }
	if err := bob.VerifyPoP(alice.Public(), proof, "PUBLISH", "nats://ubiq", env, now, time.Minute); err != nil { t.Fatal(err) }
	if err := bob.VerifyPoP(alice.Public(), proof, "PUBLISH", "nats://ubiq", env, now, time.Minute); err == nil { t.Fatal("replay accepted") }
}

func TestMemoryTransportPreservesCanonicalEvent(t *testing.T) {
	m := NewMemoryTransport("memory"); defer m.Close(); got := make(chan Envelope, 1)
	sub, err := m.Subscribe(context.Background(), "Financial.createInvoice.request", func(_ context.Context, e Envelope) error { got <- e; return nil }); if err != nil { t.Fatal(err) }; defer sub.Close()
	env := testEnvelope("evt-mem", "idem-mem"); if err := m.Publish(context.Background(), env); err != nil { t.Fatal(err) }
	select { case e := <-got: if e.Event != env.Event { t.Fatalf("event changed: %s", e.Event) }; case <-time.After(time.Second): t.Fatal("timeout") }
}
