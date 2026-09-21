package ubiq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrNothingAvailable  = errors.New("no event available")
	ErrStaleLease        = errors.New("stale lease or fencing token")
	ErrInvalidTransition = errors.New("invalid delivery transition")
)

type ControlStore struct {
	db *sql.DB
}

func OpenControlStore(path string) (*ControlStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &ControlStore{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *ControlStore) Close() error { return s.db.Close() }
func (s *ControlStore) DB() *sql.DB { return s.db }

func (s *ControlStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  event_name TEXT NOT NULL,
  envelope BLOB NOT NULL,
  state TEXT NOT NULL,
  owner TEXT,
  lease_until_ms INTEGER,
  attempt INTEGER NOT NULL DEFAULT 0,
  fencing_token INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_claim_idx ON events(state, created_at_ms);
CREATE TABLE IF NOT EXISTS settlements (
  event_id TEXT PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  fencing_token INTEGER NOT NULL,
  at_ms INTEGER NOT NULL,
  evidence BLOB
);
CREATE TABLE IF NOT EXISTS delivery_history (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  state TEXT NOT NULL,
  worker_id TEXT,
  fencing_token INTEGER,
  at_ms INTEGER NOT NULL,
  details BLOB
);
CREATE TABLE IF NOT EXISTS outbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  transport TEXT NOT NULL,
  payload BLOB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at_ms INTEGER NOT NULL,
  delivered_at_ms INTEGER
);
`)
	return err
}

func (s *ControlStore) Publish(ctx context.Context, env Envelope) (Envelope, bool, error) {
	if err := env.Validate(); err != nil {
		return Envelope{}, false, err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return Envelope{}, false, err
	}
	now := time.Now().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
INSERT INTO events(id,idempotency_key,event_name,envelope,state,created_at_ms,updated_at_ms)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(idempotency_key) DO NOTHING`, env.ID, env.IdempotencyKey, env.Event, raw, DeliveryPublished, env.CreatedAtMS, now)
	if err != nil {
		return Envelope{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return Envelope{}, false, err
	}
	if n == 1 {
		_, _ = s.db.ExecContext(ctx, `INSERT INTO delivery_history(event_id,state,at_ms) VALUES(?,?,?)`, env.ID, DeliveryPublished, now)
		return env, true, nil
	}
	var existingRaw []byte
	if err := s.db.QueryRowContext(ctx, `SELECT envelope FROM events WHERE idempotency_key=?`, env.IdempotencyKey).Scan(&existingRaw); err != nil {
		return Envelope{}, false, err
	}
	var existing Envelope
	if err := json.Unmarshal(existingRaw, &existing); err != nil {
		return Envelope{}, false, err
	}
	return existing, false, nil
}

func (s *ControlStore) Claim(ctx context.Context, workerID string, lease time.Duration) (ClaimedEvent, error) {
	if workerID == "" || lease <= 0 {
		return ClaimedEvent{}, errors.New("worker and positive lease are required")
	}
	now := time.Now().UnixMilli()
	until := now + lease.Milliseconds()
	row := s.db.QueryRowContext(ctx, `
WITH candidate AS (
  SELECT id FROM events
  WHERE state IN (?,?)
  ORDER BY created_at_ms, id
  LIMIT 1
)
UPDATE events
SET state=?, owner=?, lease_until_ms=?, attempt=attempt+1,
    fencing_token=fencing_token+1, updated_at_ms=?
WHERE id=(SELECT id FROM candidate)
RETURNING envelope, state, attempt, fencing_token`, DeliveryPublished, DeliveryExpired, DeliveryLeased, workerID, until, now)

	var raw []byte
	var state DeliveryState
	var attempt int
	var fencing uint64
	if err := row.Scan(&raw, &state, &attempt, &fencing); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ClaimedEvent{}, ErrNothingAvailable
		}
		return ClaimedEvent{}, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return ClaimedEvent{}, err
	}
	leaseRecord := Lease{EventID: env.ID, WorkerID: workerID, UntilMS: until, Attempt: attempt, FencingToken: fencing}
	_, _ = s.db.ExecContext(ctx, `INSERT INTO delivery_history(event_id,state,worker_id,fencing_token,at_ms) VALUES(?,?,?,?,?)`, env.ID, DeliveryLeased, workerID, fencing, now)
	return ClaimedEvent{Envelope: env, Lease: leaseRecord, State: state}, nil
}

func (s *ControlStore) MarkReceived(ctx context.Context, frame ControlFrame) error {
	if frame.Kind != ControlReceived {
		return ErrInvalidTransition
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
UPDATE events SET state=?, updated_at_ms=?
WHERE id=? AND owner=? AND fencing_token=? AND state=? AND lease_until_ms>?`,
		DeliveryReceived, now, frame.EventID, frame.WorkerID, frame.FencingToken, DeliveryLeased, now)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrStaleLease
	}
	_, _ = s.db.ExecContext(ctx, `INSERT INTO delivery_history(event_id,state,worker_id,fencing_token,at_ms,details) VALUES(?,?,?,?,?,?)`, frame.EventID, DeliveryReceived, frame.WorkerID, frame.FencingToken, frame.AtMS, frame.Evidence)
	return nil
}

func (s *ControlStore) Settle(ctx context.Context, frame ControlFrame) error {
	if frame.Kind != ControlSettledOK && frame.Kind != ControlSettledError {
		return ErrInvalidTransition
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	target := DeliverySettledOK
	if frame.Kind == ControlSettledError {
		target = DeliverySettledError
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `
UPDATE events SET state=?, owner=NULL, lease_until_ms=NULL, updated_at_ms=?
WHERE id=? AND owner=? AND fencing_token=? AND state IN (?,?) AND lease_until_ms>?`,
		target, now, frame.EventID, frame.WorkerID, frame.FencingToken, DeliveryLeased, DeliveryReceived, now)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrStaleLease
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO settlements(event_id,kind,worker_id,fencing_token,at_ms,evidence)
VALUES(?,?,?,?,?,?)
ON CONFLICT(event_id) DO NOTHING`, frame.EventID, frame.Kind, frame.WorkerID, frame.FencingToken, frame.AtMS, frame.Evidence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_history(event_id,state,worker_id,fencing_token,at_ms,details) VALUES(?,?,?,?,?,?)`, frame.EventID, target, frame.WorkerID, frame.FencingToken, frame.AtMS, frame.Evidence); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ControlStore) RequeueExpired(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE events SET state=?, owner=NULL, lease_until_ms=NULL, updated_at_ms=?
WHERE state IN (?,?) AND lease_until_ms<=?`, DeliveryExpired, now.UnixMilli(), DeliveryLeased, DeliveryReceived, now.UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *ControlStore) State(ctx context.Context, eventID string) (DeliveryState, error) {
	var state DeliveryState
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM events WHERE id=?`, eventID).Scan(&state); err != nil {
		return "", err
	}
	return state, nil
}

func (s *ControlStore) EnqueueOutbox(ctx context.Context, eventID, transport string, payload []byte, availableAt time.Time) error {
	if eventID == "" || transport == "" || len(payload) == 0 {
		return errors.New("invalid outbox record")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO outbox(event_id,transport,payload,available_at_ms) VALUES(?,?,?,?)`, eventID, transport, payload, availableAt.UnixMilli())
	return err
}

type OutboxRecord struct {
	Seq       int64
	EventID   string
	Transport string
	Payload   []byte
	Attempts  int
}

func (s *ControlStore) NextOutbox(ctx context.Context, now time.Time) (OutboxRecord, error) {
	var r OutboxRecord
	err := s.db.QueryRowContext(ctx, `SELECT seq,event_id,transport,payload,attempts FROM outbox WHERE delivered_at_ms IS NULL AND available_at_ms<=? ORDER BY seq LIMIT 1`, now.UnixMilli()).Scan(&r.Seq, &r.EventID, &r.Transport, &r.Payload, &r.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxRecord{}, ErrNothingAvailable
	}
	return r, err
}

func (s *ControlStore) MarkOutboxDelivered(ctx context.Context, seq int64, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE outbox SET delivered_at_ms=? WHERE seq=? AND delivered_at_ms IS NULL`, at.UnixMilli(), seq)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("outbox record %d not pending", seq)
	}
	return nil
}
