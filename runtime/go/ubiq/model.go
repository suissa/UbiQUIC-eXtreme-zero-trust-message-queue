package ubiq

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type EventState string

const (
	StateRequest EventState = "request"
	StateOK      EventState = "ok"
	StateError   EventState = "error"
)

type DeliveryGuarantee string

const (
	GuaranteeBestEffort DeliveryGuarantee = "best_effort"
	GuaranteeReceived   DeliveryGuarantee = "received"
	GuaranteeProcessed  DeliveryGuarantee = "processed"
)

type DeliveryState string

const (
	DeliveryPublished    DeliveryState = "published"
	DeliveryLeased       DeliveryState = "leased"
	DeliveryReceived     DeliveryState = "received"
	DeliverySettledOK    DeliveryState = "settled_ok"
	DeliverySettledError DeliveryState = "settled_error"
	DeliveryExpired      DeliveryState = "expired"
)

var canonicalEventPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*(\.[A-Za-z][A-Za-z0-9_-]*){2,}$`)

type Envelope struct {
	ID             string            `json:"id"`
	Event          string            `json:"event"`
	CorrelationID  string            `json:"correlation_id"`
	CausationID    string            `json:"causation_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	SchemaID       string            `json:"schema_id"`
	Payload        json.RawMessage   `json:"payload"`
	Guarantee      DeliveryGuarantee `json:"guarantee"`
	CreatedAtMS    int64             `json:"created_at_ms"`
	Security       *MessageSecurity  `json:"security,omitempty"`
}

func (e Envelope) State() (EventState, error) {
	if !canonicalEventPattern.MatchString(e.Event) { return "", fmt.Errorf("invalid canonical event %q", e.Event) }
	parts := strings.Split(e.Event, "."); state := EventState(parts[len(parts)-1])
	switch state { case StateRequest, StateOK, StateError: return state,nil; default: return "",fmt.Errorf("invalid event state %q",state) }
}
func (e Envelope) Validate() error {
	if e.ID==""||e.IdempotencyKey==""||e.SchemaID==""||e.CorrelationID=="" { return errors.New("id, idempotency_key, schema_id and correlation_id are required") }
	if _,err:=e.State();err!=nil{return err}
	switch e.Guarantee { case GuaranteeBestEffort,GuaranteeReceived,GuaranteeProcessed: default:return fmt.Errorf("invalid guarantee %q",e.Guarantee) }
	if len(e.Payload)==0||!json.Valid(e.Payload){return errors.New("payload must be valid JSON")};return nil
}
func (e Envelope) CanonicalBytes() ([]byte,error) { if err:=e.Validate();err!=nil{return nil,err};copy:=e;copy.Security=nil;return json.Marshal(copy) }
func (e Envelope) Digest()([32]byte,error){b,err:=e.CanonicalBytes();if err!=nil{return [32]byte{},err};return sha256.Sum256(b),nil}
func (e Envelope) DigestHex()(string,error){d,err:=e.Digest();if err!=nil{return "",err};return hex.EncodeToString(d[:]),nil}

type Lease struct { EventID string `json:"event_id"`;WorkerID string `json:"worker_id"`;UntilMS int64 `json:"until_ms"`;Attempt int `json:"attempt"`;FencingToken uint64 `json:"fencing_token"` }
func(l Lease)Active(now time.Time)bool{return now.UnixMilli()<l.UntilMS}
type ControlKind string
const(ControlReceived ControlKind="RECEIVED";ControlSettledOK ControlKind="SETTLED_OK";ControlSettledError ControlKind="SETTLED_ERROR";ControlLeaseExpired ControlKind="LEASE_EXPIRED")
type ControlFrame struct{Kind ControlKind `json:"kind"`;EventID string `json:"event_id"`;WorkerID string `json:"worker_id"`;FencingToken uint64 `json:"fencing_token"`;AtMS int64 `json:"at_ms"`;Evidence json.RawMessage `json:"evidence,omitempty"`}
func(f ControlFrame)Validate()error{switch f.Kind{case ControlReceived,ControlSettledOK,ControlSettledError,ControlLeaseExpired:default:return fmt.Errorf("invalid control kind %q",f.Kind)};if f.EventID==""||f.WorkerID==""||f.FencingToken==0{return errors.New("event_id, worker_id and fencing_token are required")};return nil}
type ClaimedEvent struct{Envelope Envelope `json:"envelope"`;Lease Lease `json:"lease"`;State DeliveryState `json:"state"`}
