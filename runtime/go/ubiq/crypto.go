package ubiq

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/mldsa"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

const mldsaContext = "ubiq-envelope-v1"

type PublicBundle struct {
	KeyID    string `json:"key_id"`
	Ed25519  []byte `json:"ed25519"`
	MLDSA44  []byte `json:"mldsa44"`
	X25519   []byte `json:"x25519"`
	MLKEM768 []byte `json:"mlkem768"`
}

type HybridSignature struct {
	Ed25519 []byte `json:"ed25519"`
	MLDSA44 []byte `json:"mldsa44"`
}

type SessionOffer struct {
	EphemeralX25519 []byte `json:"ephemeral_x25519"`
	MLKEMCiphertext []byte `json:"mlkem_ciphertext"`
}

type Secret struct {
	mu        sync.Mutex
	material  []byte
	destroyed bool
}

func newSecret(material []byte) *Secret {
	copyOf := append([]byte(nil), material...)
	zero(material)
	return &Secret{material: copyOf}
}

func (s *Secret) Use(fn func([]byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.destroyed {
		return errors.New("secret already destroyed")
	}
	return fn(s.material)
}

func (s *Secret) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.destroyed {
		return
	}
	zero(s.material)
	s.material = nil
	s.destroyed = true
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

type CryptoProvider struct {
	keyID string

	edPublic   ed25519.PublicKey
	edPrivate  ed25519.PrivateKey
	mlPrivate  *mldsa.PrivateKey
	xPrivate   *ecdh.PrivateKey
	kemPrivate *mlkem.DecapsulationKey768

	replayMu sync.Mutex
	replay   map[string]int64
}

func NewCryptoProvider(keyID string) (*CryptoProvider, error) {
	if keyID == "" {
		return nil, errors.New("key id is required")
	}
	edPub, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	mlPriv, err := mldsa.GenerateKey(mldsa.MLDSA44())
	if err != nil {
		return nil, err
	}
	xPriv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	kemPriv, err := mlkem.GenerateKey768()
	if err != nil {
		return nil, err
	}
	return &CryptoProvider{keyID: keyID, edPublic: edPub, edPrivate: edPriv, mlPrivate: mlPriv, xPrivate: xPriv, kemPrivate: kemPriv, replay: make(map[string]int64)}, nil
}

func (p *CryptoProvider) Public() PublicBundle {
	return PublicBundle{KeyID: p.keyID, Ed25519: append([]byte(nil), p.edPublic...), MLDSA44: append([]byte(nil), p.mlPrivate.PublicKey().Bytes()...), X25519: append([]byte(nil), p.xPrivate.PublicKey().Bytes()...), MLKEM768: append([]byte(nil), p.kemPrivate.EncapsulationKey().Bytes()...)}
}

func (p *CryptoProvider) Sign(message []byte) (HybridSignature, error) {
	ed := ed25519.Sign(p.edPrivate, message)
	ml, err := p.mlPrivate.Sign(nil, message, &mldsa.Options{Context: mldsaContext})
	if err != nil {
		return HybridSignature{}, err
	}
	return HybridSignature{Ed25519: ed, MLDSA44: ml}, nil
}

func VerifyHybrid(peer PublicBundle, message []byte, sig HybridSignature) error {
	if len(peer.Ed25519) != ed25519.PublicKeySize || !ed25519.Verify(ed25519.PublicKey(peer.Ed25519), message, sig.Ed25519) {
		return errors.New("invalid Ed25519 signature")
	}
	mlPub, err := mldsa.NewPublicKey(mldsa.MLDSA44(), peer.MLDSA44)
	if err != nil {
		return err
	}
	if err := mldsa.Verify(mlPub, message, sig.MLDSA44, &mldsa.Options{Context: mldsaContext}); err != nil {
		return fmt.Errorf("invalid ML-DSA signature: %w", err)
	}
	return nil
}

func (p *CryptoProvider) Establish(peer PublicBundle) (SessionOffer, *Secret, error) {
	peerX, err := ecdh.X25519().NewPublicKey(peer.X25519)
	if err != nil {
		return SessionOffer{}, nil, err
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return SessionOffer{}, nil, err
	}
	classical, err := ephemeral.ECDH(peerX)
	if err != nil {
		return SessionOffer{}, nil, err
	}
	peerKEM, err := mlkem.NewEncapsulationKey768(peer.MLKEM768)
	if err != nil {
		zero(classical)
		return SessionOffer{}, nil, err
	}
	pq, ciphertext := peerKEM.Encapsulate()
	combined := append(append(make([]byte, 0, len(classical)+len(pq)), classical...), pq...)
	key, err := hkdf.Key(sha256.New, combined, nil, "ubiq-hybrid-x25519-mlkem768-v1", chacha20poly1305.KeySize)
	zero(classical)
	zero(pq)
	zero(combined)
	if err != nil {
		return SessionOffer{}, nil, err
	}
	return SessionOffer{EphemeralX25519: ephemeral.PublicKey().Bytes(), MLKEMCiphertext: ciphertext}, newSecret(key), nil
}

func (p *CryptoProvider) Accept(offer SessionOffer) (*Secret, error) {
	peerX, err := ecdh.X25519().NewPublicKey(offer.EphemeralX25519)
	if err != nil {
		return nil, err
	}
	classical, err := p.xPrivate.ECDH(peerX)
	if err != nil {
		return nil, err
	}
	pq, err := p.kemPrivate.Decapsulate(offer.MLKEMCiphertext)
	if err != nil {
		zero(classical)
		return nil, err
	}
	combined := append(append(make([]byte, 0, len(classical)+len(pq)), classical...), pq...)
	key, err := hkdf.Key(sha256.New, combined, nil, "ubiq-hybrid-x25519-mlkem768-v1", chacha20poly1305.KeySize)
	zero(classical)
	zero(pq)
	zero(combined)
	if err != nil {
		return nil, err
	}
	return newSecret(key), nil
}

func Seal(secret *Secret, plaintext, aad []byte) ([]byte, error) {
	var out []byte
	err := secret.Use(func(key []byte) error {
		aead, err := chacha20poly1305.NewX(key)
		if err != nil {
			return err
		}
		nonce := make([]byte, chacha20poly1305.NonceSizeX)
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		out = aead.Seal(nonce, nonce, plaintext, aad)
		return nil
	})
	return out, err
}

func Open(secret *Secret, ciphertext, aad []byte) ([]byte, error) {
	var out []byte
	err := secret.Use(func(key []byte) error {
		aead, err := chacha20poly1305.NewX(key)
		if err != nil {
			return err
		}
		if len(ciphertext) < chacha20poly1305.NonceSizeX {
			return errors.New("ciphertext too short")
		}
		nonce := ciphertext[:chacha20poly1305.NonceSizeX]
		body := ciphertext[chacha20poly1305.NonceSizeX:]
		out, err = aead.Open(nil, nonce, body, aad)
		return err
	})
	return out, err
}

type ProofOfPossession struct {
	Holder       string          `json:"holder"`
	CreatedAtMS  int64           `json:"created_at_ms"`
	Nonce        string          `json:"nonce"`
	Method       string          `json:"method"`
	Target       string          `json:"target"`
	EnvelopeHash string          `json:"envelope_hash"`
	Signature    HybridSignature `json:"signature"`
}

type unsignedPoP struct {
	Holder       string `json:"holder"`
	CreatedAtMS  int64  `json:"created_at_ms"`
	Nonce        string `json:"nonce"`
	Method       string `json:"method"`
	Target       string `json:"target"`
	EnvelopeHash string `json:"envelope_hash"`
}

func (p *CryptoProvider) NewPoP(method, target string, env Envelope, now time.Time) (ProofOfPossession, error) {
	hash, err := env.DigestHex()
	if err != nil {
		return ProofOfPossession{}, err
	}
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return ProofOfPossession{}, err
	}
	nonce := fmt.Sprintf("%x", nonceBytes)
	unsigned := unsignedPoP{Holder: p.keyID, CreatedAtMS: now.UnixMilli(), Nonce: nonce, Method: method, Target: target, EnvelopeHash: hash}
	raw, err := json.Marshal(unsigned)
	if err != nil {
		return ProofOfPossession{}, err
	}
	sig, err := p.Sign(raw)
	if err != nil {
		return ProofOfPossession{}, err
	}
	return ProofOfPossession{Holder: unsigned.Holder, CreatedAtMS: unsigned.CreatedAtMS, Nonce: unsigned.Nonce, Method: unsigned.Method, Target: unsigned.Target, EnvelopeHash: unsigned.EnvelopeHash, Signature: sig}, nil
}

func (p *CryptoProvider) VerifyPoP(peer PublicBundle, proof ProofOfPossession, method, target string, env Envelope, now time.Time, maxSkew time.Duration) error {
	if proof.Holder != peer.KeyID || proof.Method != method || proof.Target != target {
		return errors.New("proof binding mismatch")
	}
	delta := now.Sub(time.UnixMilli(proof.CreatedAtMS))
	if delta < -maxSkew || delta > maxSkew {
		return errors.New("proof outside freshness window")
	}
	hash, err := env.DigestHex()
	if err != nil {
		return err
	}
	if hash != proof.EnvelopeHash {
		return errors.New("proof envelope hash mismatch")
	}
	unsigned := unsignedPoP{Holder: proof.Holder, CreatedAtMS: proof.CreatedAtMS, Nonce: proof.Nonce, Method: proof.Method, Target: proof.Target, EnvelopeHash: proof.EnvelopeHash}
	raw, err := json.Marshal(unsigned)
	if err != nil {
		return err
	}
	if err := VerifyHybrid(peer, raw, proof.Signature); err != nil {
		return err
	}
	replayKey := proof.Holder + ":" + proof.Nonce
	p.replayMu.Lock()
	defer p.replayMu.Unlock()
	cutoff := now.Add(-2 * maxSkew).UnixMilli()
	for k, at := range p.replay {
		if at < cutoff {
			delete(p.replay, k)
		}
	}
	if _, exists := p.replay[replayKey]; exists {
		return errors.New("replayed proof")
	}
	p.replay[replayKey] = proof.CreatedAtMS
	return nil
}
