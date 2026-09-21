package ubiq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type MessageSecurity struct { KeyID string `json:"key_id"`; Proof ProofOfPossession `json:"proof"` }
type TrustStore interface { Resolve(string) (PublicBundle, bool) }
type StaticTrustStore struct { mu sync.RWMutex; peers map[string]PublicBundle }
func NewStaticTrustStore()*StaticTrustStore{return &StaticTrustStore{peers:make(map[string]PublicBundle)}}
func(s *StaticTrustStore)Add(p PublicBundle){s.mu.Lock();s.peers[p.KeyID]=p;s.mu.Unlock()}
func(s *StaticTrustStore)Resolve(id string)(PublicBundle,bool){s.mu.RLock();defer s.mu.RUnlock();p,ok:=s.peers[id];return p,ok}
type SecureTransport struct { inner Transport; provider *CryptoProvider; trust TrustStore; freshness time.Duration }
func WrapSecureTransport(inner Transport,provider *CryptoProvider,trust TrustStore,freshness time.Duration)(*SecureTransport,error){if inner==nil||provider==nil||trust==nil{return nil,errors.New("inner transport, crypto provider and trust store are required")};if freshness<=0{freshness=2*time.Minute};return &SecureTransport{inner:inner,provider:provider,trust:trust,freshness:freshness},nil}
func(s *SecureTransport)Name()string{return s.inner.Name()}
func(s *SecureTransport)Capabilities()Capabilities{return s.inner.Capabilities()}
func(s *SecureTransport)target()string{return "ubiq://transport/"+s.inner.Name()}
func(s *SecureTransport)Publish(ctx context.Context,env Envelope)error{unsigned:=env;unsigned.Security=nil;proof,err:=s.provider.NewPoP("PUBLISH",s.target(),unsigned,time.Now());if err!=nil{return err};env.Security=&MessageSecurity{KeyID:s.provider.Public().KeyID,Proof:proof};return s.inner.Publish(ctx,env)}
func(s *SecureTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){return s.inner.Subscribe(ctx,event,func(ctx context.Context,env Envelope)error{if env.Security==nil{return errors.New("unsigned UbiQ envelope")};peer,ok:=s.trust.Resolve(env.Security.KeyID);if !ok{return fmt.Errorf("untrusted UbiQ holder %q",env.Security.KeyID)};unsigned:=env;unsigned.Security=nil;if err:=s.provider.VerifyPoP(peer,env.Security.Proof,"PUBLISH",s.target(),unsigned,time.Now(),s.freshness);err!=nil{return err};return h(ctx,unsigned)})}
func(s *SecureTransport)Close()error{return s.inner.Close()}
