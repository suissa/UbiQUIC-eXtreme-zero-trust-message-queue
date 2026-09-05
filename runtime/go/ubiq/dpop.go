package ubiq

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

type DPoPVerifier struct { mu sync.Mutex; seen map[string]int64; MaxSkew time.Duration }
func NewDPoPVerifier()*DPoPVerifier{return &DPoPVerifier{seen:make(map[string]int64),MaxSkew:5*time.Minute}}
type dpopClaims struct{JTI string `json:"jti"`;HTM string `json:"htm"`;HTU string `json:"htu"`;IAT int64 `json:"iat"`;ATH string `json:"ath,omitempty"`;Nonce string `json:"nonce,omitempty"`}
func(v *DPoPVerifier)Verify(token,method,target,accessToken string,now time.Time)error{obj,err:=jose.ParseSigned(token,[]jose.SignatureAlgorithm{jose.ES256,jose.EdDSA});if err!=nil{return err};if len(obj.Signatures)!=1{return errors.New("DPoP must contain exactly one signature")};header:=obj.Signatures[0].Header;if header.JSONWebKey==nil||!header.JSONWebKey.Valid()||!header.JSONWebKey.IsPublic(){return errors.New("DPoP requires an embedded public JWK")};if typ,ok:=header.ExtraHeaders[jose.HeaderKey("typ")];ok&&!strings.EqualFold(fmt.Sprint(typ),"dpop+jwt"){return errors.New("invalid DPoP typ")};payload,err:=obj.Verify(header.JSONWebKey.Key);if err!=nil{return fmt.Errorf("DPoP signature: %w",err)};var claims dpopClaims;if err:=json.Unmarshal(payload,&claims);err!=nil{return err};if claims.JTI==""||claims.HTM==""||claims.HTU==""||claims.IAT==0{return errors.New("incomplete DPoP claims")};if !strings.EqualFold(claims.HTM,method)||claims.HTU!=target{return errors.New("DPoP request binding mismatch")};skew:=v.MaxSkew;if skew<=0{skew=5*time.Minute};issued:=time.Unix(claims.IAT,0);if now.Sub(issued)>skew||issued.Sub(now)>skew{return errors.New("DPoP proof outside freshness window")};if accessToken!=""{sum:=sha256.Sum256([]byte(accessToken));expected:=base64.RawURLEncoding.EncodeToString(sum[:]);if claims.ATH!=expected{return errors.New("DPoP ath mismatch")}};jwkRaw,err:=json.Marshal(header.JSONWebKey);if err!=nil{return err};fingerprint:=sha256.Sum256(jwkRaw);replayKey:=base64.RawURLEncoding.EncodeToString(fingerprint[:])+":"+claims.JTI;v.mu.Lock();defer v.mu.Unlock();cutoff:=now.Add(-2*skew).Unix();for k,at:=range v.seen{if at<cutoff{delete(v.seen,k)}};if _,ok:=v.seen[replayKey];ok{return errors.New("replayed DPoP proof")};v.seen[replayKey]=claims.IAT;return nil}
