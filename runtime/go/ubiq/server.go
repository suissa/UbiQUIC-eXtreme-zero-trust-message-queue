package ubiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Hub struct { mu sync.RWMutex; next uint64; subs map[uint64]chan Envelope }
func NewHub()*Hub{return &Hub{subs:make(map[uint64]chan Envelope)}}
func(h *Hub)Subscribe(buffer int)(uint64,<-chan Envelope){h.mu.Lock();defer h.mu.Unlock();id:=h.next;h.next++;ch:=make(chan Envelope,buffer);h.subs[id]=ch;return id,ch}
func(h *Hub)Unsubscribe(id uint64){h.mu.Lock();if ch,ok:=h.subs[id];ok{delete(h.subs,id);close(ch)};h.mu.Unlock()}
func(h *Hub)Publish(env Envelope){h.mu.RLock();defer h.mu.RUnlock();for _,ch:=range h.subs{select{case ch<-env:default:}}}

type Server struct{Store *ControlStore;Registry *Registry;Hub *Hub;DefaultTransport string;LeaseDuration time.Duration;upgrader websocket.Upgrader}
func NewServer(store *ControlStore,registry *Registry)*Server{return &Server{Store:store,Registry:registry,Hub:NewHub(),LeaseDuration:5*time.Second,upgrader:websocket.Upgrader{CheckOrigin:func(*http.Request)bool{return true}}}}
func(s *Server)Handler()http.Handler{mux:=http.NewServeMux();mux.HandleFunc("POST /v1/events/{event}",s.handlePublish);mux.HandleFunc("GET /v1/events/sse",s.handleSSE);mux.HandleFunc("GET /v1/events/ws",s.handleWS);mux.HandleFunc("GET /v1/work/claim",s.handleClaim);mux.HandleFunc("POST /v1/control/received",s.handleReceived);mux.HandleFunc("POST /v1/control/settle",s.handleSettle);mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,_ *http.Request){w.WriteHeader(http.StatusNoContent)});return mux}
func(s *Server)ingest(ctx context.Context,env Envelope,transport string)(Envelope,bool,error){stored,created,err:=s.Store.Publish(ctx,env);if err!=nil{return Envelope{},false,err};s.Hub.Publish(stored);if transport==""{transport=s.DefaultTransport};if transport!=""&&created{t,err:=s.Registry.Get(transport);if err!=nil{return Envelope{},false,err};if err:=t.Publish(ctx,stored);err!=nil{raw,_:=json.Marshal(stored);_=s.Store.EnqueueOutbox(ctx,stored.ID,transport,raw,time.Now().Add(time.Second));return stored,created,err}};return stored,created,nil}
func(s *Server)handlePublish(w http.ResponseWriter,r *http.Request){var env Envelope;dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,8<<20));dec.DisallowUnknownFields();if err:=dec.Decode(&env);err!=nil{writeError(w,http.StatusBadRequest,err);return};pathEvent:=r.PathValue("event");if env.Event!=pathEvent{writeError(w,http.StatusBadRequest,fmt.Errorf("path event %q differs from envelope %q",pathEvent,env.Event));return};stored,created,err:=s.ingest(r.Context(),env,r.URL.Query().Get("transport"));if err!=nil{writeError(w,http.StatusBadGateway,err);return};status:=http.StatusAccepted;if !created{status=http.StatusOK};writeJSON(w,status,stored)}
func(s *Server)handleClaim(w http.ResponseWriter,r *http.Request){worker:=r.URL.Query().Get("worker");lease:=s.LeaseDuration;if ms:=r.URL.Query().Get("lease_ms");ms!=""{var v int64;if _,err:=fmt.Sscan(ms,&v);err==nil&&v>0{lease=time.Duration(v)*time.Millisecond}};claimed,err:=s.Store.Claim(r.Context(),worker,lease);if errors.Is(err,ErrNothingAvailable){w.WriteHeader(http.StatusNoContent);return};if err!=nil{writeError(w,http.StatusConflict,err);return};writeJSON(w,http.StatusOK,claimed)}
func(s *Server)handleReceived(w http.ResponseWriter,r *http.Request){var frame ControlFrame;if err:=json.NewDecoder(r.Body).Decode(&frame);err!=nil{writeError(w,400,err);return};frame.Kind=ControlReceived;if frame.AtMS==0{frame.AtMS=time.Now().UnixMilli()};if err:=s.Store.MarkReceived(r.Context(),frame);err!=nil{writeError(w,http.StatusConflict,err);return};w.WriteHeader(http.StatusNoContent)}
func(s *Server)handleSettle(w http.ResponseWriter,r *http.Request){var frame ControlFrame;if err:=json.NewDecoder(r.Body).Decode(&frame);err!=nil{writeError(w,400,err);return};if frame.AtMS==0{frame.AtMS=time.Now().UnixMilli()};if err:=s.Store.Settle(r.Context(),frame);err!=nil{writeError(w,http.StatusConflict,err);return};w.WriteHeader(http.StatusNoContent)}
func(s *Server)handleSSE(w http.ResponseWriter,r *http.Request){flusher,ok:=w.(http.Flusher);if !ok{writeError(w,500,errors.New("streaming unsupported"));return};filter:=r.URL.Query().Get("event");id,ch:=s.Hub.Subscribe(64);defer s.Hub.Unsubscribe(id);w.Header().Set("Content-Type","text/event-stream");w.Header().Set("Cache-Control","no-cache");w.Header().Set("Connection","keep-alive");flusher.Flush();for{select{case<-r.Context().Done():return;case env,ok:=<-ch:if !ok{return};if filter!=""&&filter!="*"&&filter!=env.Event{continue};raw,_:=json.Marshal(env);fmt.Fprintf(w,"event: %s\ndata: %s\n\n",env.Event,raw);flusher.Flush()}}}
func(s *Server)handleWS(w http.ResponseWriter,r *http.Request){conn,err:=s.upgrader.Upgrade(w,r,nil);if err!=nil{return};defer conn.Close();id,ch:=s.Hub.Subscribe(64);defer s.Hub.Unsubscribe(id);ctx,cancel:=context.WithCancel(r.Context());defer cancel();go func(){defer cancel();for env:=range ch{if err:=conn.WriteJSON(env);err!=nil{return}}}();for{var env Envelope;if err:=conn.ReadJSON(&env);err!=nil{return};transport:=r.URL.Query().Get("transport");if _,_,err:=s.ingest(ctx,env,transport);err!=nil{_=conn.WriteJSON(map[string]string{"error":err.Error()})}}}
func(s *Server)BindTransport(ctx context.Context,name,event string)(Subscription,error){t,err:=s.Registry.Get(name);if err!=nil{return nil,err};return t.Subscribe(ctx,event,func(ctx context.Context,env Envelope)error{_,_,err:=s.Store.Publish(ctx,env);if err==nil{s.Hub.Publish(env)};return err})}
func writeJSON(w http.ResponseWriter,status int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(status);_=json.NewEncoder(w).Encode(v)}
func writeError(w http.ResponseWriter,status int,err error){writeJSON(w,status,map[string]string{"error":strings.TrimSpace(err.Error())})}
func(s *Server)RunMaintenance(ctx context.Context,interval time.Duration){if interval<=0{interval=time.Second};ticker:=time.NewTicker(interval);defer ticker.Stop();for{select{case<-ctx.Done():return;case now:=<-ticker.C:_,_=s.Store.RequeueExpired(ctx,now);for i:=0;i<64;i++{record,err:=s.Store.NextOutbox(ctx,now);if errors.Is(err,ErrNothingAvailable){break};if err!=nil{break};var env Envelope;if err:=json.Unmarshal(record.Payload,&env);err!=nil{_=s.Store.MarkOutboxDelivered(ctx,record.Seq,now);continue};t,err:=s.Registry.Get(record.Transport);if err!=nil||t.Publish(ctx,env)!=nil{break};_=s.Store.MarkOutboxDelivered(ctx,record.Seq,now)}}}}
