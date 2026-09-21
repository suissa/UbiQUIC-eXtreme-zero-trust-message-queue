package ubiq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcencoding "google.golang.org/grpc/encoding"
)

type grpcJSONCodec struct{}
func (grpcJSONCodec)Marshal(v any)([]byte,error){return json.Marshal(v)}
func (grpcJSONCodec)Unmarshal(data []byte,v any)error{return json.Unmarshal(data,v)}
func (grpcJSONCodec)Name()string{return "json"}
func init(){grpcencoding.RegisterCodec(grpcJSONCodec{})}
type GRPCConfig struct{Name,PeerAddr,ListenAddr string;ClientTLS,ServerTLS *tls.Config}
type grpcAck struct{Accepted bool `json:"accepted"`}
type grpcSubscribeRequest struct{Event string `json:"event"`}
type GRPCTransport struct{cfg GRPCConfig;server *grpc.Server;listener net.Listener;conn *grpc.ClientConn;mu sync.RWMutex;handlers map[string][]Handler}
func NewGRPCTransport(cfg GRPCConfig)(*GRPCTransport,error){if cfg.Name==""{return nil,errors.New("grpc name required")};t:=&GRPCTransport{cfg:cfg,handlers:make(map[string][]Handler)};if cfg.PeerAddr!=""{var creds credentials.TransportCredentials;if cfg.ClientTLS!=nil{creds=credentials.NewTLS(cfg.ClientTLS)}else{creds=insecure.NewCredentials()};conn,err:=grpc.NewClient(cfg.PeerAddr,grpc.WithTransportCredentials(creds),grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcJSONCodec{})));if err!=nil{return nil,err};t.conn=conn};if cfg.ListenAddr!=""{ln,err:=net.Listen("tcp",cfg.ListenAddr);if err!=nil{return nil,err};t.listener=ln;opts:=[]grpc.ServerOption{grpc.ForceServerCodec(grpcJSONCodec{})};if cfg.ServerTLS!=nil{opts=append(opts,grpc.Creds(credentials.NewTLS(cfg.ServerTLS)))};t.server=grpc.NewServer(opts...);t.server.RegisterService(&ubiqGRPCServiceDesc,t);go t.server.Serve(ln)};return t,nil}
func(t *GRPCTransport)Name()string{return t.cfg.Name}
func(t *GRPCTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Ordered:true,Bidirectional:true,Streaming:true}}
func(t *GRPCTransport)Publish(ctx context.Context,env Envelope)error{if t.conn==nil{return errors.New("grpc peer not configured")};if err:=env.Validate();err!=nil{return err};var ack grpcAck;return t.conn.Invoke(ctx,"/ubiq.v1.UbiQ/Publish",&env,&ack,grpc.ForceCodec(grpcJSONCodec{}))}
func(t *GRPCTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){if h==nil{return nil,errors.New("handler required")};if t.conn==nil{return t.addLocal(event,h),nil};desc:=&grpc.StreamDesc{ServerStreams:true};stream,err:=t.conn.NewStream(ctx,desc,"/ubiq.v1.UbiQ/Subscribe",grpc.ForceCodec(grpcJSONCodec{}));if err!=nil{return nil,err};if err:=stream.SendMsg(&grpcSubscribeRequest{Event:event});err!=nil{return nil,err};if err:=stream.CloseSend();err!=nil{return nil,err};subctx,cancel:=context.WithCancel(ctx);go func(){for{var env Envelope;if err:=stream.RecvMsg(&env);err!=nil{return};if h(subctx,env)!=nil{return}}}();return &funcSubscription{close:func()error{cancel();return nil}},nil}
func(t *GRPCTransport)addLocal(event string,h Handler)Subscription{t.mu.Lock();t.handlers[event]=append(t.handlers[event],h);idx:=len(t.handlers[event])-1;t.mu.Unlock();return &funcSubscription{close:func()error{t.mu.Lock();defer t.mu.Unlock();if idx<len(t.handlers[event]){t.handlers[event][idx]=nil};return nil}}}
func(t *GRPCTransport)dispatch(ctx context.Context,env Envelope)error{t.mu.RLock();specific:=append([]Handler(nil),t.handlers[env.Event]...);all:=append([]Handler(nil),t.handlers["*"]...);t.mu.RUnlock();for _,h:=range append(specific,all...){if h!=nil{if err:=h(ctx,env);err!=nil{return err}}};return nil}
func(t *GRPCTransport)Close()error{if t.server!=nil{t.server.GracefulStop()};if t.conn!=nil{return t.conn.Close()};if t.listener!=nil{return t.listener.Close()};return nil}
func grpcPublishHandler(srv any,ctx context.Context,dec func(any)error,interceptor grpc.UnaryServerInterceptor)(any,error){in:=new(Envelope);if err:=dec(in);err!=nil{return nil,err};handler:=func(ctx context.Context,req any)(any,error){env:=req.(*Envelope);if err:=env.Validate();err!=nil{return nil,err};if err:=srv.(*GRPCTransport).dispatch(ctx,*env);err!=nil{return nil,err};return &grpcAck{Accepted:true},nil};if interceptor==nil{return handler(ctx,in)};return interceptor(ctx,in,&grpc.UnaryServerInfo{Server:srv,FullMethod:"/ubiq.v1.UbiQ/Publish"},handler)}
func grpcSubscribeHandler(srv any,stream grpc.ServerStream)error{var req grpcSubscribeRequest;if err:=stream.RecvMsg(&req);err!=nil{return err};if req.Event==""{req.Event="*"};ch:=make(chan Envelope,64);sub:=srv.(*GRPCTransport).addLocal(req.Event,func(_ context.Context,e Envelope)error{select{case ch<-e:return nil;default:return errors.New("grpc subscriber backpressure")}});defer sub.Close();for{select{case<-stream.Context().Done():return stream.Context().Err();case env:=<-ch:if err:=stream.SendMsg(&env);err!=nil{return err}}}}
var ubiqGRPCServiceDesc=grpc.ServiceDesc{ServiceName:"ubiq.v1.UbiQ",HandlerType:(*interface{})(nil),Methods:[]grpc.MethodDesc{{MethodName:"Publish",Handler:grpcPublishHandler}},Streams:[]grpc.StreamDesc{{StreamName:"Subscribe",Handler:grpcSubscribeHandler,ServerStreams:true}},Metadata:"ubiq-json"}
