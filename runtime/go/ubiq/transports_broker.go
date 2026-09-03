package ubiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type funcSubscription struct { once sync.Once; close func() error }
func (s *funcSubscription) Close() error { var err error; s.once.Do(func(){ if s.close != nil { err=s.close() } }); return err }

func encodeEnvelope(env Envelope) ([]byte,error) { if err:=env.Validate(); err!=nil{return nil,err}; return json.Marshal(env) }
func decodeEnvelope(b []byte) (Envelope,error) { var env Envelope; if err:=json.Unmarshal(b,&env); err!=nil{return Envelope{},err}; return env,env.Validate() }
func canonicalSubject(prefix,event string) string { if prefix=="" {return event}; return strings.TrimSuffix(prefix,".")+"."+event }

type MemoryTransport struct { name string; mu sync.RWMutex; next uint64; subs map[uint64]memorySub }
type memorySub struct { event string; handler Handler }
func NewMemoryTransport(name string)*MemoryTransport{return &MemoryTransport{name:name,subs:make(map[uint64]memorySub)}}
func(m *MemoryTransport)Name()string{return m.name}
func(m *MemoryTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Ordered:true,Bidirectional:true,Streaming:true,ExecutionSettlement:true}}
func(m *MemoryTransport)Publish(ctx context.Context,env Envelope)error{if err:=env.Validate();err!=nil{return err};m.mu.RLock();defer m.mu.RUnlock();for _,sub:=range m.subs{if sub.event=="*"||sub.event==env.Event{if err:=sub.handler(ctx,env);err!=nil{return err}}};return nil}
func(m *MemoryTransport)Subscribe(_ context.Context,event string,h Handler)(Subscription,error){if h==nil{return nil,errors.New("handler required")};m.mu.Lock();id:=m.next;m.next++;m.subs[id]=memorySub{event:event,handler:h};m.mu.Unlock();return &funcSubscription{close:func()error{m.mu.Lock();delete(m.subs,id);m.mu.Unlock();return nil}},nil}
func(m *MemoryTransport)Close()error{m.mu.Lock();clear(m.subs);m.mu.Unlock();return nil}

type NATSConfig struct{Name,URL,Prefix string;JetStream bool;Stream,DurablePrefix string}
type NATSTransport struct{cfg NATSConfig;nc *nats.Conn;js nats.JetStreamContext}
func NewNATSTransport(cfg NATSConfig)(*NATSTransport,error){nc,err:=nats.Connect(cfg.URL,nats.Name("UbiQ:"+cfg.Name));if err!=nil{return nil,err};t:=&NATSTransport{cfg:cfg,nc:nc};if cfg.JetStream{js,err:=nc.JetStream();if err!=nil{nc.Close();return nil,err};t.js=js;stream:=cfg.Stream;if stream==""{stream="UBIQ_EVENTS"};subject:=canonicalSubject(cfg.Prefix,">");if _,err:=js.StreamInfo(stream);err!=nil{if _,addErr:=js.AddStream(&nats.StreamConfig{Name:stream,Subjects:[]string{subject},Storage:nats.FileStorage});addErr!=nil{nc.Close();return nil,addErr}}};return t,nil}
func(t *NATSTransport)Name()string{return t.cfg.Name}
func(t *NATSTransport)Capabilities()Capabilities{if t.cfg.JetStream{return Capabilities{Reliable:true,Durable:true,Replay:true,Bidirectional:true,ConsumerGroups:true,Streaming:true,ExecutionSettlement:true}};return Capabilities{Reliable:true,Bidirectional:true,Streaming:true}}
func(t *NATSTransport)Publish(ctx context.Context,env Envelope)error{raw,err:=encodeEnvelope(env);if err!=nil{return err};subject:=canonicalSubject(t.cfg.Prefix,env.Event);if t.cfg.JetStream{_,err=t.js.Publish(subject,raw,nats.Context(ctx));return err};if err=t.nc.Publish(subject,raw);err!=nil{return err};return t.nc.FlushWithContext(ctx)}
func(t *NATSTransport)Subscribe(_ context.Context,event string,h Handler)(Subscription,error){subject:=canonicalSubject(t.cfg.Prefix,event);cb:=func(msg *nats.Msg){env,err:=decodeEnvelope(msg.Data);if err!=nil{return};if err=h(context.Background(),env);err==nil&&t.cfg.JetStream{_=msg.Ack()}};if t.cfg.JetStream{opts:=[]nats.SubOpt{nats.ManualAck(),nats.AckExplicit()};if t.cfg.DurablePrefix!=""{opts=append(opts,nats.Durable(t.cfg.DurablePrefix+"-"+sanitizeName(event)))};sub,err:=t.js.Subscribe(subject,cb,opts...);if err!=nil{return nil,err};return &funcSubscription{close:sub.Unsubscribe},nil};sub,err:=t.nc.Subscribe(subject,cb);if err!=nil{return nil,err};return &funcSubscription{close:sub.Unsubscribe},nil}
func(t *NATSTransport)Close()error{t.nc.Close();return nil}
func sanitizeName(s string)string{return strings.NewReplacer(".","-","*","all",">","all","_","-").Replace(s)}

type KafkaConfig struct{Name string;Brokers []string;Topic,Group string}
type KafkaTransport struct{cfg KafkaConfig;writer *kafka.Writer;mu sync.Mutex;readers []*kafka.Reader}
func NewKafkaTransport(cfg KafkaConfig)(*KafkaTransport,error){if len(cfg.Brokers)==0{return nil,errors.New("kafka brokers required")};if cfg.Topic==""{cfg.Topic="ubiq-events"};w:=&kafka.Writer{Addr:kafka.TCP(cfg.Brokers...),Topic:cfg.Topic,RequiredAcks:kafka.RequireAll,Balancer:&kafka.Hash{}};return &KafkaTransport{cfg:cfg,writer:w},nil}
func(t *KafkaTransport)Name()string{return t.cfg.Name}
func(t *KafkaTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Ordered:true,Durable:true,Replay:true,ConsumerGroups:true,Streaming:true,ExecutionSettlement:true}}
func(t *KafkaTransport)Publish(ctx context.Context,env Envelope)error{raw,err:=encodeEnvelope(env);if err!=nil{return err};return t.writer.WriteMessages(ctx,kafka.Message{Key:[]byte(env.Event),Value:raw,Headers:[]kafka.Header{{Key:"ubiq-event",Value:[]byte(env.Event)}}})}
func(t *KafkaTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){group:=t.cfg.Group;if group==""{group="ubiq-"+sanitizeName(event)};r:=kafka.NewReader(kafka.ReaderConfig{Brokers:t.cfg.Brokers,Topic:t.cfg.Topic,GroupID:group,MinBytes:1,MaxBytes:10e6});t.mu.Lock();t.readers=append(t.readers,r);t.mu.Unlock();subctx,cancel:=context.WithCancel(ctx);go func(){for{msg,err:=r.FetchMessage(subctx);if err!=nil{return};env,err:=decodeEnvelope(msg.Value);if err!=nil{continue};if event!="*"&&env.Event!=event{_=r.CommitMessages(subctx,msg);continue};if h(subctx,env)==nil{_=r.CommitMessages(subctx,msg)}}}();return &funcSubscription{close:func()error{cancel();return r.Close()}},nil}
func(t *KafkaTransport)Close()error{t.mu.Lock();defer t.mu.Unlock();for _,r:=range t.readers{_=r.Close()};return t.writer.Close()}

type RabbitConfig struct{Name,URL,Exchange,QueuePrefix string}
type RabbitTransport struct{cfg RabbitConfig;conn *amqp.Connection;ch *amqp.Channel}
func NewRabbitTransport(cfg RabbitConfig)(*RabbitTransport,error){conn,err:=amqp.Dial(cfg.URL);if err!=nil{return nil,err};ch,err:=conn.Channel();if err!=nil{conn.Close();return nil,err};if cfg.Exchange==""{cfg.Exchange="ubiq"};if err=ch.ExchangeDeclare(cfg.Exchange,"topic",true,false,false,false,nil);err!=nil{ch.Close();conn.Close();return nil,err};return &RabbitTransport{cfg:cfg,conn:conn,ch:ch},nil}
func(t *RabbitTransport)Name()string{return t.cfg.Name}
func(t *RabbitTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Durable:true,Bidirectional:true,ConsumerGroups:true,ExecutionSettlement:true}}
func(t *RabbitTransport)Publish(ctx context.Context,env Envelope)error{raw,err:=encodeEnvelope(env);if err!=nil{return err};return t.ch.PublishWithContext(ctx,t.cfg.Exchange,env.Event,false,false,amqp.Publishing{ContentType:"application/json",DeliveryMode:amqp.Persistent,MessageId:env.ID,CorrelationId:env.CorrelationID,Body:raw})}
func(t *RabbitTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){qname:=t.cfg.QueuePrefix;if qname!=""{qname+="-"+sanitizeName(event)};q,err:=t.ch.QueueDeclare(qname,qname!="",qname=="",qname=="",false,nil);if err!=nil{return nil,err};key:=event;if key=="*"{key="#"};if err=t.ch.QueueBind(q.Name,key,t.cfg.Exchange,false,nil);err!=nil{return nil,err};deliveries,err:=t.ch.Consume(q.Name,"",false,false,false,false,nil);if err!=nil{return nil,err};subctx,cancel:=context.WithCancel(ctx);go func(){for{select{case<-subctx.Done():return;case d,ok:=<-deliveries:if !ok{return};env,err:=decodeEnvelope(d.Body);if err!=nil{_=d.Nack(false,false);continue};if h(subctx,env)==nil{_=d.Ack(false)}else{_=d.Nack(false,true)}}}}();return &funcSubscription{close:func()error{cancel();return nil}},nil}
func(t *RabbitTransport)Close()error{_=t.ch.Close();return t.conn.Close()}

type RedisStreamsConfig struct{Name,Addr,Password,Stream,Group string;DB int}
type RedisStreamsTransport struct{cfg RedisStreamsConfig;client *redis.Client}
func NewRedisStreamsTransport(cfg RedisStreamsConfig)*RedisStreamsTransport{if cfg.Stream==""{cfg.Stream="ubiq-events"};if cfg.Group==""{cfg.Group="ubiq"};return &RedisStreamsTransport{cfg:cfg,client:redis.NewClient(&redis.Options{Addr:cfg.Addr,Password:cfg.Password,DB:cfg.DB})}}
func(t *RedisStreamsTransport)Name()string{return t.cfg.Name}
func(t *RedisStreamsTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Ordered:true,Durable:true,Replay:true,ConsumerGroups:true,Streaming:true,ExecutionSettlement:true}}
func(t *RedisStreamsTransport)Publish(ctx context.Context,env Envelope)error{raw,err:=encodeEnvelope(env);if err!=nil{return err};return t.client.XAdd(ctx,&redis.XAddArgs{Stream:t.cfg.Stream,Values:map[string]any{"event":env.Event,"envelope":string(raw)}}).Err()}
func(t *RedisStreamsTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){_=t.client.XGroupCreateMkStream(ctx,t.cfg.Stream,t.cfg.Group,"0").Err();consumer:="ubiq-"+sanitizeName(event)+fmt.Sprintf("-%d",time.Now().UnixNano());subctx,cancel:=context.WithCancel(ctx);go func(){for{subs,err:=t.client.XReadGroup(subctx,&redis.XReadGroupArgs{Group:t.cfg.Group,Consumer:consumer,Streams:[]string{t.cfg.Stream,">"},Count:32,Block:time.Second}).Result();if err!=nil{if errors.Is(err,context.Canceled){return};continue};for _,stream:=range subs{for _,msg:=range stream.Messages{raw,ok:=msg.Values["envelope"].(string);if !ok{continue};env,err:=decodeEnvelope([]byte(raw));if err!=nil{continue};if event!="*"&&env.Event!=event{_=t.client.XAck(subctx,t.cfg.Stream,t.cfg.Group,msg.ID).Err();continue};if h(subctx,env)==nil{_=t.client.XAck(subctx,t.cfg.Stream,t.cfg.Group,msg.ID).Err()}}}}}();return &funcSubscription{close:func()error{cancel();return nil}},nil}
func(t *RedisStreamsTransport)Close()error{return t.client.Close()}

type MQTTConfig struct{Name,Broker,ClientID,Prefix string;Username,Password string}
type MQTTTransport struct{cfg MQTTConfig;client mqtt.Client}
func NewMQTTTransport(cfg MQTTConfig)(*MQTTTransport,error){opts:=mqtt.NewClientOptions().AddBroker(cfg.Broker).SetClientID(cfg.ClientID).SetAutoReconnect(true);if cfg.Username!=""{opts.SetUsername(cfg.Username);opts.SetPassword(cfg.Password)};c:=mqtt.NewClient(opts);tok:=c.Connect();if !tok.WaitTimeout(15*time.Second){return nil,errors.New("mqtt connect timeout")};if err:=tok.Error();err!=nil{return nil,err};return &MQTTTransport{cfg:cfg,client:c},nil}
func(t *MQTTTransport)Name()string{return t.cfg.Name}
func(t *MQTTTransport)Capabilities()Capabilities{return Capabilities{Reliable:true,Ordered:true,Bidirectional:true,Streaming:true}}
func(t *MQTTTransport)Publish(_ context.Context,env Envelope)error{raw,err:=encodeEnvelope(env);if err!=nil{return err};tok:=t.client.Publish(strings.ReplaceAll(canonicalSubject(t.cfg.Prefix,env.Event),".","/"),1,false,raw);tok.Wait();return tok.Error()}
func(t *MQTTTransport)Subscribe(ctx context.Context,event string,h Handler)(Subscription,error){topic:=strings.ReplaceAll(canonicalSubject(t.cfg.Prefix,event),".","/");if event=="*"{topic=strings.TrimSuffix(strings.ReplaceAll(t.cfg.Prefix,".","/"),"/")+"/#"};tok:=t.client.Subscribe(topic,1,func(_ mqtt.Client,msg mqtt.Message){env,err:=decodeEnvelope(msg.Payload());if err==nil{_=h(ctx,env)}});tok.Wait();if err:=tok.Error();err!=nil{return nil,err};return &funcSubscription{close:func()error{u:=t.client.Unsubscribe(topic);u.Wait();return u.Error()}},nil}
func(t *MQTTTransport)Close()error{t.client.Disconnect(250);return nil}
