package ubiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type ClusterNode struct { ID string `json:"id"`; Address string `json:"address"`; Healthy bool `json:"healthy"`; LastHeartbeatMS int64 `json:"last_heartbeat_ms"` }
type clusterState struct { Epoch uint64 `json:"epoch"`; Nodes map[string]ClusterNode `json:"nodes"` }
type clusterCommand struct { Op string `json:"op"`; Node ClusterNode `json:"node"` }
type clusterFSM struct { mu sync.RWMutex; state clusterState }
func newClusterFSM()*clusterFSM{return &clusterFSM{state:clusterState{Nodes:make(map[string]ClusterNode)}}}
func(f *clusterFSM)Apply(log *raft.Log)interface{}{var cmd clusterCommand;if err:=json.Unmarshal(log.Data,&cmd);err!=nil{return err};f.mu.Lock();defer f.mu.Unlock();switch cmd.Op{case"upsert":f.state.Nodes[cmd.Node.ID]=cmd.Node;f.state.Epoch++;case"delete":delete(f.state.Nodes,cmd.Node.ID);f.state.Epoch++;default:return fmt.Errorf("unknown cluster op %q",cmd.Op)};return nil}
func(f *clusterFSM)Snapshot()(raft.FSMSnapshot,error){f.mu.RLock();defer f.mu.RUnlock();raw,err:=json.Marshal(f.state);return &clusterSnapshot{raw:raw},err}
func(f *clusterFSM)Restore(rc io.ReadCloser)error{defer rc.Close();var state clusterState;if err:=json.NewDecoder(rc).Decode(&state);err!=nil{return err};if state.Nodes==nil{state.Nodes=make(map[string]ClusterNode)};f.mu.Lock();f.state=state;f.mu.Unlock();return nil}
func(f *clusterFSM)copyState()clusterState{f.mu.RLock();defer f.mu.RUnlock();out:=clusterState{Epoch:f.state.Epoch,Nodes:make(map[string]ClusterNode,len(f.state.Nodes))};for k,v:=range f.state.Nodes{out.Nodes[k]=v};return out}
type clusterSnapshot struct{raw []byte}
func(s *clusterSnapshot)Persist(sink raft.SnapshotSink)error{if _,err:=sink.Write(s.raw);err!=nil{_=sink.Cancel();return err};return sink.Close()}
func(s *clusterSnapshot)Release(){}

type RaftCluster struct{raft *raft.Raft;fsm *clusterFSM;transport *raft.NetworkTransport}
func OpenRaftCluster(nodeID,bindAddr,dataDir string,bootstrap bool)(*RaftCluster,error){if nodeID==""||bindAddr==""||dataDir==""{return nil,errors.New("nodeID, bindAddr and dataDir are required")};if err:=os.MkdirAll(dataDir,0o700);err!=nil{return nil,err};cfg:=raft.DefaultConfig();cfg.LocalID=raft.ServerID(nodeID);cfg.SnapshotInterval=30*time.Second;cfg.SnapshotThreshold=256;logStore,err:=raftboltdb.NewBoltStore(filepath.Join(dataDir,"raft-log.bolt"));if err!=nil{return nil,err};stableStore,err:=raftboltdb.NewBoltStore(filepath.Join(dataDir,"raft-stable.bolt"));if err!=nil{return nil,err};snapshots,err:=raft.NewFileSnapshotStore(dataDir,3,os.Stderr);if err!=nil{return nil,err};transport,err:=raft.NewTCPTransport(bindAddr,nil,5,10*time.Second,os.Stderr);if err!=nil{return nil,err};fsm:=newClusterFSM();r,err:=raft.NewRaft(cfg,fsm,logStore,stableStore,snapshots,transport);if err!=nil{return nil,err};hasState,err:=raft.HasExistingState(logStore,stableStore,snapshots);if err!=nil{return nil,err};if bootstrap&&!hasState{future:=r.BootstrapCluster(raft.Configuration{Servers:[]raft.Server{{ID:raft.ServerID(nodeID),Address:transport.LocalAddr(),Suffrage:raft.Voter}}});if err:=future.Error();err!=nil{return nil,err}};return &RaftCluster{raft:r,fsm:fsm,transport:transport},nil}
func(c *RaftCluster)Close()error{return c.raft.Shutdown().Error()}
func(c *RaftCluster)IsLeader()bool{return c.raft.State()==raft.Leader}
func(c *RaftCluster)Leader()string{return string(c.raft.Leader())}
func(c *RaftCluster)Join(id,address string)error{if !c.IsLeader(){return errors.New("join must be sent to leader")};future:=c.raft.GetConfiguration();if err:=future.Error();err!=nil{return err};for _,srv:=range future.Configuration().Servers{if string(srv.ID)==id||string(srv.Address)==address{if string(srv.ID)==id&&string(srv.Address)==address{return nil};if err:=c.raft.RemoveServer(srv.ID,0,10*time.Second).Error();err!=nil{return err}}};if err:=c.raft.AddVoter(raft.ServerID(id),raft.ServerAddress(address),0,10*time.Second).Error();err!=nil{return err};return c.UpsertNode(ClusterNode{ID:id,Address:address,Healthy:true,LastHeartbeatMS:time.Now().UnixMilli()})}
func(c *RaftCluster)Remove(id string)error{if !c.IsLeader(){return errors.New("remove must be sent to leader")};if err:=c.raft.RemoveServer(raft.ServerID(id),0,10*time.Second).Error();err!=nil{return err};raw,_:=json.Marshal(clusterCommand{Op:"delete",Node:ClusterNode{ID:id}});return c.raft.Apply(raw,10*time.Second).Error()}
func(c *RaftCluster)UpsertNode(node ClusterNode)error{if !c.IsLeader(){return errors.New("upsert must be sent to leader")};raw,err:=json.Marshal(clusterCommand{Op:"upsert",Node:node});if err!=nil{return err};return c.raft.Apply(raw,10*time.Second).Error()}
func(c *RaftCluster)Heartbeat(id,address string)error{return c.UpsertNode(ClusterNode{ID:id,Address:address,Healthy:true,LastHeartbeatMS:time.Now().UnixMilli()})}
func(c *RaftCluster)ReapStale(maxAge time.Duration)error{if !c.IsLeader(){return errors.New("reap must run on leader")};state:=c.fsm.copyState();cutoff:=time.Now().Add(-maxAge).UnixMilli();for _,node:=range state.Nodes{if node.Healthy&&node.LastHeartbeatMS<cutoff{node.Healthy=false;if err:=c.UpsertNode(node);err!=nil{return err}}};return nil}
func(c *RaftCluster)State()(uint64,[]ClusterNode){s:=c.fsm.copyState();nodes:=make([]ClusterNode,0,len(s.Nodes));for _,n:=range s.Nodes{nodes=append(nodes,n)};sort.Slice(nodes,func(i,j int)bool{return nodes[i].ID<nodes[j].ID});return s.Epoch,nodes}
type ReplicaPlacement struct{Epoch uint64 `json:"epoch"`;Shard uint32 `json:"shard"`;Nodes []ClusterNode `json:"nodes"`}
func(c *RaftCluster)Placement(key string,shardCount uint32,replication int)(ReplicaPlacement,error){if shardCount==0||replication<=0{return ReplicaPlacement{},errors.New("invalid shard/replication values")};epoch,nodes:=c.State();healthy:=nodes[:0];for _,n:=range nodes{if n.Healthy{healthy=append(healthy,n)}};if len(healthy)<replication{return ReplicaPlacement{},errors.New("not enough healthy nodes")};type scored struct{n ClusterNode;score uint64};scores:=make([]scored,0,len(healthy));for _,n:=range healthy{scores=append(scores,scored{n:n,score:rendezvousScore(key,n.ID)})};sort.Slice(scores,func(i,j int)bool{return scores[i].score>scores[j].score});selected:=make([]ClusterNode,replication);for i:=0;i<replication;i++{selected[i]=scores[i].n};return ReplicaPlacement{Epoch:epoch,Shard:uint32(fnv64(key)%uint64(shardCount)),Nodes:selected},nil}
func fnv64(s string)uint64{h:=uint64(14695981039346656037);for i:=0;i<len(s);i++{h^=uint64(s[i]);h*=1099511628211};return h}
func rendezvousScore(key,node string)uint64{return fnv64(key+"\x00"+node)}
type ReplicaApply struct{Shard uint32 `json:"shard"`;Key string `json:"key"`;FencingToken uint64 `json:"fencing_token"`;Payload json.RawMessage `json:"payload"`}
func Replicate(ctx context.Context,t Transport,apply ReplicaApply,correlation string)error{raw,err:=json.Marshal(apply);if err!=nil{return err};env:=Envelope{ID:fmt.Sprintf("replica-%d-%d",time.Now().UnixNano(),apply.Shard),Event:"System.Replica.apply.request",CorrelationID:correlation,CausationID:correlation,IdempotencyKey:fmt.Sprintf("replica:%d:%s:%d",apply.Shard,apply.Key,apply.FencingToken),SchemaID:"System.Replica.apply.request@1",Payload:raw,Guarantee:GuaranteeProcessed,CreatedAtMS:time.Now().UnixMilli()};return t.Publish(ctx,env)}
