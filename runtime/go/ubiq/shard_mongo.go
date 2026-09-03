package ubiq

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoShardBackend struct { id string; keyField string; client *mongo.Client; collection *mongo.Collection }
func OpenMongoShardBackend(ctx context.Context,id,uri,database,collection,keyField string)(*MongoShardBackend,error){if id==""||uri==""||database==""||collection==""||keyField==""{return nil,errors.New("invalid Mongo shard config")};client,err:=mongo.Connect(ctx,options.Client().ApplyURI(uri));if err!=nil{return nil,err};if err=client.Ping(ctx,nil);err!=nil{client.Disconnect(ctx);return nil,err};return &MongoShardBackend{id:id,keyField:keyField,client:client,collection:client.Database(database).Collection(collection)},nil}
func(b *MongoShardBackend)ID()string{return b.id}
func(b *MongoShardBackend)Close(ctx context.Context)error{return b.client.Disconnect(ctx)}
func(b *MongoShardBackend)rangeFilter(start,end,after string)bson.M{rangeQ:=bson.M{"$gte":start,"$lt":end};if after!=""{rangeQ["$gt"]=after};return bson.M{b.keyField:rangeQ}}
func(b *MongoShardBackend)ExportRange(ctx context.Context,start,end,after string,limit int)(ShardBatch,error){cur,err:=b.collection.Find(ctx,b.rangeFilter(start,end,after),options.Find().SetSort(bson.D{{Key:b.keyField,Value:1}}).SetLimit(int64(limit)));if err!=nil{return ShardBatch{},err};defer cur.Close(ctx);batch:=ShardBatch{};for cur.Next(ctx){var doc bson.M;if err:=cur.Decode(&doc);err!=nil{return ShardBatch{},err};key:=fmt.Sprint(doc[b.keyField]);raw,err:=bson.MarshalExtJSON(doc,true,false);if err!=nil{return ShardBatch{},err};batch.Documents=append(batch.Documents,ShardDocument{Key:key,Data:raw});batch.Next=key};if err:=cur.Err();err!=nil{return ShardBatch{},err};batch.Done=len(batch.Documents)<limit;return batch,nil}
func(b *MongoShardBackend)Import(ctx context.Context,docs []ShardDocument)error{for _,item:=range docs{var doc bson.M;if err:=bson.UnmarshalExtJSON(item.Data,true,&doc);err!=nil{return err};key,ok:=doc[b.keyField];if !ok{return fmt.Errorf("document missing shard key %q",b.keyField)};if _,err:=b.collection.ReplaceOne(ctx,bson.M{b.keyField:key},doc,options.Replace().SetUpsert(true));err!=nil{return err}};return nil}
func(b *MongoShardBackend)CountRange(ctx context.Context,start,end string)(int64,error){return b.collection.CountDocuments(ctx,b.rangeFilter(start,end,""))}
func(b *MongoShardBackend)DeleteRange(ctx context.Context,start,end string)error{_,err:=b.collection.DeleteMany(ctx,b.rangeFilter(start,end,""));return err}
