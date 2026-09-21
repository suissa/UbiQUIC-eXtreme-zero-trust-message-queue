package ubiq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

var sqlIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type SQLShardBackend struct { id, driver, table, keyColumn string; db *sql.DB }
func OpenSQLShardBackend(id,driver,dsn,table,keyColumn string)(*SQLShardBackend,error){if !sqlIdentifier.MatchString(table)||!sqlIdentifier.MatchString(keyColumn){return nil,errors.New("unsafe SQL identifier")};db,err:=sql.Open(driver,dsn);if err!=nil{return nil,err};if err=db.Ping();err!=nil{db.Close();return nil,err};return &SQLShardBackend{id:id,driver:driver,table:table,keyColumn:keyColumn,db:db},nil}
func(b *SQLShardBackend)ID()string{return b.id}
func(b *SQLShardBackend)Close()error{return b.db.Close()}
func(b *SQLShardBackend)ph(i int)string{if strings.Contains(b.driver,"pgx"){return fmt.Sprintf("$%d",i)};return "?"}
func(b *SQLShardBackend)ExportRange(ctx context.Context,start,end,after string,limit int)(ShardBatch,error){args:=[]any{start,end};q:=fmt.Sprintf("SELECT * FROM %s WHERE %s >= %s AND %s < %s",b.table,b.keyColumn,b.ph(1),b.keyColumn,b.ph(2));if after!=""{args=append(args,after);q+=fmt.Sprintf(" AND %s > %s",b.keyColumn,b.ph(3))};args=append(args,limit);q+=fmt.Sprintf(" ORDER BY %s LIMIT %s",b.keyColumn,b.ph(len(args)));rows,err:=b.db.QueryContext(ctx,q,args...);if err!=nil{return ShardBatch{},err};defer rows.Close();cols,err:=rows.Columns();if err!=nil{return ShardBatch{},err};batch:=ShardBatch{};for rows.Next(){vals:=make([]any,len(cols));ptrs:=make([]any,len(cols));for i:=range vals{ptrs[i]=&vals[i]};if err:=rows.Scan(ptrs...);err!=nil{return ShardBatch{},err};doc:=make(map[string]any,len(cols));var key string;for i,col:=range cols{v:=vals[i];if data,ok:=v.([]byte);ok{v=string(data)};doc[col]=v;if col==b.keyColumn{key=fmt.Sprint(v)}};raw,err:=json.Marshal(doc);if err!=nil{return ShardBatch{},err};batch.Documents=append(batch.Documents,ShardDocument{Key:key,Data:raw});batch.Next=key};if err:=rows.Err();err!=nil{return ShardBatch{},err};batch.Done=len(batch.Documents)<limit;return batch,nil}
func(b *SQLShardBackend)Import(ctx context.Context,docs []ShardDocument)error{tx,err:=b.db.BeginTx(ctx,nil);if err!=nil{return err};defer tx.Rollback();for _,doc:=range docs{var fields map[string]any;if err:=json.Unmarshal(doc.Data,&fields);err!=nil{return err};cols:=make([]string,0,len(fields));for col:=range fields{if !sqlIdentifier.MatchString(col){return errors.New("unsafe imported column")};cols=append(cols,col)};sort.Strings(cols);vals:=make([]any,len(cols));ph:=make([]string,len(cols));for i,col:=range cols{vals[i]=fields[col];ph[i]=b.ph(i+1)};q:=fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",b.table,strings.Join(cols,","),strings.Join(ph,","),b.keyColumn);if _,err:=tx.ExecContext(ctx,q,vals...);err!=nil{return err}};return tx.Commit()}
func(b *SQLShardBackend)CountRange(ctx context.Context,start,end string)(int64,error){q:=fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s >= %s AND %s < %s",b.table,b.keyColumn,b.ph(1),b.keyColumn,b.ph(2));var n int64;err:=b.db.QueryRowContext(ctx,q,start,end).Scan(&n);return n,err}
func(b *SQLShardBackend)DeleteRange(ctx context.Context,start,end string)error{q:=fmt.Sprintf("DELETE FROM %s WHERE %s >= %s AND %s < %s",b.table,b.keyColumn,b.ph(1),b.keyColumn,b.ph(2));_,err:=b.db.ExecContext(ctx,q,start,end);return err}
