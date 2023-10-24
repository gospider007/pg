package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/gospider007/gson"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	conn *pgxpool.Pool
}
type ClientOption struct {
	Host string
	Port int
	Usr  string //用户名
	Pwd  string //密码
	Db   string //数据库名称
}

func (obj *Client) Close() {
	obj.conn.Close()
}
func NewClient(ctx context.Context, option ClientOption) (*Client, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if option.Host == "" {
		option.Host = "127.0.0.1"
	}
	if option.Port == 0 {
		option.Port = 5432
	}
	if option.Usr == "" {
		option.Usr = "postgres"
	}
	if option.Db == "" {
		option.Db = "postgres"
	}
	dataBaseUrl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", option.Usr, option.Pwd, option.Host, option.Port, option.Db)
	conn, err := pgxpool.New(ctx, dataBaseUrl)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
	}, conn.Ping(ctx)
}

type Rows struct {
	rows  pgx.Rows
	names []string
}
type Result struct {
	result pgconn.CommandTag
}

// 受影响的行数
func (obj *Result) RowsAffected() int64 {
	return obj.result.RowsAffected()
}

// 结果
func (obj *Result) String() string {
	return obj.result.String()
}

// 是否有下一个数据
func (obj *Rows) Next() bool {
	if obj.rows.Next() {
		return true
	} else {
		obj.Close()
		return false
	}
}

// 返回游标的数据
func (obj *Rows) Data() (map[string]any, error) {
	datas, err := obj.rows.Values()
	if err != nil {
		return nil, err
	}
	maprs := map[string]any{}
	for k, v := range datas {
		maprs[obj.names[k]] = v
	}
	return maprs, nil
}

// 返回游标的数据
func (obj *Rows) Json() (*gson.Client, error) {
	datas, err := obj.rows.Values()
	if err != nil {
		return nil, err
	}
	maprs := map[string]any{}
	for k, v := range datas {
		maprs[obj.names[k]] = v
	}
	return gson.Decode(maprs)
}

// 关闭游标
func (obj *Rows) Close() {
	obj.rows.Close()
}

// insert   $1  is args
func (obj *Client) Insert(ctx context.Context, table string, datas ...any) error {
	if ctx == nil {
		ctx = context.TODO()
	}
	for _, data := range datas {
		names := []string{}
		values := []any{}
		jsonData, err := gson.Decode(data)
		if err != nil {
			return err
		}
		for k, v := range jsonData.Map() {
			names = append(names, k)
			values = append(values, v.Value())
		}
		indexs := make([]string, len(names))
		for i := range names {
			indexs[i] = fmt.Sprintf("$%d", i+1)
		}
		if _, err := obj.Exec(ctx, fmt.Sprintf("insert into %s (%s) values (%s)", table, strings.Join(names, ","), strings.Join(indexs, ",")), values...); err != nil {
			return err
		}
	}
	return nil
}

// finds   $1  is args
func (obj *Client) Finds(ctx context.Context, query string, args ...any) (*Rows, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	row, err := obj.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	cols := row.FieldDescriptions()
	names := make([]string, len(cols))
	for coln, col := range cols {
		names[coln] = col.Name
	}
	return &Rows{
		names: names,
		rows:  row,
	}, err
}

// $1  is args  执行
func (obj *Client) Exec(ctx context.Context, query string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	exeResult, err := obj.conn.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}
