package pg

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"slices"
	"sort"
	"strings"

	"github.com/gospider007/bar"
	"github.com/gospider007/gson"
	"github.com/gospider007/re"
	"github.com/gospider007/thread"
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
	cnl   context.CancelFunc
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
func (obj *Rows) next() bool {
	if obj.rows.Next() {
		return true
	} else {
		obj.Close()
		return false
	}
}

func (obj *Rows) Range() iter.Seq[map[string]any] {
	return func(yield func(map[string]any) bool) {
		defer obj.Close()
		for obj.next() {
			if !yield(obj.data()) {
				return
			}
		}
	}
}

// 返回游标的数据
func (obj *Rows) data() map[string]any {
	datas, err := obj.rows.Values()
	if err != nil {
		obj.Close()
		return nil
	}
	maprs := map[string]any{}
	for k, v := range datas {
		maprs[obj.names[k]] = v
	}
	return maprs
}

// 关闭游标
func (obj *Rows) Close() {
	obj.cnl()
	obj.rows.Close()
}
func (obj *Client) parseInsertWithValues(values ...any) (string, string, []string, []any, error) {
	indexs := make([]string, len(values))
	vvs := []any{}
	keys := []string{}
	ti := 0

	for i, vs := range values {
		jsonData, err := gson.ParseRawMap(vs)
		if err != nil {
			return "", "", nil, nil, err
		}
		if i == 0 {
			for key := range jsonData {
				keys = append(keys, key)
			}
		}
		if len(keys) != len(jsonData) {
			return "", "", nil, nil, fmt.Errorf("the field name is not consistent")
		}
		index := make([]string, len(keys))
		for j, key := range keys {
			val, iok := jsonData[key]
			if !iok {
				return "", "", nil, nil, fmt.Errorf("the field name %s is empty", key)
			}
			index[j] = fmt.Sprintf("$%d", ti+1)
			ti++
			vvs = append(vvs, val)
		}
		indexs[i] = fmt.Sprintf("(%s)", strings.Join(index, ", "))
	}
	return strings.Join(keys, ", "), strings.Join(indexs, ", "), keys, vvs, nil
}

func (obj *Client) Upsert(ctx context.Context, table string, conflicts []string, datas ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	keys, indexs, names, values, err := obj.parseInsertWithValues(datas...)
	if err != nil {
		return nil, err
	}
	var query string
	if len(conflicts) > 0 {
		upKeys := []string{}
		for _, name := range names {
			if !slices.Contains(conflicts, name) {
				upKeys = append(upKeys, fmt.Sprintf("%s=EXCLUDED.%s", name, name))
			}
		}
		if len(upKeys) > 0 {
			query = fmt.Sprintf("insert into %s (%s) values %s on conflict (%s) do update set %s", table, keys, indexs, strings.Join(conflicts, ", "), strings.Join(upKeys, ", "))
		} else {
			query = fmt.Sprintf("insert into %s (%s) values %s on conflict (%s)", table, keys, indexs, strings.Join(conflicts, ", "))
		}
	} else {
		query = fmt.Sprintf("insert into %s (%s) values %s", table, keys, indexs)
	}
	return obj.Exec(ctx, query, values...)
}

// finds   $1  is args
func (obj *Client) Finds(preCtx context.Context, query string, args ...any) (*Rows, error) {
	if preCtx == nil {
		preCtx = context.TODO()
	}
	ctx, cnl := context.WithCancel(preCtx)
	row, err := obj.conn.Query(ctx, query, args...)
	if err != nil {
		cnl()
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
		cnl:   cnl,
	}, err
}

type Column struct {
	Position       int    `json:"position"`
	Name           string `json:"name"`
	Default        string `json:"default"`
	Type           string `json:"type"`
	Desc           string `json:"desc"`
	NotNull        bool   `json:"not_null"`
	Primary        bool   `json:"primary"`
	Unique         bool   `json:"unique"`
	ConstraintType string `json:"constrainttype"`
	Btree          bool   `json:"btree"`
	IndexGroup     int    `json:"index_group"`
}

func (obj *Client) CreateTable(ctx context.Context, table string, columns ...Column) error {
	if len(columns) == 0 {
		return nil
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Position < columns[j].Position
	})
	lines := []string{}
	primarys := map[int][]string{}
	uniques := map[int][]string{}
	btrees := map[int][]string{}
	blines := []string{}
	for _, col := range columns {
		line := fmt.Sprintf(`%s %s`, col.Name, col.Type)
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Default != "" && col.Default != "<nil>" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		lines = append(lines, line)
		if col.Primary {
			if primarys[col.IndexGroup] == nil {
				primarys[col.IndexGroup] = []string{col.Name}
			} else {
				primarys[col.IndexGroup] = append(primarys[col.IndexGroup], col.Name)
			}
		}
		if col.Unique {
			if uniques[col.IndexGroup] == nil {
				uniques[col.IndexGroup] = []string{col.Name}
			} else {
				uniques[col.IndexGroup] = append(uniques[col.IndexGroup], col.Name)
			}
		}
		if col.Btree {
			if btrees[col.IndexGroup] == nil {
				btrees[col.IndexGroup] = []string{col.Name}
			} else {
				btrees[col.IndexGroup] = append(btrees[col.IndexGroup], col.Name)
			}
		}
		if col.Desc != "" {
			blines = append(blines, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", table, col.Name, col.Desc))
		}
	}
	if len(primarys) > 1 {
		return fmt.Errorf("the table can only have one primary key")
	}
	for i, primary := range primarys {
		if i == 0 {
			if len(primary) > 1 {
				return fmt.Errorf("the primary key can only have one column")
			}
			lines = append(lines, fmt.Sprintf("PRIMARY KEY (%s)", primary[0]))
		} else {
			lines = append(lines, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primary, ", ")))
		}
	}
	for i, unique := range uniques {
		if i == 0 {
			for _, u := range unique {
				lines = append(lines, fmt.Sprintf("UNIQUE (%s)", u))
			}
		} else {
			lines = append(lines, fmt.Sprintf("UNIQUE (%s)", strings.Join(unique, ", ")))
		}
	}
	for i, btree := range btrees {
		if i == 0 {
			for _, b := range btree {
				blines = append(blines, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING btree (%s);", fmt.Sprintf("idx_%s_%s", table, b), table, b))
			}
		} else {
			blines = append(blines, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING btree (%s);", fmt.Sprintf("idx_%s_%s", table, strings.Join(btree, "_")), table, strings.Join(btree, ", ")))
		}
	}
	sql := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
%s
);`,
		table,
		"  "+strings.Join(lines, ",\n  "),
	)
	if len(blines) > 0 {
		sql += "\n" + strings.Join(blines, "\n")
	}
	_, err := obj.Exec(ctx, sql)
	return err
}

func (obj *Client) Fields(preCtx context.Context, table string) ([]Column, error) {
	if preCtx == nil {
		preCtx = context.TODO()
	}
	row, err := obj.Finds(preCtx, `SELECT
    c.ordinal_position                     AS position,
    c.column_name                          AS name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
    (c.is_nullable != 'YES')                AS not_null,
    pg_get_expr(ad.adbin, ad.adrelid)      AS default,
	tc.constraint_type As constrainttype,
    (tc.constraint_type = 'PRIMARY KEY')  AS primary,
    (tc.constraint_type = 'UNIQUE')       AS unique
FROM information_schema.columns c
JOIN pg_catalog.pg_class cl
    ON cl.relname = c.table_name
JOIN pg_catalog.pg_namespace ns
    ON ns.oid = cl.relnamespace
   AND ns.nspname = c.table_schema
JOIN pg_catalog.pg_attribute a
    ON a.attrelid = cl.oid
   AND a.attname = c.column_name
LEFT JOIN pg_catalog.pg_attrdef ad
    ON ad.adrelid = cl.oid
   AND ad.adnum = a.attnum
LEFT JOIN information_schema.key_column_usage kcu
    ON kcu.table_schema = c.table_schema
   AND kcu.table_name = c.table_name
   AND kcu.column_name = c.column_name
LEFT JOIN information_schema.table_constraints tc
    ON tc.constraint_schema = kcu.constraint_schema
   AND tc.constraint_name = kcu.constraint_name
WHERE c.table_schema = 'public'
  AND c.table_name = $1
ORDER BY c.ordinal_position;
`, table)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	datas := []Column{}
	for data := range row.Range() {
		var colum Column
		if _, err = gson.Decode(data, &colum); err != nil {
			return nil, err
		}
		datas = append(datas, colum)
	}
	return datas, nil
}
func (obj *Client) Count(ctx context.Context, tableName string, where string, args ...any) (int64, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	var query string
	if where == "" {
		query = fmt.Sprintf(`SELECT reltuples::BIGINT AS approx_rows FROM pg_class WHERE relname = '%s';`, tableName)
	} else {
		query = fmt.Sprintf(`select count(1) from %s where %s`, tableName, where)
	}
	row, err := obj.Find(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	rowData, err := gson.Decode(row)
	if err != nil {
		return 0, err
	}
	return rowData.Get("approx_rows").Int(), nil
}
func (obj *Client) Find(ctx context.Context, query string, args ...any) (map[string]any, error) {
	rows, err := obj.Finds(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	for row := range rows.Range() {
		return row, nil
	}
	return nil, nil
}

// $1  is args  执行
func (obj *Client) Exec(ctx context.Context, query string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	clearValues(args)
	exeResult, err := obj.conn.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}

// 执行
func (obj *Client) Update(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	return obj.update(ctx, table, data, false, where, args...)
}
func (obj *Client) UpdateOne(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	return obj.update(ctx, table, data, true, where, args...)
}
func ConverKey(key string) string {
	return pgx.Identifier{strings.ToLower(key)}.Sanitize()
}
func clearValues(vals []any) {
	for i, val := range vals {
		switch v := val.(type) {
		case string:
			vals[i] = strings.ReplaceAll(v, "\x00", "")
		default:
		}
	}
}
func (obj *Client) update(ctx context.Context, table string, data any, isOne bool, where string, args ...any) (*Result, error) {
	table = ConverKey(table)
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return nil, fmt.Errorf("where is empty")
	}
	jsonData, err := gson.Decode(data)
	if err != nil {
		return nil, err
	}
	names := []string{}
	values := []any{}
	i := 0
	for _, val := range args {
		values = append(values, val)
		i++
	}
	for key, val := range jsonData.Map() {
		names = append(names, fmt.Sprintf("%s=$%d", ConverKey(key), i+1))
		values = append(values, val.Value())
		i++
	}
	var query string
	if isOne {
		query = fmt.Sprintf("update %s set %s where ctid = (select ctid from %s where %s limit 1);", table, strings.Join(names, ", "), table, where)
	} else {
		query = fmt.Sprintf("update %s set %s where %s", table, strings.Join(names, ", "), where)
	}
	r, err := obj.Exec(ctx, query, values...)
	return r, err
}
func (obj *Client) Delete(ctx context.Context, table string, where string, args ...any) (*Result, error) {
	return obj.delete(ctx, table, false, where, args...)
}
func (obj *Client) DeleteOne(ctx context.Context, table string, where string, args ...any) (*Result, error) {
	return obj.delete(ctx, table, true, where, args...)
}
func (obj *Client) delete(ctx context.Context, table string, isOne bool, where string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return nil, fmt.Errorf("where is empty")
	}
	var queyr string
	if isOne {
		queyr = fmt.Sprintf("delete from %s where %s limit 1", table, where)
	} else {
		queyr = fmt.Sprintf("delete from %s where %s", table, where)
	}
	return obj.Exec(ctx, queyr, args...)
}

// 执行
func (obj *Client) Exists(ctx context.Context, table string, where string, args ...any) (bool, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return false, fmt.Errorf("where is empty")
	}
	exeResult, err := obj.Finds(ctx, fmt.Sprintf("select 1 from %s where %s limit 1", table, where), args...)
	if err != nil {
		return false, err
	}
	var exists bool
	for range exeResult.Range() {
		exists = true
		break
	}
	return exists, nil
}

type ClearOption struct {
	Thread int      //线程数量
	Init   bool     //是否初始化
	Oid    any      //起始id
	Show   []string //展示的字段
	Desc   bool     //是否倒序
	Where  string
	Args   []any
	Bar    bool //是否开启进度条
	Debug  bool //是否开启debug
}
type errStat string

const (
	ErrStatUnknow        errStat = "unknow error"
	ErrStatTableNoExists errStat = "table no exists"
	ErrStatDeadLock      errStat = "deadlock"
)

func ParseError(err error) errStat {
	if err == nil {
		return ErrStatUnknow
	}
	rs := re.Search(`\(SQLSTATE (.*?)\)`, err.Error())
	if rs == nil {
		return ErrStatUnknow
	}
	switch rs.Group(1) {
	case "42P01":
		return ErrStatTableNoExists
	case "40P01":
		return ErrStatDeadLock
	default:
		return ErrStatUnknow
	}
}

func (obj *Client) ClearTable(ctx context.Context, table string, indexName string, tag string, clearFn func(context.Context, map[string]any) error, options ...ClearOption) error {
	if ctx == nil {
		ctx = context.TODO()
	}
	var option ClearOption
	if len(options) > 0 {
		option = options[0]
	}
	if option.Thread == 0 {
		option.Thread = 10
	}
	var indexColum Column
	colums, err := obj.Fields(ctx, table)
	if err != nil {
		return err
	}
	for _, colum := range colums {
		if colum.Name == indexName {
			indexColum = colum
			break
		}
	}
	if indexColum.Name == "" {
		return errors.New("not found indexName")
	}
	logTableName := table + "_clear_log"
	logData, err := obj.Find(ctx, fmt.Sprintf("select total,current,oid from %s where tag=$1", logTableName), tag)
	if err != nil {
		if ParseError(err) == ErrStatTableNoExists {
			indexColum.Name = "oid"
			if err = obj.CreateTable(ctx, logTableName,
				indexColum,
				Column{
					Name: "total",
					Type: "bigint",
				},
				Column{
					Name: "current",
					Type: "bigint",
				},
				Column{
					Name:   "tag",
					Type:   "text",
					Unique: true,
				},
			); err != nil {
				return err
			}
			logData, err = obj.Find(ctx, fmt.Sprintf("select total,current,oid from %s where tag=$1", logTableName), tag)
		}
		if err != nil {
			return err
		}
		if option.Init {
			logData = map[string]any{}
		}
	}
	if len(logData) == 0 {
		logData = map[string]any{}
	}
	logJsonData, err := gson.Decode(logData)
	if err != nil {
		return err
	}
	if option.Oid == nil {
		if logJsonData.Get("oid").Exists() {
			option.Oid = logData["oid"]
		}
	} else {
		logData["oid"] = option.Oid
	}
	total, err := obj.Count(ctx, table, "")
	if err != nil {
		log.Panic(err)
	}
	logData["total"] = total
	logData["tag"] = tag
	current := logJsonData.Get("current").Int()
	var subWhere string
	subArgs := []any{}
	if option.Oid != nil {
		if option.Desc {
			subWhere = fmt.Sprintf("where %s<$1 ", indexName)
		} else {
			subWhere = fmt.Sprintf("where %s>$1 ", indexName)
		}
		subArgs = append(subArgs, option.Oid)
	}
	if option.Desc {
		subWhere += fmt.Sprintf("order by %s desc", indexName)
	} else {
		subWhere += fmt.Sprintf("order by %s asc", indexName)
	}
	var baseQuery string
	if len(option.Show) > 0 {
		baseQuery = fmt.Sprintf("select %s from %s %s", strings.Join(option.Show, ", "), table, subWhere)
	} else {
		baseQuery = fmt.Sprintf("select * from %s %s", table, subWhere)
	}
	rows, err := obj.Finds(ctx, baseQuery, subArgs...)
	if err != nil {
		return err
	}
	var lastOid any
	var barC *bar.Client
	if option.Bar {
		barC = bar.NewClient(total, bar.ClientOption{
			Cur: current,
		})
	}
	thC := thread.NewClient(ctx, option.Thread, thread.ClientOption{
		Debug: option.Debug, //是否显示调试信息
		TaskDoneCallBack: func(t *thread.Task) error {
			rss, terr := t.Result(1)
			if terr != nil {
				return terr
			}
			current++
			if barC != nil {
				barC.Print(current)
			}
			if current%100 == 0 {
				logData["current"] = current
				logData["oid"] = rss[0]
				_, terr = obj.Upsert(ctx, logTableName, []string{"tag"}, logData)
			}
			if terr == nil {
				lastOid = rss[0]
			}
			return terr
		},
	})
	for row := range rows.Range() {
		_, err = thC.Write(ctx, &thread.Task{
			Func: func(cctx context.Context, data map[string]any) (any, error) {
				indexValue, ok := data[indexName]
				if !ok {
					return nil, errors.New("not found indexName with data")
				}
				return indexValue, clearFn(cctx, data)
			},
			Args: []any{row},
		})
		if err != nil {
			break
		}
	}
	if err == nil {
		err = thC.JoinClose()
	}
	if lastOid != nil && current > 0 {
		logData["current"] = current
		logData["oid"] = lastOid
		_, ce := obj.Upsert(ctx, logTableName, []string{"tag"}, logData)
		if err == nil {
			err = ce
		}
	}
	return err
}
