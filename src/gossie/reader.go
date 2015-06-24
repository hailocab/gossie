package gossie

import (
	"errors"
	"fmt"
	"github.com/hailocab/gossie/src/cassandra"
	"time"
)

/*
	to do:
	support Where for RangeGet in Cassandra 1.1
	figure out what's the deal with get_paged_slice in 1.1 and try to implement it in a sane way
*/

// Columns encapsulate the individual columns from/to Cassandra reads and writes
type Column struct {
	Name      []byte
	Value     []byte
	Ttl       int32
	Timestamp int64
}

// Row is a Cassandra row, including its row key
type Row struct {
	Key     []byte
	Columns []*Column
}

// RowColumnCount stores the number of columns matched in a MultiCount reader
type RowColumnCount struct {
	Key   []byte
	Count int
}

// Slice allows to specify a range of columns to return
// Always specify a Count value since there is an interface-mandated default of 100.
type Slice struct {
	Start    []byte
	End      []byte
	Count    int
	Reversed bool
}

// Range represents a range of rows to return, in order to be able to iterate over their keys.
// The low level token range is not exposed. Use an empty slice to indicate if you want the first
// or the last possible key in a range then pass the last read row key as the new Start key in a
// new RangeGet reader to page results. This will allow you to iterate over an entire CF even when
// using the random partitioner. Always specify a Count value since there is an interface-mandated
// default of 100.
type Range struct {
	Start []byte
	End   []byte
	Count int
}

// IndexedRange represents a range of rows to return for the IndexedGet method.
// The low level token range is not exposed. Use an empty slice to indicate if you want the first key
// in a range, then pass the last read row key as the new Start key in a new IndexedGet reader to page
// results. Always specify a Count value since there is an interface-mandated default of 100.
type IndexedRange struct {
	Start []byte
	Count int
}

// Operator for Where
type Operator int

const (
	EQ  Operator = 0
	GTE Operator = 1
	GT  Operator = 2
	LTE Operator = 3
	LT  Operator = 4
)

// Reader is the interface for all read operations over Cassandra.
// The method calls support chaining so you can build concise queries
type Reader interface {

	// ConsistencyLevel sets the consistency level for this particular call.
	// It is optional, if left uncalled it will default to your connection pool options value.
	ConsistencyLevel(int) Reader

	// Cf sets the column family name for the reader.
	// This method must be always called.
	Cf(string) Reader

	// Slice optionally sets a slice to set a range of column names and potentially iterate over the
	// columns of the returned row(s)
	Slice(*Slice) Reader

	// Columns optionally filters the returned columns to only the passed set of column names
	Columns([][]byte) Reader

	// Each call to this method adds a new comparison to be checked against the returned rows of
	// IndexedGet
	// All the comparisons are checked for every row. In the current Cassandra implementation at
	// least one of the Where calls must use a secondary indexed column with an EQ operator.
	Where(column []byte, op Operator, value []byte) Reader

	// Get looks up a row with the given key and returns it, or nil in case it is not found
	Get(key []byte) (*Row, error)

	// MultiGet performs a parallel Get operation for all the passed keys, and returns a slice of
	// RowColumnCounts pointers to the gathered rows, which may be empty if none were found. It returns
	// nil only on error conditions
	MultiGet(keys [][]byte) ([]*Row, error)

	// Count looks up a row with the given key and returns the number of columns it has
	Count(key []byte) (int, error)

	// MultiGet performs a parallel Count operation for all the passed keys, and returns a slice of Row
	// pointers to the gathered rows, which may be empty if none were found. It returns nil only on
	// error conditions
	MultiCount(keys [][]byte) ([]*RowColumnCount, error)

	// RangeGet performs a sequential Get operation for a range of rows. See the docs for Range for an
	// explanation on how to page results. It returns a slice of Row pointers to the gathered rows, which
	// may be empty if none were found. It returns nil only on error conditions
	RangeGet(*Range) ([]*Row, error)

	// IndexedGet performs a sequential Get operation for a range of rows and returns only those that match
	// the Where clauses. See the docs for Range for an explanation on how to page results. It returns a
	// slice of Row pointers to the gathered rows, which may be empty if none were found. It returns nil only
	// on error conditions
	IndexedGet(*IndexedRange) ([]*Row, error)
}

type reader struct {
	pool             *connectionPool
	consistencyLevel int
	cf               string
	slice            Slice
	setSlice         bool
	columns          [][]byte
	setColumns       bool
	setWhere         bool
	expressions      []*cassandra.IndexExpression
}

func newReader(cp *connectionPool, cl int) *reader {
	return &reader{
		pool:             cp,
		consistencyLevel: cl,
	}
}

func (r *reader) ConsistencyLevel(l int) Reader {
	r.consistencyLevel = l
	return r
}

func (r *reader) Cf(cf string) Reader {
	r.cf = cf
	return r
}

func (r *reader) Slice(s *Slice) Reader {
	r.slice = *s
	r.setSlice = true
	return r
}

func (r *reader) Columns(c [][]byte) Reader {
	r.columns = make([][]byte, len(c))
	copy(r.columns, c)
	r.setColumns = true
	return r
}

func (r *reader) Where(column []byte, op Operator, value []byte) Reader {
	if r.expressions == nil {
		r.expressions = make([]*cassandra.IndexExpression, 0)
	}
	exp := cassandra.NewIndexExpression()
	exp.ColumnName = column
	exp.Op = cassandra.IndexOperator(op)
	exp.Value = value
	r.expressions = append(r.expressions, exp)
	r.setWhere = true
	return r
}

func sliceToCassandra(slice *Slice) *cassandra.SliceRange {
	sr := cassandra.NewSliceRange()
	sr.Start = slice.Start
	sr.Finish = slice.End
	if slice.Count != 0 {
		sr.Count = int32(slice.Count)
	}
	sr.Reversed = slice.Reversed
	// workaround some uninitialized slice == nil quirks that trickle down into the generated thrift4go code
	if sr.Start == nil {
		sr.Start = make([]byte, 0)
	}
	if sr.Finish == nil {
		sr.Finish = make([]byte, 0)
	}
	return sr
}

func fullSlice() *cassandra.SliceRange {
	sr := cassandra.NewSliceRange()
	// workaround some uninitialized slice == nil quirks that trickle down into the generated thrift4go code
	sr.Start = make([]byte, 0)
	sr.Finish = make([]byte, 0)
	return sr
}

func (r *reader) buildPredicate() *cassandra.SlicePredicate {
	sp := cassandra.NewSlicePredicate()
	if r.setColumns {
		colArr := make([][]byte, 0)
		for _, col := range r.columns {
			colArr = append(colArr, col)
		}
		sp.ColumnNames = &colArr
	} else if r.setSlice {
		sp.SliceRange = sliceToCassandra(&r.slice)
	} else {
		sp.SliceRange = fullSlice()
	}
	return sp
}

func (r *reader) buildColumnParent() *cassandra.ColumnParent {
	cp := cassandra.NewColumnParent()
	cp.ColumnFamily = r.cf
	return cp
}

func (q *reader) buildKeyRange(r *Range) *cassandra.KeyRange {
	kr := cassandra.NewKeyRange()
	if r.Start != nil {
		kr.StartKey = &r.Start
	}
	if r.End != nil {
		kr.EndKey = &r.End
	}
	kr.Count = int32(r.Count)
	// workaround some uninitialized slice == nil quirks that trickle down into the generated thrift4go code
	if kr.StartKey == nil {
		sKeyArr := make([]byte, 0)
		kr.StartKey = &sKeyArr
	}
	if kr.EndKey == nil {
		eKeyArr := make([]byte, 0)
		kr.EndKey = &eKeyArr
	}
	return kr
}

func (r *reader) buildIndexClause(ir *IndexedRange) *cassandra.IndexClause {
	ic := cassandra.NewIndexClause()
	ic.Expressions = r.expressions
	ic.StartKey = ir.Start
	ic.Count = int32(ir.Count)
	// workaround some uninitialized slice == nil quirks that trickle down into the generated thrift4go code
	if ic.StartKey == nil {
		ic.StartKey = make([]byte, 0)
	}
	return ic
}

func (r *reader) Get(key []byte) (*Row, error) {
	if r.cf == "" {
		return nil, errors.New("No column family specified")
	}

	beforeBuild := time.Now()
	cp := r.buildColumnParent()
	sp := r.buildPredicate()
	fmt.Println("Time spent building query: ", time.Now().Sub(beforeBuild))

	var ret []*cassandra.ColumnOrSuperColumn
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		beforeGetSlice := time.Now()
		ret, err = c.client.GetSlice(
			key, cp, sp, cassandra.ConsistencyLevel(r.consistencyLevel))
		fmt.Println("Time spent on GetSlice: ", time.Now().Sub(beforeGetSlice))
		return &transactionError{err}
	})

	if err != nil {
		return nil, err
	}

	beforeRowFromTListColumns := time.Now()
	result := rowFromTListColumns(key, ret)
	fmt.Println("Time spent on rowFromTListColumns: ", time.Now().Sub(beforeRowFromTListColumns))

	return result, nil
}

func (r *reader) Count(key []byte) (int, error) {
	if r.cf == "" {
		return 0, errors.New("No column family specified")
	}

	cp := r.buildColumnParent()
	sp := r.buildPredicate()

	var ret int32
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		ret, err = c.client.GetCount(
			key, cp, sp, cassandra.ConsistencyLevel(r.consistencyLevel))
		return &transactionError{err}
	})

	if err != nil {
		return 0, err
	}

	return int(ret), nil
}

func (r *reader) MultiGet(keys [][]byte) ([]*Row, error) {
	if r.cf == "" {
		return nil, errors.New("No column family specified")
	}

	if len(keys) <= 0 {
		return make([]*Row, 0), nil
	}

	cp := r.buildColumnParent()
	sp := r.buildPredicate()
	tk := keys

	var ret map[string][]*cassandra.ColumnOrSuperColumn
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		ret, err = c.client.MultigetSlice(
			tk, cp, sp, cassandra.ConsistencyLevel(r.consistencyLevel))
		return &transactionError{err}
	})

	if err != nil {
		return nil, err
	}

	return rowsFromTMap(ret), nil
}

func (r *reader) MultiCount(keys [][]byte) ([]*RowColumnCount, error) {
	if r.cf == "" {
		return nil, errors.New("No column family specified")
	}

	if len(keys) <= 0 {
		return make([]*RowColumnCount, 0), nil
	}

	cp := r.buildColumnParent()
	sp := r.buildPredicate()
	tk := keys

	var ret map[string]int32
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		ret, err = c.client.MultigetCount(
			tk, cp, sp, cassandra.ConsistencyLevel(r.consistencyLevel))
		return &transactionError{err}
	})

	if err != nil {
		return nil, err
	}

	return rowsColumnCountFromTMap(ret), nil
}

func (r *reader) RangeGet(rang *Range) ([]*Row, error) {
	if r.cf == "" {
		return nil, errors.New("No column family specified")
	}

	if rang == nil || rang.Count <= 0 {
		return make([]*Row, 0), nil
	}

	kr := r.buildKeyRange(rang)
	cp := r.buildColumnParent()
	sp := r.buildPredicate()

	var ret []*cassandra.KeySlice
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		ret, err = c.client.GetRangeSlices(
			cp, sp, kr, cassandra.ConsistencyLevel(r.consistencyLevel))
		return &transactionError{err}
	})

	if err != nil {
		return nil, err
	}

	return rowsFromTListKeySlice(ret), nil
}

func (r *reader) IndexedGet(rang *IndexedRange) ([]*Row, error) {
	if r.cf == "" {
		return nil, errors.New("No column family specified")
	}

	if !r.setWhere {
		return nil, errors.New("At least one Where call must be made")
	}

	if rang == nil || rang.Count <= 0 {
		return make([]*Row, 0), nil
	}

	ic := r.buildIndexClause(rang)
	cp := r.buildColumnParent()
	sp := r.buildPredicate()

	var ret []*cassandra.KeySlice
	err := r.pool.run(func(c *connection) *transactionError {
		var err error
		ret, err = c.client.GetIndexedSlices(
			cp, ic, sp, cassandra.ConsistencyLevel(r.consistencyLevel))
		return &transactionError{err}
	})

	if err != nil {
		return nil, err
	}

	return rowsFromTListKeySlice(ret), nil
}

func rowFromTListColumns(key []byte, tl []*cassandra.ColumnOrSuperColumn) *Row {
	if len(tl) <= 0 {
		return nil
	}
	r := &Row{Key: key}
	for _, col := range tl {
		if col.Column != nil {
			c := &Column{Name: col.Column.Name}
			if col.Column.IsSetValue() {
				c.Value = *col.Column.Value
			}
			if col.Column.IsSetTimestamp() {
				c.Timestamp = *col.Column.Timestamp
			}
			if col.Column.IsSetTtl() {
				c.Ttl = *col.Column.Ttl
			}
			r.Columns = append(r.Columns, c)
		} else if col.CounterColumn != nil {
			v, _ := Marshal(col.CounterColumn.Value, LongType)
			c := &Column{
				Name:  col.CounterColumn.Name,
				Value: v,
			}
			r.Columns = append(r.Columns, c)
		}
	}
	return r
}

func rowsFromTMap(tm map[string][]*cassandra.ColumnOrSuperColumn) []*Row {
	if len(tm) <= 0 {
		return make([]*Row, 0)
	}
	r := make([]*Row, 0)
	for key, v := range tm {
		columns := v
		row := rowFromTListColumns([]byte(key), columns)
		if row != nil {
			r = append(r, row)
		}
	}
	return r
}

func rowsColumnCountFromTMap(tm map[string]int32) []*RowColumnCount {
	if len(tm) <= 0 {
		return make([]*RowColumnCount, 0)
	}
	r := make([]*RowColumnCount, 0)
	for key, count := range tm {
		if count > 0 {
			r = append(r, &RowColumnCount{Key: []byte(key), Count: int(count)})
		}
	}
	return r
}

func rowsFromTListKeySlice(tl []*cassandra.KeySlice) []*Row {
	if len(tl) <= 0 {
		return make([]*Row, 0)
	}
	r := make([]*Row, 0)
	for _, keySlice := range tl {
		key := keySlice.Key
		row := rowFromTListColumns(key, keySlice.Columns)
		if row != nil {
			r = append(r, row)
		}
	}
	return r
}

func (r *Row) ColumnNames() [][]byte {
	names := [][]byte{}
	for _, col := range r.Columns {
		names = append(names, col.Name)
	}
	return names
}
