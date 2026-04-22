package executor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

var (
	errHashAggregateNilChild           = errors.New("executor: hash aggregate child is nil")
	errAggregateMissingExpr            = errors.New("executor: aggregate expression is required")
	errAggregateCountStarRequiresCount = errors.New("executor: COUNT(*) is only valid for COUNT")
	errUnsupportedAggregate            = errors.New("executor: unsupported aggregate")
)

// AggregateName identifies one executor-native aggregate function.
type AggregateName string

const (
	// AggregateCount counts input rows or non-NULL aggregate arguments.
	AggregateCount AggregateName = "COUNT"
	// AggregateSum sums the non-NULL aggregate arguments.
	AggregateSum AggregateName = "SUM"
	// AggregateAvg averages the non-NULL aggregate arguments.
	AggregateAvg AggregateName = "AVG"
	// AggregateMin returns the minimum non-NULL aggregate argument.
	AggregateMin AggregateName = "MIN"
	// AggregateMax returns the maximum non-NULL aggregate argument.
	AggregateMax AggregateName = "MAX"
	// AggregateEvery returns SQL EVERY / PostgreSQL bool_and semantics over
	// non-NULL boolean inputs.
	AggregateEvery AggregateName = "EVERY"
)

// AggregateSpec configures one executor-native aggregate output.
//
// HashAggregate emits one output row per group. Each output row contains the
// evaluated group-key values first, followed by aggregate results in Aggregate
// spec order.
type AggregateSpec struct {
	Name      AggregateName
	Expr      CompiledExpr
	CountStar bool
}

// HashAggregate is the Phase 1 executor-native grouping and aggregate operator.
//
// The operator materializes and hashes its child rows on the first Next call,
// preserving the order in which groups are first encountered. Output rows are
// synthetic and therefore do not preserve child storage handles.
//
// Phase 1 aggregate semantics intentionally follow PostgreSQL:
//   - COUNT returns 0 when there are no qualifying inputs.
//   - SUM, AVG, MIN, MAX, and EVERY return NULL when there are no qualifying
//     non-NULL inputs.
//   - EVERY ignores NULL inputs and otherwise behaves like PostgreSQL bool_and.
//   - AVG over exact numerics uses NUMERIC semantics with 16 fractional digits
//     before the in-repo Decimal normalization step.
type HashAggregate struct {
	lifecycle lifecycle
	child     Operator
	groups    []CompiledExpr
	aggs      []AggregateSpec
	results   []Row
	index     int

	childOpen      bool
	materialized   bool
	materializeErr error
}

var _ Operator = (*HashAggregate)(nil)

// NewHashAggregate constructs an executor-native hash aggregate over one child
// operator.
func NewHashAggregate(child Operator, groups []CompiledExpr, aggs ...AggregateSpec) *HashAggregate {
	return &HashAggregate{
		child:  child,
		groups: append([]CompiledExpr(nil), groups...),
		aggs:   append([]AggregateSpec(nil), aggs...),
	}
}

// Open prepares the child operator for later materialization.
func (h *HashAggregate) Open() error {
	if err := h.lifecycle.Open(); err != nil {
		return err
	}
	if h.child == nil {
		h.lifecycle = lifecycle{}

		return errHashAggregateNilChild
	}
	if err := validateAggregateSpecs(h.aggs); err != nil {
		h.lifecycle = lifecycle{}

		return err
	}
	if err := h.child.Open(); err != nil {
		h.lifecycle = lifecycle{}

		return err
	}

	h.childOpen = true
	h.results = nil
	h.index = 0
	h.materialized = false
	h.materializeErr = nil

	return nil
}

// Next returns the next grouped aggregate row.
func (h *HashAggregate) Next() (Row, error) {
	if err := h.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if !h.materialized {
		if h.materializeErr != nil {
			return Row{}, h.materializeErr
		}
		if err := h.materialize(); err != nil {
			h.materializeErr = err

			return Row{}, err
		}

		h.materialized = true
	}
	if h.index >= len(h.results) {
		return Row{}, io.EOF
	}

	row := h.results[h.index].Clone()
	h.index++

	return row, nil
}

// Close releases the child operator and terminally closes the aggregate.
func (h *HashAggregate) Close() error {
	child := h.child
	childOpen := h.childOpen

	h.childOpen = false
	h.results = nil
	h.index = 0
	h.materialized = false
	h.materializeErr = nil

	if err := h.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}

type aggregateBucket struct {
	groupValues []sqltypes.Value
	states      []aggregateState
}

func (h *HashAggregate) materialize() error {
	buckets := make([]aggregateBucket, 0)
	bucketIndex := make(map[string][]int)
	sawRow := false

	for {
		row, err := h.child.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		sawRow = true

		groupValues, err := evalAggregateGroupValues(h.groups, row)
		if err != nil {
			return err
		}

		signature := aggregateGroupSignature(groupValues)
		index, ok := findAggregateBucket(bucketIndex[signature], buckets, groupValues)
		if !ok {
			states, err := newAggregateStates(h.aggs)
			if err != nil {
				return err
			}

			buckets = append(buckets, aggregateBucket{
				groupValues: append([]sqltypes.Value(nil), groupValues...),
				states:      states,
			})
			index = len(buckets) - 1
			bucketIndex[signature] = append(bucketIndex[signature], index)
		}

		for aggIndex, spec := range h.aggs {
			value, err := evalAggregateValue(spec, row)
			if err != nil {
				return err
			}
			if err := buckets[index].states[aggIndex].Step(value); err != nil {
				return err
			}
		}
	}

	if !sawRow {
		if len(h.groups) != 0 {
			h.results = nil

			return nil
		}

		states, err := newAggregateStates(h.aggs)
		if err != nil {
			return err
		}
		buckets = append(buckets, aggregateBucket{states: states})
	}

	results := make([]Row, 0, len(buckets))
	for _, bucket := range buckets {
		values := make([]sqltypes.Value, 0, len(bucket.groupValues)+len(bucket.states))
		values = append(values, bucket.groupValues...)
		for _, state := range bucket.states {
			value, err := state.Result()
			if err != nil {
				return err
			}
			values = append(values, value)
		}

		results = append(results, NewRow(values...))
	}

	h.results = results
	h.index = 0

	return nil
}

func findAggregateBucket(candidates []int, buckets []aggregateBucket, groupValues []sqltypes.Value) (int, bool) {
	for _, index := range candidates {
		if index < 0 || index >= len(buckets) {
			continue
		}
		if aggregateGroupValuesEqual(buckets[index].groupValues, groupValues) {
			return index, true
		}
	}

	return 0, false
}

func aggregateGroupValuesEqual(left []sqltypes.Value, right []sqltypes.Value) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if !left[index].Equal(right[index]) {
			return false
		}
	}

	return true
}

func validateAggregateSpecs(aggs []AggregateSpec) error {
	for _, spec := range aggs {
		name := normalizeAggregateName(spec.Name)
		if spec.CountStar && name != AggregateCount {
			return errAggregateCountStarRequiresCount
		}
		switch name {
		case AggregateCount:
			if !spec.CountStar && spec.Expr.eval == nil {
				return errAggregateMissingExpr
			}
		case AggregateSum, AggregateAvg, AggregateMin, AggregateMax, AggregateEvery:
			if spec.Expr.eval == nil {
				return errAggregateMissingExpr
			}
		default:
			return fmt.Errorf("%w: %s", errUnsupportedAggregate, spec.Name)
		}
	}

	return nil
}

func evalAggregateGroupValues(groups []CompiledExpr, row Row) ([]sqltypes.Value, error) {
	values := make([]sqltypes.Value, len(groups))
	for index, expr := range groups {
		value, err := expr.Eval(row)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}

	return values, nil
}

func evalAggregateValue(spec AggregateSpec, row Row) (sqltypes.Value, error) {
	if spec.CountStar {
		return sqltypes.NullValue(), nil
	}

	return spec.Expr.Eval(row)
}

func newAggregateStates(aggs []AggregateSpec) ([]aggregateState, error) {
	states := make([]aggregateState, 0, len(aggs))
	for _, spec := range aggs {
		state, err := newAggregateState(spec)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}

	return states, nil
}

func newAggregateState(spec AggregateSpec) (aggregateState, error) {
	switch normalizeAggregateName(spec.Name) {
	case AggregateCount:
		return &countAggregateState{countStar: spec.CountStar}, nil
	case AggregateSum:
		return &sumAggregateState{sourceHint: spec.Expr.Type()}, nil
	case AggregateAvg:
		return &avgAggregateState{sourceHint: spec.Expr.Type()}, nil
	case AggregateMin:
		return &minMaxAggregateState{pickMin: true}, nil
	case AggregateMax:
		return &minMaxAggregateState{pickMin: false}, nil
	case AggregateEvery:
		return &everyAggregateState{}, nil
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedAggregate, spec.Name)
	}
}

func normalizeAggregateName(name AggregateName) AggregateName {
	return AggregateName(strings.ToUpper(strings.TrimSpace(string(name))))
}

type aggregateState interface {
	Step(value sqltypes.Value) error
	Result() (sqltypes.Value, error)
}

type countAggregateState struct {
	count     int64
	countStar bool
}

func (s *countAggregateState) Step(value sqltypes.Value) error {
	if s.countStar || !value.IsNull() {
		s.count++
	}

	return nil
}

func (s *countAggregateState) Result() (sqltypes.Value, error) {
	return sqltypes.Int64Value(s.count), nil
}

type sumAggregateState struct {
	sourceHint sqltypes.TypeDesc
	targetType sqltypes.TypeDesc
	value      sqltypes.Value
	seen       bool
}

func (s *sumAggregateState) Step(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}

	source, ok := sourceTypeForValue(value, s.sourceHint)
	if !ok {
		return fmt.Errorf("%w: SUM argument missing numeric type", ErrInvalidExpressionType)
	}
	if !s.seen {
		target, err := aggregateSumTargetType(source)
		if err != nil {
			return err
		}
		coerced, err := coerceRuntimeValue(value, source, target)
		if err != nil {
			return err
		}

		s.targetType = target
		s.value = coerced
		s.seen = true

		return nil
	}

	coerced, err := coerceRuntimeValue(value, source, s.targetType)
	if err != nil {
		return err
	}

	sum, err := numericBinaryValue("+", s.value, s.targetType, coerced, s.targetType, s.targetType)
	if err != nil {
		return err
	}

	s.value = sum

	return nil
}

func (s *sumAggregateState) Result() (sqltypes.Value, error) {
	if !s.seen {
		return sqltypes.NullValue(), nil
	}

	return s.value, nil
}

func aggregateSumTargetType(source sqltypes.TypeDesc) (sqltypes.TypeDesc, error) {
	switch source.Kind {
	case sqltypes.TypeKindSmallInt, sqltypes.TypeKindInteger:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt}, nil
	case sqltypes.TypeKindBigInt:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric}, nil
	case sqltypes.TypeKindNumeric, sqltypes.TypeKindDecimal:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric}, nil
	case sqltypes.TypeKindReal, sqltypes.TypeKindDoublePrecision:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision}, nil
	default:
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: SUM requires numeric input, found %s", ErrInvalidExpressionType, source.Kind)
	}
}

type avgExactAggregateState struct {
	sum   *big.Rat
	count int64
}

func (s *avgExactAggregateState) Step(value sqltypes.Value, source sqltypes.TypeDesc) error {
	if value.IsNull() {
		return nil
	}

	if !isExactAggregateType(source) {
		return fmt.Errorf("%w: AVG exact state requires exact numeric input, found %s", ErrInvalidExpressionType, source.Kind)
	}

	coerced, err := coerceRuntimeValue(value, source, sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric})
	if err != nil {
		return err
	}

	decimal, ok := coerced.Raw().(sqltypes.Decimal)
	if !ok {
		return fmt.Errorf("%w: AVG expected DECIMAL input, found %s", ErrInvalidExpressionType, coerced.Kind())
	}

	if s.sum == nil {
		s.sum = new(big.Rat)
	}
	s.sum.Add(s.sum, ratFromDecimal(decimal))
	s.count++

	return nil
}

func (s *avgExactAggregateState) Result() (sqltypes.Value, error) {
	if s.count == 0 {
		return sqltypes.NullValue(), nil
	}

	average := new(big.Rat).Quo(s.sum, big.NewRat(s.count, 1))
	decimal, err := sqltypes.ParseDecimal(average.FloatString(16))
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.DecimalValue(decimal), nil
}

type avgApproximateAggregateState struct {
	sum   float64
	count int64
}

func (s *avgApproximateAggregateState) Step(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}

	floating, err := float64FromValue(value)
	if err != nil {
		return err
	}

	s.sum += floating
	s.count++

	return nil
}

func (s *avgApproximateAggregateState) Result() (sqltypes.Value, error) {
	if s.count == 0 {
		return sqltypes.NullValue(), nil
	}

	return sqltypes.Float64Value(s.sum / float64(s.count)), nil
}

type avgAggregateState struct {
	sourceHint sqltypes.TypeDesc
	mode       avgMode
	exact      avgExactAggregateState
	approx     avgApproximateAggregateState
}

type avgMode uint8

const (
	avgModeUnset avgMode = iota
	avgModeExact
	avgModeApproximate
)

func (s *avgAggregateState) Step(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}

	source, ok := sourceTypeForValue(value, s.sourceHint)
	if !ok {
		return fmt.Errorf("%w: AVG argument missing numeric type", ErrInvalidExpressionType)
	}

	switch {
	case isApproximateAggregateType(source):
		s.mode = avgModeApproximate
		return s.approx.Step(value)
	case isExactAggregateType(source):
		s.mode = avgModeExact
		return s.exact.Step(value, source)
	default:
		return fmt.Errorf("%w: AVG requires numeric input, found %s", ErrInvalidExpressionType, source.Kind)
	}
}

func (s *avgAggregateState) Result() (sqltypes.Value, error) {
	switch s.mode {
	case avgModeApproximate:
		return s.approx.Result()
	case avgModeExact:
		return s.exact.Result()
	default:
		return sqltypes.NullValue(), nil
	}
}

type minMaxAggregateState struct {
	value   sqltypes.Value
	seen    bool
	pickMin bool
}

func (s *minMaxAggregateState) Step(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	if !s.seen {
		s.value = value
		s.seen = true

		return nil
	}

	comparison, err := value.Compare(s.value)
	if err != nil {
		return err
	}

	if (s.pickMin && comparison < 0) || (!s.pickMin && comparison > 0) {
		s.value = value
	}

	return nil
}

func (s *minMaxAggregateState) Result() (sqltypes.Value, error) {
	if !s.seen {
		return sqltypes.NullValue(), nil
	}

	return s.value, nil
}

type everyAggregateState struct {
	seen    bool
	allTrue bool
}

func (s *everyAggregateState) Step(value sqltypes.Value) error {
	truth, err := sqlBoolFromValue(value)
	if err != nil {
		return err
	}
	if truth == sqlUnknown {
		return nil
	}
	if !s.seen {
		s.seen = true
		s.allTrue = truth == sqlTrue

		return nil
	}
	if truth == sqlFalse {
		s.allTrue = false
	}

	return nil
}

func (s *everyAggregateState) Result() (sqltypes.Value, error) {
	if !s.seen {
		return sqltypes.NullValue(), nil
	}

	return sqltypes.BoolValue(s.allTrue), nil
}

func isApproximateAggregateType(desc sqltypes.TypeDesc) bool {
	switch desc.Kind {
	case sqltypes.TypeKindReal, sqltypes.TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func isExactAggregateType(desc sqltypes.TypeDesc) bool {
	switch desc.Kind {
	case sqltypes.TypeKindSmallInt, sqltypes.TypeKindInteger, sqltypes.TypeKindBigInt, sqltypes.TypeKindNumeric, sqltypes.TypeKindDecimal:
		return true
	default:
		return false
	}
}

func aggregateGroupSignature(values []sqltypes.Value) string {
	var builder strings.Builder
	for _, value := range values {
		writeAggregateValueSignature(&builder, value)
	}

	return builder.String()
}

func writeAggregateValueSignature(builder *strings.Builder, value sqltypes.Value) {
	builder.WriteString(value.Kind().String())
	builder.WriteByte(':')

	switch value.Kind() {
	case sqltypes.ValueKindNull:
		builder.WriteByte(';')
	case sqltypes.ValueKindBool:
		builder.WriteString(strconv.FormatBool(value.Raw().(bool)))
		builder.WriteByte(';')
	case sqltypes.ValueKindInt16:
		builder.WriteString(strconv.FormatInt(int64(value.Raw().(int16)), 10))
		builder.WriteByte(';')
	case sqltypes.ValueKindInt32:
		builder.WriteString(strconv.FormatInt(int64(value.Raw().(int32)), 10))
		builder.WriteByte(';')
	case sqltypes.ValueKindInt64:
		builder.WriteString(strconv.FormatInt(value.Raw().(int64), 10))
		builder.WriteByte(';')
	case sqltypes.ValueKindFloat32:
		float := float64(value.Raw().(float32))
		if float == 0 {
			builder.WriteByte('0')
		} else {
			builder.WriteString(strconv.FormatFloat(float, 'g', -1, 32))
		}
		builder.WriteByte(';')
	case sqltypes.ValueKindFloat64:
		float := value.Raw().(float64)
		if float == 0 {
			builder.WriteByte('0')
		} else {
			builder.WriteString(strconv.FormatFloat(float, 'g', -1, 64))
		}
		builder.WriteByte(';')
	case sqltypes.ValueKindString:
		text := value.Raw().(string)
		builder.WriteString(strconv.Itoa(len(text)))
		builder.WriteByte('=')
		builder.WriteString(text)
		builder.WriteByte(';')
	case sqltypes.ValueKindBytes:
		bytes := value.Raw().([]byte)
		builder.WriteString(hex.EncodeToString(bytes))
		builder.WriteByte(';')
	case sqltypes.ValueKindDateTime:
		builder.WriteString(value.Raw().(time.Time).UTC().Format(time.RFC3339Nano))
		builder.WriteByte(';')
	case sqltypes.ValueKindTimeOfDay:
		builder.WriteString(strconv.FormatInt(int64(value.Raw().(time.Duration)), 10))
		builder.WriteByte(';')
	case sqltypes.ValueKindInterval:
		interval := value.Raw().(sqltypes.Interval).Normalize()
		builder.WriteString(strconv.FormatInt(interval.Months, 10))
		builder.WriteByte('/')
		builder.WriteString(strconv.FormatInt(interval.Days, 10))
		builder.WriteByte('/')
		builder.WriteString(strconv.FormatInt(interval.Nanos, 10))
		builder.WriteByte(';')
	case sqltypes.ValueKindDecimal:
		builder.WriteString(value.Raw().(sqltypes.Decimal).String())
		builder.WriteByte(';')
	case sqltypes.ValueKindArray:
		builder.WriteByte('[')
		for _, element := range value.Raw().(sqltypes.Array) {
			writeAggregateValueSignature(builder, element)
		}
		builder.WriteString("];")
	case sqltypes.ValueKindRow:
		builder.WriteByte('(')
		for _, field := range value.Raw().(sqltypes.Row) {
			writeAggregateValueSignature(builder, field)
		}
		builder.WriteString(");")
	default:
		fmt.Fprintf(builder, "%v;", value.Raw())
	}
}

func ratFromDecimal(decimal sqltypes.Decimal) *big.Rat {
	coefficient := decimal.Coefficient()
	scale := decimal.Scale()

	switch {
	case scale == 0:
		return new(big.Rat).SetInt(coefficient)
	case scale > 0:
		return new(big.Rat).SetFrac(coefficient, pow10(int64(scale)))
	default:
		coefficient.Mul(coefficient, pow10(int64(-scale)))
		return new(big.Rat).SetInt(coefficient)
	}
}
