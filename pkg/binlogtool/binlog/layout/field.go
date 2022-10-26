/*
Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package layout

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"

	"github.com/google/uuid"
	"golang.org/x/exp/constraints"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

var (
	ErrNotEnoughBytes = errors.New("not enough bytes")
	ErrNotNull        = errors.New("not null")
)

type ExtractFrom interface {
	FromBlock(data []byte) (int, error)
	FromStream(r io.Reader) error
}

func checkDataSize(expect int, data []byte) error {
	if len(data) < expect {
		return ErrNotEnoughBytes
	}
	return nil
}

type FieldType uint8

const (
	NullType      FieldType = 0
	BoolType      FieldType = 1
	NumberType    FieldType = 2
	PackedIntType FieldType = 3
	BytesType     FieldType = 4
	BitSetType    FieldType = 5
	UUIDType      FieldType = 6
	AreaType      FieldType = 7
	RepeatType    FieldType = 8
	SkipType      FieldType = 9
	EOFType       FieldType = 0xff
)

func FieldTypeName(t FieldType) string {
	switch t {
	case NullType:
		return "Null"
	case BoolType:
		return "Bool"
	case NumberType:
		return "Number"
	case PackedIntType:
		return "PackedInt"
	case BytesType:
		return "Bytes"
	case BitSetType:
		return "BitSet"
	case UUIDType:
		return "UUID"
	case AreaType:
		return "Area"
	case RepeatType:
		return "Repeat"
	case SkipType:
		return "Skip"
	case EOFType:
		return "EOF"
	default:
		return "Unknown"
	}
}

type Field interface {
	ExtractFrom

	Type() FieldType
	IsVariant() bool
	SizeRange() (int, int)
}

type NullField struct {
}

func (f NullField) FromBlock(data []byte) (int, error) {
	if err := checkDataSize(1, data); err != nil {
		return 0, err
	}
	if data[0] != 0 {
		return 0, ErrNotNull
	}
	return 1, nil
}

func (f NullField) FromStream(r io.Reader) error {
	buf := [1]byte{}
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}
	if buf[0] != 0 {
		return ErrNotNull
	}
	return nil
}

func (f NullField) Type() FieldType {
	return NullType
}

func (f NullField) IsVariant() bool {
	return false
}

func (f NullField) SizeRange() (int, int) {
	return 1, 1
}

type BoolField struct {
	r *bool
}

func (f BoolField) FromBlock(data []byte) (int, error) {
	if err := checkDataSize(1, data); err != nil {
		return 0, err
	}
	*f.r = data[0] != 0
	return 1, nil
}

func (f BoolField) FromStream(r io.Reader) error {
	buf := [1]byte{}
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}
	*f.r = buf[0] != 0
	return nil
}

func (f BoolField) Type() FieldType {
	return BoolType
}

func (f BoolField) IsVariant() bool {
	return false
}

func (f BoolField) SizeRange() (int, int) {
	return 1, 1
}

type NumberField[N constraints.Integer | constraints.Float] struct {
	r *N
}

func (f NumberField[N]) FromBlock(data []byte) (int, error) {
	if err := checkDataSize(int(unsafe.Sizeof(N(0))), data); err != nil {
		return 0, err
	}
	utils.ReadNumberLittleEndianHack(f.r, data)
	return int(unsafe.Sizeof(N(0))), nil
}

func (f NumberField[N]) FromStream(r io.Reader) error {
	buf := make([]byte, int(unsafe.Sizeof(N(0))))
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}
	utils.ReadNumberLittleEndianHack(f.r, buf)
	return nil
}

func (f NumberField[N]) Type() FieldType {
	return NumberType
}

func (f NumberField[N]) IsVariant() bool {
	return false
}

func (f NumberField[N]) SizeRange() (int, int) {
	return int(unsafe.Sizeof(N(0))), int(unsafe.Sizeof(N(0)))
}

type PackedIntField struct {
	r *uint64
}

func (f PackedIntField) FromBlock(bs []byte) (int, error) {
	if len(bs) < 1 {
		return 0, ErrNotEnoughBytes
	}
	firstByte := bs[0]
	if firstByte <= 250 {
		*f.r = uint64(firstByte)
		return 1, nil
	} else if firstByte == 252 {
		if len(bs) < 3 {
			return 0, ErrNotEnoughBytes
		}
		var v uint16
		utils.ReadNumberLittleEndianHack(&v, bs[1:])
		*f.r = uint64(v)
		return 3, nil
	} else if firstByte == 253 {
		if len(bs) < 4 {
			return 0, ErrNotEnoughBytes
		}
		v := uint32(bs[0]) + uint32(bs[1])<<8 + uint32(bs[2])<<16
		*f.r = uint64(v)
		return 4, nil
	} else if firstByte == 254 {
		if len(bs) < 9 {
			return 0, ErrNotEnoughBytes
		}
		utils.ReadNumberLittleEndianHack(f.r, bs[1:])
		return 9, nil
	} else {
		return 0, errors.New("packed int invalid")
	}
}

func (f PackedIntField) FromStream(r io.Reader) error {
	head := [1]byte{}
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}

	firstByte := head[0]
	if firstByte <= 250 {
		*f.r = uint64(firstByte)
		return nil
	} else if firstByte == 252 {
		bs := [2]byte{}
		if _, err := io.ReadFull(r, bs[:]); err != nil {
			return fmt.Errorf("unable to read: %w", err)
		}
		var v uint16
		utils.ReadNumberLittleEndianHack(&v, bs[:])
		*f.r = uint64(v)
		return nil
	} else if firstByte == 253 {
		bs := [3]byte{}
		if _, err := io.ReadFull(r, bs[:3]); err != nil {
			return fmt.Errorf("unable to read: %w", err)
		}
		var v uint32
		utils.ReadNumberLittleEndianHack(&v, bs[:])
		*f.r = uint64(v)
		return nil
	} else if firstByte == 254 {
		bs := [8]byte{}
		if _, err := io.ReadFull(r, bs[:]); err != nil {
			return fmt.Errorf("unable to read: %w", err)
		}
		utils.ReadNumberLittleEndianHack(f.r, bs[:])
		return nil
	} else {
		return errors.New("packed int invalid")
	}
}

func (f PackedIntField) Type() FieldType {
	return PackedIntType
}

func (f PackedIntField) IsVariant() bool {
	return true
}

func (f PackedIntField) SizeRange() (int, int) {
	return 1, 5
}

type BytesField[N constraints.Integer, B ~[]byte] struct {
	l       *N
	r       *B
	nullEnd bool
}

func (f BytesField[N, B]) FromBlock(data []byte) (int, error) {
	// Copy to end
	if f.l == nil {
		*f.r = make([]byte, len(data))
		copy(*f.r, data)
		return len(data), nil
	} else {

		// Read length
		l := int(*f.l)
		if len(data) < l {
			return 0, ErrNotEnoughBytes
		}

		// Copy
		if !f.nullEnd {
			*f.r = make([]byte, l)
			copy(*f.r, data[:l])
		} else {
			if l == 0 {
				return 0, ErrNotEnoughBytes
			}
			if data[l] != 0 {
				return 0, ErrNotNull
			}
			*f.r = make([]byte, l-1)
			copy(*f.r, data[:l-1])
		}

		return l, nil
	}
}

func (f BytesField[N, B]) FromStream(r io.Reader) error {
	// Copy to end
	if f.l == nil {
		var err error
		*f.r, err = io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("unable to read: %w", err)
		}
		return nil
	} else {

		// Read length
		l := int(*f.l)

		// Copy
		if !f.nullEnd {
			*f.r = make([]byte, l)
			if _, err := io.ReadFull(r, *f.r); err != nil {
				if err == io.ErrUnexpectedEOF {
					return ErrNotEnoughBytes
				}
				return fmt.Errorf("unable to read: %w", err)
			}
		} else {
			if l == 0 {
				return ErrNotEnoughBytes
			}
			*f.r = make([]byte, l)
			if _, err := io.ReadFull(r, *f.r); err != nil {
				if err == io.ErrUnexpectedEOF {
					return ErrNotEnoughBytes
				}
				return fmt.Errorf("unable to read: %w", err)
			}
			trailNull := [1]byte{}
			if _, err := io.ReadFull(r, trailNull[:]); err != nil {
				return fmt.Errorf("unable to read: %w", err)
			}
			if trailNull[0] != 0 {
				return ErrNotNull
			}
		}

		return nil
	}
}

func (f BytesField[N, B]) Type() FieldType {
	return BytesType
}

func (f BytesField[N, B]) IsVariant() bool {
	return true
}

func (f BytesField[N, B]) SizeRange() (int, int) {
	return 0, math.MaxInt
}

type BitSetField[N constraints.Integer] struct {
	l *N
	r *bitmap.Bitmap
}

func (f BitSetField[N]) FromBlock(data []byte) (int, error) {
	// Read length
	l := (int(*f.l) + 7) / 8
	if len(data) < l {
		return 0, ErrNotEnoughBytes
	}

	// Copy
	buf := make([]byte, l)
	copy(buf, data[:l])
	*f.r = bitmap.NewBitmap(buf, int(*f.l))

	return l, nil
}

func (f BitSetField[N]) FromStream(r io.Reader) error {
	// Read length
	l := (int(*f.l) + 7) / 8

	// Copy
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.ErrUnexpectedEOF {
			return ErrNotEnoughBytes
		}
		return fmt.Errorf("unable to read: %w", err)
	}
	*f.r = bitmap.NewBitmap(buf, int(*f.l))

	return nil
}

func (f BitSetField[N]) Type() FieldType {
	return BitSetType
}

func (f BitSetField[N]) IsVariant() bool {
	return true
}

func (f BitSetField[N]) SizeRange() (int, int) {
	return 0, math.MaxInt
}

type UUIDField struct {
	r *uuid.UUID
}

func (f UUIDField) FromBlock(data []byte) (int, error) {
	if err := checkDataSize(16, data); err != nil {
		return 0, err
	}
	copy((*f.r)[:], data)
	return 16, nil
}

func (f UUIDField) FromStream(r io.Reader) error {
	_, err := io.ReadFull(r, (*f.r)[:])
	if err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}
	return nil
}

func (f UUIDField) Type() FieldType {
	return UUIDType
}

func (f UUIDField) IsVariant() bool {
	return false
}

func (f UUIDField) SizeRange() (int, int) {
	return 16, 16
}

type RepeatField[N constraints.Integer, T any] struct {
	size *N
	r    *[]T
	bind func(x *T) []Field
}

func (rf RepeatField[N, T]) FromBlock(data []byte) (int, error) {
	if rf.size == nil {
		sl, sr := rf.fieldsSizeRange()
		if sl != sr {
			return 0, errors.New("repeated fields are not in fixed size")
		}

		if err := rf.fromBytesInfinite(data, sl); err != nil {
			return 0, err
		}
		return len(data), nil
	} else {
		buf := bytes.NewBuffer(data)
		if err := rf.fromReaderFinite(buf); err != nil {
			return 0, err
		}
		return len(data) - buf.Cap(), nil
	}
}

func (rf *RepeatField[N, T]) fromReaderOne(r io.Reader, x *T, i int) error {
	fields := rf.bind(x)
	for fieldIndex, f := range fields {
		if f == nil {
			continue
		}
		err := f.FromStream(r)
		if err != nil {
			return fmt.Errorf("unable to extract field[%s, %d] of data[%d]: %w", FieldTypeName(f.Type()), fieldIndex, i, err)
		}
	}
	return nil
}

func (rf *RepeatField[N, T]) fromReaderFinite(r io.Reader) error {
	l := int(*rf.size)
	rr := make([]T, l)
	for i := 0; i < l; i++ {
		if err := rf.fromReaderOne(r, &rr[i], i); err != nil {
			return err
		}
	}
	*rf.r = rr
	return nil
}

func (rf *RepeatField[N, T]) fieldsSizeRange() (int, int) {
	var x T
	fields := rf.bind(&x)
	sl, sr := 0, 0
	for _, f := range fields {
		if f == nil {
			continue
		}
		l, r := f.SizeRange()
		sl += l
		if sr == math.MaxInt || r == math.MaxInt {
			sr = math.MaxInt
		} else {
			sr += r
		}
	}
	return sl, sr
}

func (rf *RepeatField[N, T]) fromBytesInfinite(data []byte, oneSize int) error {
	if len(data)%oneSize != 0 {
		return fmt.Errorf("left data is not fit for repeated fields, left: %d, each: %d", len(data), oneSize)
	}

	buf := bytes.NewBuffer(data)
	rr := make([]T, len(data)/oneSize)
	for i := 0; i < len(data)/oneSize; i++ {
		if err := rf.fromReaderOne(buf, &rr[i], i); err != nil {
			return err
		}
	}

	return nil
}

func (rf *RepeatField[N, T]) fromReaderInfinite(r io.Reader) error {
	sl, sr := rf.fieldsSizeRange()
	if sl != sr {
		return errors.New("repeated fields are not in fixed size")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("unable to read: %w", err)
	}

	return rf.fromBytesInfinite(data, sl)
}

func (rf RepeatField[N, T]) FromStream(r io.Reader) error {
	if rf.size == nil {
		return rf.fromReaderInfinite(r)
	} else {
		return rf.fromReaderFinite(r)
	}
}

func (rf RepeatField[N, T]) Type() FieldType {
	return RepeatType
}

func (rf RepeatField[N, T]) IsVariant() bool {
	return true
}

func (rf RepeatField[N, T]) SizeRange() (int, int) {
	return 0, math.MaxInt
}

type SkipField[N constraints.Integer] struct {
	l *N
}

func (s SkipField[N]) FromBlock(data []byte) (int, error) {
	// Skip all
	if s.l == nil {
		return len(data), nil
	} else {

		// Skip to length
		l := int(*s.l)
		if err := checkDataSize(l, data); err != nil {
			return 0, err
		}

		return l, nil
	}
}

func (s SkipField[N]) FromStream(r io.Reader) error {
	// Skip all
	if s.l == nil {
		_, err := io.Copy(io.Discard, r)
		if err != nil {
			return fmt.Errorf("unable to skip: %w", err)
		}
		return nil
	} else {
		// Skip to length
		l := int(*s.l)
		err := utils.SkipBytes(r, int64(l))
		if err != nil {
			if err == io.EOF {
				return ErrNotEnoughBytes
			}
			return fmt.Errorf("unable to skip: %w", err)
		}

		return nil
	}
}

func (s SkipField[N]) Type() FieldType {
	return SkipType
}

func (s SkipField[N]) IsVariant() bool {
	return true
}

func (s SkipField[N]) SizeRange() (int, int) {
	return 0, math.MaxInt
}

type BlockHandle func(data []byte) (int, error)
type StreamHandle func(r io.Reader) error

type Handler interface {
	~func(data []byte) (int, error) | ~func(r io.Reader) error
}

func handleBlock[H Handler](block []byte, h H) (int, error) {
	v := any(h)
	if bh, ok := v.(func(data []byte) (int, error)); ok {
		return bh(block)
	}
	if sh, ok := v.(func(r io.Reader) error); ok {
		r := bytes.NewBuffer(block)
		if err := sh(r); err != nil {
			return 0, err
		}
		return len(block) - r.Len(), nil
	}
	panic("never reach here")
}

func handleStream[H Handler](r io.Reader, h H) error {
	v := any(h)
	if bh, ok := v.(func(data []byte) (int, error)); ok {
		block, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("unable to read: %w", err)
		}
		_, err = bh(block)
		return err
	}
	if sh, ok := v.(func(r io.Reader) error); ok {
		return sh(r)
	}
	panic("never reach here")
}

type AreaField[N constraints.Integer, H Handler] struct {
	l *N
	h H
}

func (a AreaField[N, H]) FromBlock(data []byte) (int, error) {
	if a.l == nil {
		return handleBlock[H](data, a.h)
	} else {
		l := int(*a.l)
		if err := checkDataSize(l, data); err != nil {
			return 0, err
		}

		return handleBlock[H](data[:l], a.h)
	}
}

func (a AreaField[N, H]) FromStream(r io.Reader) error {
	if a.l == nil {
		return handleStream[H](r, a.h)
	} else {
		l := int64(*a.l)
		return handleStream(io.LimitReader(r, l), a.h)
	}
}

func (a AreaField[N, H]) Type() FieldType {
	return AreaType
}

func (a AreaField[N, H]) IsVariant() bool {
	return true
}

func (a AreaField[N, H]) SizeRange() (int, int) {
	return 0, math.MaxInt
}

// Declare helpers.

func Null() NullField {
	return NullField{}
}

func Bool(r *bool) BoolField {
	return BoolField{r: r}
}

func Number[N constraints.Integer | constraints.Float](r *N) NumberField[N] {
	return NumberField[N]{r: r}
}

func PackedInt(r *uint64) PackedIntField {
	return PackedIntField{r: r}
}

func Bytes[N constraints.Integer, B ~[]byte](l *N, r *B) BytesField[N, B] {
	return BytesField[N, B]{l: l, r: r}
}

func BytesEndWithNull[N constraints.Integer, B ~[]byte](l *N, r *B) BytesField[N, B] {
	return BytesField[N, B]{l: l, r: r, nullEnd: true}
}

func BitSet[N constraints.Integer](l *N, r *bitmap.Bitmap) BitSetField[N] {
	if l == nil {
		panic("infinite bitset field is not allowed")
	}
	return BitSetField[N]{l: l, r: r}
}

func UUID(r *uuid.UUID) UUIDField {
	return UUIDField{r: r}
}

func Array[N constraints.Integer, T any](l *N, r *[]T, bind func(x *T) Field) RepeatField[N, T] {
	return RepeatField[N, T]{size: l, r: r, bind: func(x *T) []Field {
		return []Field{bind(x)}
	}}
}

func Repeat[N constraints.Integer, T any](l *N, r *[]T, bind func(x *T) []Field) RepeatField[N, T] {
	return RepeatField[N, T]{size: l, r: r, bind: bind}
}

func Const[T any](t T) *T {
	return &t
}

func Infinite() *uint8 {
	return nil
}

func Skip[T constraints.Integer](t *T) SkipField[T] {
	return SkipField[T]{l: t}
}

func Area[T constraints.Integer, H Handler](l *T, h H) AreaField[T, H] {
	return AreaField[T, H]{l: l, h: h}
}
