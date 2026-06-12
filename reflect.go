package stats

import (
	"reflect"
	"time"
	"unsafe"
)

type structField struct {
	typ reflect.Type
	off uintptr
}

func (f structField) pointer(ptr unsafe.Pointer) unsafe.Pointer {
	return unsafe.Add(ptr, f.off)
}

func (f structField) bool(ptr unsafe.Pointer) bool {
	return *(*bool)(f.pointer(ptr))
}

func (f structField) int(ptr unsafe.Pointer) int {
	return *(*int)(f.pointer(ptr))
}

func (f structField) int8(ptr unsafe.Pointer) int8 {
	return *(*int8)(f.pointer(ptr))
}

func (f structField) int16(ptr unsafe.Pointer) int16 {
	return *(*int16)(f.pointer(ptr))
}

func (f structField) int32(ptr unsafe.Pointer) int32 {
	return *(*int32)(f.pointer(ptr))
}

func (f structField) int64(ptr unsafe.Pointer) int64 {
	return *(*int64)(f.pointer(ptr))
}

func (f structField) uint(ptr unsafe.Pointer) uint {
	return *(*uint)(f.pointer(ptr))
}

func (f structField) uint8(ptr unsafe.Pointer) uint8 {
	return *(*uint8)(f.pointer(ptr))
}

func (f structField) uint16(ptr unsafe.Pointer) uint16 {
	return *(*uint16)(f.pointer(ptr))
}

func (f structField) uint32(ptr unsafe.Pointer) uint32 {
	return *(*uint32)(f.pointer(ptr))
}

func (f structField) uint64(ptr unsafe.Pointer) uint64 {
	return *(*uint64)(f.pointer(ptr))
}

func (f structField) uintptr(ptr unsafe.Pointer) uintptr {
	return *(*uintptr)(f.pointer(ptr))
}

func (f structField) float32(ptr unsafe.Pointer) float32 {
	return *(*float32)(f.pointer(ptr))
}

func (f structField) float64(ptr unsafe.Pointer) float64 {
	return *(*float64)(f.pointer(ptr))
}

func (f structField) duration(ptr unsafe.Pointer) time.Duration {
	return *(*time.Duration)(f.pointer(ptr))
}

func (f structField) string(ptr unsafe.Pointer) string {
	return *(*string)(f.pointer(ptr))
}

var (
	boolType     = reflect.TypeFor[bool]()
	intType      = reflect.TypeFor[int]()
	int8Type     = reflect.TypeFor[int8]()
	int16Type    = reflect.TypeFor[int16]()
	int32Type    = reflect.TypeFor[int32]()
	int64Type    = reflect.TypeFor[int64]()
	uintType     = reflect.TypeFor[uint]()
	uint8Type    = reflect.TypeFor[uint8]()
	uint16Type   = reflect.TypeFor[uint16]()
	uint32Type   = reflect.TypeFor[uint32]()
	uint64Type   = reflect.TypeFor[uint64]()
	uintptrType  = reflect.TypeFor[uintptr]()
	float32Type  = reflect.TypeFor[float32]()
	float64Type  = reflect.TypeFor[float64]()
	durationType = reflect.TypeFor[time.Duration]()
	stringType   = reflect.TypeFor[string]()
)
