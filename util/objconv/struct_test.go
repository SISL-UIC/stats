package objconv

import (
	"reflect"
	"testing"
)

func TestMakeStructField(t *testing.T) {
	type A struct{ A int }
	type a struct{ a int }
	type B struct{ A }
	type b struct{ a } //nolint:unused // embedded unexported field is read via reflection below

	tests := []struct {
		s reflect.StructField
		f structField
	}{
		{
			s: reflect.TypeFor[A]().Field(0),
			f: structField{
				index: []int{0},
				name:  "A",
			},
		},

		{
			s: reflect.TypeFor[a]().Field(0),
			f: structField{
				index: []int{0},
				name:  "a",
			},
		},

		{
			s: reflect.TypeFor[B]().Field(0),
			f: structField{
				index: []int{0},
				name:  "A",
			},
		},

		{
			s: reflect.TypeFor[b]().Field(0),
			f: structField{
				index: []int{0},
				name:  "a",
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			f := makeStructField(test.s, map[reflect.Type]*structType{})
			f.decode = nil // function types are not comparable
			f.encode = nil

			if !reflect.DeepEqual(test.f, f) {
				t.Errorf("%#v != %#v", test.f, f)
			}
		})
	}
}
