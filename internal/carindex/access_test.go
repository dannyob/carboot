// SPDX-License-Identifier: BSD-3-Clause

package carindex

import (
	"reflect"
	"testing"
)

func TestAccessCollectorRecordsDistinctCarsAndBytes(t *testing.T) {
	c := NewAccessCollector()
	c.record("a.car", 100)
	c.record("b.car", 50)
	c.record("a.car", 25) // duplicate car, more bytes

	gotCars := c.Cars()
	wantCars := []string{"a.car", "b.car"}
	if !reflect.DeepEqual(gotCars, wantCars) {
		t.Errorf("Cars() = %v, want %v", gotCars, wantCars)
	}

	if got, want := c.Bytes(), int64(175); got != want {
		t.Errorf("Bytes() = %d, want %d", got, want)
	}
}
