package main

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func main() {
	pool := memory.NewGoAllocator()

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	b.AppendValues(
		[]float64{1, 2, 3, -1, 4, 5},
		[]bool{true, true, true, false, true, true},
	)

	arr := b.NewFloat64Array()
	defer arr.Release()

	fmt.Printf("array = %v\n", arr)

	sli := array.NewSlice(arr, 2, 5).(*array.Float64)
	defer sli.Release()

	fmt.Printf("slice = %v\n", sli)

}
