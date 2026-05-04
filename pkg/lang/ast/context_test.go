package ast

import (
	"fmt"
	"sync"
	"testing"
)

func TestASTContext_ConcurrentRead(t *testing.T) {
	ctx := AcquireASTContext()
	defer ReleaseASTContext(ctx)

	str := "hello concurrency"
	ref := ctx.AddString(str)

	const numReaders = 100
	const numReads = 1000
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numReads; j++ {
				got := ctx.GetString(ref)
				if got != str {
					t.Errorf("expected %q, got %q", str, got)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestASTContext_ConcurrentReadWrite(t *testing.T) {
	ctx := AcquireASTContext()
	defer ReleaseASTContext(ctx)

	var wg sync.WaitGroup
	const numOps = 1000

	wg.Add(2)

	// Writer
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			ctx.AddString(fmt.Sprintf("string-%d", i))
		}
	}()

	// Reader
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			// Just verify we can call GetString without crashing/racing
			ctx.GetString(StringRef{Start: 0, End: 0})
		}
	}()

	wg.Wait()
}
