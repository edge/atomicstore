# Edge Atomic Store

This lightweight logging library is used throughout the Edge suite to standardise reporting.

Example of basic usage:

```go
package myapp

import (
  "fmt"
  "github.com/edge/atomicstore"
)

func main() {
  // Value can be any type
  v := "value"
  // Create lockable store
  store := atomicstore.New(true)
  // Insert value by key
  store.Insert("key", v)

  // Check that the value exists
  if v, ok := store.Get("key"); ok {
    // Values are stored as an Interface so must be
    // cast as the correct type
    str := v.(string)
    fmt.Println(str) // prints "value"
  }
}
```

## Blocking an action until a store changes

```go
package myapp

import (
  "time"
  "github.com/edge/atomicstore"
)

func main() {
  v := "value"
  store := atomicstore.New(true)
  
  go waitForChange(store)
  
  // Wait for a second.
  time.Sleep(time.Second)
  // Insert value by key
  store.Insert("key", v)
  // Let blocked methods know a change has occured
  store.NotifyDidChange()
}

func waitForChange(store *atomicstore.Store) {
  ctx := context.Background()
  // This will block unless the context is canceled
  <-store.WaitForChange(ctx)
  // Continue
}
```