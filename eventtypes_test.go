// File: eventtypes_test.go

package grnoti

import (
	"sync"
	"testing"
)

func TestEventType_IsValid(t *testing.T) {
	if !EventTypeCustom.IsValid() {
		t.Error("EventTypeCustom.IsValid() = false, want true")
	}
	if !EventType("anything_a_consumer_makes_up").IsValid() {
		t.Error("an unregistered application-specific EventType must still be structurally valid")
	}
	if EventType("").IsValid() {
		t.Error(`EventType("").IsValid() = true, want false`)
	}
}

func TestEventTypeRegistry_PreSeeded(t *testing.T) {
	reg := NewEventTypeRegistry()
	for _, want := range []EventType{
		EventTypeCustom, EventTypeSystemAlert, EventTypeAccountVerification,
		EventTypePasswordReset, EventTypeGenericTransactional, EventTypeGenericMarketing,
	} {
		if _, ok := reg.Lookup(want); !ok {
			t.Errorf("Lookup(%s) not found in a freshly-constructed registry", want)
		}
	}
}

func TestEventTypeRegistry_RegisterAndLookup(t *testing.T) {
	reg := NewEventTypeRegistry()
	custom := EventType("order_shipped")
	meta := EventTypeMetadata{DefaultPriority: PriorityHigh, Category: CategoryTransactional, Transactional: true}

	if err := reg.Register(custom, meta); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := reg.Lookup(custom)
	if !ok {
		t.Fatal("Lookup after Register: not found")
	}
	if got != meta {
		t.Fatalf("Lookup() = %+v, want %+v", got, meta)
	}
}

func TestEventTypeRegistry_RegisterInvalid(t *testing.T) {
	reg := NewEventTypeRegistry()
	if err := reg.Register("", EventTypeMetadata{}); err != ErrInvalidEventType {
		t.Fatalf("Register(\"\") error = %v, want ErrInvalidEventType", err)
	}
}

func TestEventTypeRegistry_All(t *testing.T) {
	reg := NewEventTypeRegistry()
	all := reg.All()
	if len(all) < 6 {
		t.Fatalf("All() returned %d types, want at least the 6 pre-seeded ones", len(all))
	}
}

func TestEventTypeRegistry_ConcurrentAccess(t *testing.T) {
	reg := NewEventTypeRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t := EventType("concurrent")
			_ = reg.Register(t, EventTypeMetadata{DefaultPriority: PriorityNormal})
			_, _ = reg.Lookup(t)
			_ = reg.All()
		}(i)
	}
	wg.Wait()
}
