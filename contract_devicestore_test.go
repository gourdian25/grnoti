// File: contract_devicestore_test.go

package grnoti

import (
	"context"
	"testing"
)

// testDeviceStoreContract is the shared behavioral contract every DeviceStore
// backend must satisfy identically, per gourdiantoken's own table-driven
// cross-backend testing convention (see docs/plan/grnoti-plan.md §4, §7) —
// grnoti's flat, no-subpackage layout means there's no import-cycle reason
// for a separate conformance package.
//
// newStore takes the currently-executing *testing.T (not a captured outer
// one) so a backend-unavailable skip inside it calls Skip on the actual
// running subtest — calling an outer *testing.T's Skip/Fatal from inside a
// nested t.Run's goroutine targets the wrong test's bookkeeping.
func testDeviceStoreContract(t *testing.T, newStore func(t *testing.T) DeviceStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("SaveThenGetActiveDevices", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.SaveDevice(ctx, Device{FID: testFID("contract-t1"), UserID: "contract-u1", Platform: PlatformAndroid}); err != nil {
			t.Fatalf("SaveDevice: %v", err)
		}
		devices, err := store.GetActiveDevices(ctx, "contract-u1")
		if err != nil {
			t.Fatalf("GetActiveDevices: %v", err)
		}
		if len(devices) != 1 || devices[0].FID != testFID("contract-t1") {
			t.Fatalf("GetActiveDevices() = %v, want [contract-t1]", devices)
		}
	})

	t.Run("SaveRejectsInvalidFID", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.SaveDevice(ctx, Device{FID: legacyRegistrationToken, UserID: "contract-u0"}); err != ErrInvalidFID {
			t.Fatalf("SaveDevice(registration token) = %v, want ErrInvalidFID", err)
		}
		if devices, err := store.GetActiveDevices(ctx, "contract-u0"); err != nil || len(devices) != 0 {
			t.Fatalf("GetActiveDevices after a refused SaveDevice = (%v, %v), want empty", devices, err)
		}
	})

	t.Run("GetActiveDevicesForUnknownUserIsEmptyNotError", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		devices, err := store.GetActiveDevices(ctx, "contract-never-seen")
		if err != nil {
			t.Fatalf("GetActiveDevices(unknown user) error = %v, want nil", err)
		}
		if len(devices) != 0 {
			t.Fatalf("GetActiveDevices(unknown user) = %v, want empty", devices)
		}
	})

	t.Run("MarkInvalidRemovesFromActiveList", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveDevice(ctx, Device{FID: testFID("contract-t2"), UserID: "contract-u2"})
		if err := store.MarkInvalid(ctx, testFID("contract-t2")); err != nil {
			t.Fatalf("MarkInvalid: %v", err)
		}
		devices, _ := store.GetActiveDevices(ctx, "contract-u2")
		if len(devices) != 0 {
			t.Fatalf("GetActiveDevices after MarkInvalid = %v, want empty", devices)
		}
	})

	t.Run("MarkInvalidOnNonexistentIsNotAnError", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.MarkInvalid(ctx, "contract-never-existed"); err != nil {
			t.Fatalf("MarkInvalid(nonexistent) error = %v, want nil", err)
		}
	})

	t.Run("DeleteDeviceRemovesIt", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveDevice(ctx, Device{FID: testFID("contract-t3"), UserID: "contract-u3"})
		if err := store.DeleteDevice(ctx, testFID("contract-t3")); err != nil {
			t.Fatalf("DeleteDevice: %v", err)
		}
		devices, _ := store.GetActiveDevices(ctx, "contract-u3")
		if len(devices) != 0 {
			t.Fatalf("GetActiveDevices after DeleteDevice = %v, want empty", devices)
		}
		if err := store.DeleteDevice(ctx, testFID("contract-t3")); err != nil {
			t.Fatalf("DeleteDevice(already deleted) error = %v, want nil", err)
		}
	})

	t.Run("GetActiveDevicesByAnonymousID", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveDevice(ctx, Device{FID: testFID("contract-t4"), AnonymousID: "contract-a1"})
		devices, err := store.GetActiveDevicesByAnonymousID(ctx, "contract-a1")
		if err != nil || len(devices) != 1 {
			t.Fatalf("GetActiveDevicesByAnonymousID() = (%v, %v), want 1 device", devices, err)
		}
	})

	t.Run("GetActiveDevicesBatchGroupsByUser", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.SaveDevice(ctx, Device{FID: testFID("contract-t5"), UserID: "contract-u5"})
		_ = store.SaveDevice(ctx, Device{FID: testFID("contract-t6"), UserID: "contract-u6"})
		out, err := store.GetActiveDevicesBatch(ctx, []string{"contract-u5", "contract-u6", "contract-u-missing"})
		if err != nil {
			t.Fatalf("GetActiveDevicesBatch: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("GetActiveDevicesBatch() = %v, want exactly 2 users present", out)
		}
	})
}

func TestDeviceStore_Contract(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		testDeviceStoreContract(t, func(t *testing.T) DeviceStore { return NewMemoryDeviceStore() })
	})
	t.Run("Mongo", func(t *testing.T) {
		testDeviceStoreContract(t, func(t *testing.T) DeviceStore {
			collection := contractCollectionName(t)
			store, err := NewMongoDeviceStore(MongoDeviceStoreConfig{
				URI: testMongoURI, Database: "grnoti_test", CollectionName: collection,
			})
			if err != nil {
				t.Skipf("MongoDB not available, skipping: %v", err)
			}
			cleanupContractCollection(t, "grnoti_test", collection)
			return store
		})
	})
	t.Run("Postgres", func(t *testing.T) {
		testDeviceStoreContract(t, func(t *testing.T) DeviceStore {
			ensureTestPostgresSchema(t)
			store, err := NewPostgresDeviceStore(PostgresConfig{DSN: testPostgresDSN})
			if err != nil {
				t.Skipf("PostgreSQL not available, skipping: %v", err)
			}
			cleanupContractRows(t, "grnoti_devices", "user_id")
			cleanupContractRows(t, "grnoti_devices", "anonymous_id")
			return store
		})
	})
}
