//go:build unit
// +build unit

/*
Copyright 2024 Google LLC

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

package adapter

import "testing"

func TestGlobalState_StoreAndLoad(t *testing.T) {
	cache, _ := NewDefaultGlobalState(maxGlobalStateSize)

	cache.Store("key1", "val1")

	t.Run("LoadExistingEntry", func(t *testing.T) {
		val, ok := cache.Load("key1")
		if !ok {
			t.Fatal("Expected entry to be found")
		}
		if val != "val1" {
			t.Errorf("Expected val1, got %v", val)
		}
	})

	t.Run("LoadNonExistingEntry", func(t *testing.T) {
		_, ok := cache.Load("id3")
		if ok {
			t.Fatal("Expected entry not to be found")
		}
	})
}

func TestGlobalState_LRUEviction(t *testing.T) {
	cache, _ := NewDefaultGlobalState(2)
	cache.Store("key1", "val1")
	cache.Store("key2", "val2")
	cache.Store("key3", "val3") // Should evict key1

	t.Run("EvictedEntry", func(t *testing.T) {
		_, ok := cache.Load("key1")
		if ok {
			t.Fatal("Expected key1 to be evicted")
		}
	})

	t.Run("RemainingEntry", func(t *testing.T) {
		_, ok := cache.Load("key2")
		if !ok {
			t.Fatal("Expected key2 to be present")
		}
		_, ok = cache.Load("key3")
		if !ok {
			t.Fatal("Expected key3 to be present")
		}
	})
}
