// Copyright 2025 肖其顿 (XIAO QI DUN)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package klock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLockUnlock 测试基本的写锁获取与释放功能。
func TestLockUnlock(t *testing.T) {
	kl := New()
	key := "test_key"
	kl.Lock(key)
	kl.Unlock(key)
}

// TestRLockRUnlock 测试基本的读锁获取与释放功能，并验证多个读锁可以共存。
func TestRLockRUnlock(t *testing.T) {
	kl := New()
	key := "test_key"
	kl.RLock(key)
	kl.RLock(key)
	kl.RUnlock(key)
	kl.RUnlock(key)
}

// TestLockBlocksRLock 测试写锁会阻塞后续的读锁请求。
func TestLockBlocksRLock(t *testing.T) {
	kl := New()
	key := "test_key"
	ch := make(chan struct{})
	kl.Lock(key)
	go func() {
		kl.RLock(key)
		kl.RUnlock(key)
		close(ch)
	}()
	select {
	case <-ch:
		t.Fatal("RLock 应该被 Lock 阻塞")
	case <-time.After(10 * time.Millisecond):
		// 预期的行为
	}
	kl.Unlock(key)
	select {
	case <-ch:
		// 预期的行为
	case <-time.After(10 * time.Millisecond):
		t.Fatal("RLock 在 Unlock 后应该被解除阻塞")
	}
}

// TestRLockBlocksLock 测试读锁会阻塞后续的写锁请求。
func TestRLockBlocksLock(t *testing.T) {
	kl := New()
	key := "test_key"
	ch := make(chan struct{})
	kl.RLock(key)
	go func() {
		kl.Lock(key)
		kl.Unlock(key)
		close(ch)
	}()
	select {
	case <-ch:
		t.Fatal("Lock 应该被 RLock 阻塞")
	case <-time.After(10 * time.Millisecond):
		// 预期的行为
	}
	kl.RUnlock(key)
	select {
	case <-ch:
		// 预期的行为
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Lock 在所有 RUnlock 后应该被解除阻塞")
	}
}

// TestConcurrentLock 测试多个 goroutine 并发获取同一个写锁的互斥性。
func TestConcurrentLock(t *testing.T) {
	kl := New()
	key := "test_key"
	var counter int32
	var wg sync.WaitGroup
	numGoroutines := 100
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kl.Lock(key)
			atomic.AddInt32(&counter, 1)
			kl.Unlock(key)
		}()
	}
	wg.Wait()
	if counter != int32(numGoroutines) {
		t.Errorf("期望计数器为 %d, 但得到 %d", numGoroutines, counter)
	}
}

// TestConcurrentRead 测试多个 goroutine 可以并发地持有读锁并读取数据。
func TestConcurrentRead(t *testing.T) {
	kl := New()
	key := "test_key"
	var wg sync.WaitGroup
	numGoroutines := 100
	kl.Lock(key)
	sharedData := 42
	kl.Unlock(key)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kl.RLock(key)
			if sharedData != 42 {
				t.Errorf("读取到错误数据: 得到 %d, 期望 42", sharedData)
			}
			kl.RUnlock(key)
		}()
	}
	wg.Wait()
}

// TestConcurrentReadWrite 测试读写锁在并发读写场景下的正确性。
func TestConcurrentReadWrite(t *testing.T) {
	kl := New()
	key := "test_key"
	var data int
	var wg sync.WaitGroup
	numGoroutines := 100
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numGoroutines; i++ {
			kl.Lock(key)
			data++
			kl.Unlock(key)
		}
	}()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numGoroutines; j++ {
				kl.RLock(key)
				_ = data // 只是读取
				kl.RUnlock(key)
			}
		}()
	}
	wg.Wait()
	if data != numGoroutines {
		t.Errorf("期望最终数据为 %d, 得到 %d", numGoroutines, data)
	}
}

// TestDifferentKeysDoNotBlock 测试对不同键的操作不会相互阻塞。
func TestDifferentKeysDoNotBlock(t *testing.T) {
	kl := New()
	key1, key2 := "key1", "key2"
	ch := make(chan struct{})
	kl.Lock(key1)
	go func() {
		kl.Lock(key2)
		kl.Unlock(key2)
		close(ch)
	}()
	select {
	case <-ch:
	// 预期的行为，不应该被阻塞
	case <-time.After(20 * time.Millisecond):
		t.Fatal("锁定不同的键不应该被阻塞")
	}
	kl.Unlock(key1)
}

// TestTryLock 测试非阻塞获取写锁的正确性。
func TestTryLock(t *testing.T) {
	kl := New()
	key := "test_key"
	if !kl.TryLock(key) {
		t.Fatal("TryLock 在键未锁定时应该成功")
	}
	if kl.TryLock(key) {
		t.Fatal("TryLock 在键已锁定时应该失败")
	}
	kl.Unlock(key)
	if !kl.TryLock(key) {
		t.Fatal("TryLock 在键解锁后应该成功")
	}
	kl.Unlock(key)
}

// TestTryRLock 测试非阻塞获取读锁的正确性。
func TestTryRLock(t *testing.T) {
	kl := New()
	key := "test_key"
	if !kl.TryRLock(key) {
		t.Fatal("TryRLock 在键未锁定时应该成功")
	}
	if !kl.TryRLock(key) {
		t.Fatal("TryRLock 在键已被读锁定时应该成功")
	}
	kl.RUnlock(key)
	kl.RUnlock(key)
	kl.Lock(key)
	if kl.TryRLock(key) {
		t.Fatal("TryRLock 在键已被写锁定时应该失败")
	}
	kl.Unlock(key)
}

// TestLockWithTimeout 测试带超时的写锁获取功能。
func TestLockWithTimeout(t *testing.T) {
	kl := New()
	key := "test_key"
	if !kl.LockWithTimeout(key, 10*time.Millisecond) {
		t.Fatal("LockWithTimeout 在键未锁定时应该立即成功")
	}
	kl.Unlock(key)
	kl.Lock(key)
	go func() {
		time.Sleep(5 * time.Millisecond)
		kl.Unlock(key)
	}()
	if !kl.LockWithTimeout(key, 20*time.Millisecond) {
		t.Fatal("LockWithTimeout 在锁于超时前被释放时应该成功")
	}
	kl.Unlock(key)
	kl.Lock(key)
	defer kl.Unlock(key)
	if kl.LockWithTimeout(key, 10*time.Millisecond) {
		t.Fatal("LockWithTimeout 在超时后应该失败")
	}
}

// TestRLockWithTimeout 测试带超时的读锁获取功能。
func TestRLockWithTimeout(t *testing.T) {
	kl := New()
	key := "test_key"
	kl.Lock(key)
	go func() {
		time.Sleep(10 * time.Millisecond)
		kl.Unlock(key)
	}()
	if !kl.RLockWithTimeout(key, 20*time.Millisecond) {
		t.Fatal("RLockWithTimeout 在锁于超时前被释放时应该成功")
	}
	kl.RUnlock(key)
	kl.Lock(key)
	defer kl.Unlock(key)
	if kl.RLockWithTimeout(key, 10*time.Millisecond) {
		t.Fatal("RLockWithTimeout 在超时后应该失败")
	}
}

// TestPanicOnUnlockOfUnlockedKey 测试对未锁定的键执行 Unlock 操作是否会引发 panic。
func TestPanicOnUnlockOfUnlockedKey(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("代码在 Unlock 未锁定的键时没有 panic")
		}
	}()
	kl := New()
	kl.Unlock("non_existent_key")
}

// TestPanicOnRUnlockOfUnlockedKey 测试对未读锁定的键执行 RUnlock 操作是否会引发 panic。
func TestPanicOnRUnlockOfUnlockedKey(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("代码在 RUnlock 未读锁定的键时没有 panic")
		}
	}()
	kl := New()
	kl.RUnlock("non_existent_key")
}

// TestStatus 测试 Status 方法是否能正确返回锁的持有者和等待者数量。
func TestStatus(t *testing.T) {
	kl := New()
	key := "test_key"
	var wg sync.WaitGroup
	holders, waiters := kl.Status(key)
	if holders != 0 || waiters != 0 {
		t.Fatalf("期望 0 个持有者和 0 个等待者, 得到 %d 和 %d", holders, waiters)
	}
	kl.Lock(key)
	holders, waiters = kl.Status(key)
	if holders != 1 || waiters != 0 {
		t.Fatalf("期望 1 个持有者和 0 个等待者, 得到 %d 和 %d", holders, waiters)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		kl.Lock(key)
		kl.Unlock(key)
	}()
	go func() {
		defer wg.Done()
		kl.RLock(key)
		kl.RUnlock(key)
	}()
	time.Sleep(10 * time.Millisecond)
	holders, waiters = kl.Status(key)
	if holders != 1 || waiters != 2 {
		t.Fatalf("期望 1 个持有者和 2 个等待者, 得到 %d 和 %d", holders, waiters)
	}
	kl.Unlock(key)
	wg.Wait()
	holders, waiters = kl.Status(key)
	if holders != 0 || waiters != 0 {
		t.Fatalf("期望解锁后有 0 个持有者和 0 个等待者, 得到 %d 和 %d", holders, waiters)
	}
}

// TestLockChurn 测试在高并发和键频繁创建销毁的情况下，引用计数是否能正常工作，防止内存泄漏。
func TestLockChurn(t *testing.T) {
	kl := New()
	numGoroutines := 100
	iterations := 1000
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key-%d-%d", gid, j%10) // 限制键的数量以增加竞争
				kl.Lock(key)
				kl.Unlock(key)
			}
		}(i)
	}
	wg.Wait()
	for i := uint32(0); i < shardCount; i++ {
		shard := kl.shards[i]
		shard.mu.Lock()
		if len(shard.locks) != 0 {
			t.Errorf("分片 %d 存在锁泄漏, 数量: %d", i, len(shard.locks))
		}
		shard.mu.Unlock()
	}
}

// --- 基准测试 ---

// BenchmarkLockUnlock_SingleKey 测试单个键上高竞争的写锁性能。
func BenchmarkLockUnlock_SingleKey(b *testing.B) {
	kl := New()
	key := "key"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.Lock(key)
			kl.Unlock(key)
		}
	})
}

// BenchmarkRLockRUnlock_SingleKey 测试单个键上高竞争的读锁性能。
func BenchmarkRLockRUnlock_SingleKey(b *testing.B) {
	kl := New()
	key := "key"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.RLock(key)
			kl.RUnlock(key)
		}
	})
}

// BenchmarkLockUnlock_MultipleKeys 测试多个键上低竞争的写锁性能。
func BenchmarkLockUnlock_MultipleKeys(b *testing.B) {
	kl := New()
	var counter uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddUint64(&counter, 1)%1024)
			kl.Lock(key)
			kl.Unlock(key)
		}
	})
}

// BenchmarkRLockRUnlock_MultipleKeys 测试多个键上低竞争的读锁性能。
func BenchmarkRLockRUnlock_MultipleKeys(b *testing.B) {
	kl := New()
	var counter uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddUint64(&counter, 1)%1024)
			kl.RLock(key)
			kl.RUnlock(key)
		}
	})
}

// BenchmarkReadWrite_SingleKey 测试单个键上混合读写的性能。
func BenchmarkReadWrite_SingleKey(b *testing.B) {
	kl := New()
	key := "key"
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.RLock(key)
			kl.RUnlock(key)
		}
	})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.Lock(key)
			kl.Unlock(key)
		}
	})
}

// BenchmarkTryLock_Contended 测试在锁已被持有的情况下，TryLock 的性能。
func BenchmarkTryLock_Contended(b *testing.B) {
	kl := New()
	key := "key"
	kl.Lock(key)
	defer kl.Unlock(key)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			kl.TryLock(key)
		}
	})
}
