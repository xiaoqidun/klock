// Copyright 2025 肖其顿
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

package klock_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/xiaoqidun/klock"
)

// ExampleKeyLock_Lock 演示了如何使用 Lock 和 Unlock 来保护对共享资源的并发写操作。
func ExampleKeyLock_Lock() {
	kl := klock.New()
	var wg sync.WaitGroup
	// 共享数据
	sharedData := make(map[string]int)
	// 模拟10个并发请求更新同一个键的值
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 为 "dataKey" 加锁，确保同一时间只有一个 goroutine 能修改其值
			kl.Lock("dataKey")
			sharedData["dataKey"]++
			kl.Unlock("dataKey")
		}()
	}
	wg.Wait()
	fmt.Printf("键 'dataKey' 的最终计数值是: %d\n", sharedData["dataKey"])
	// Output: 键 'dataKey' 的最终计数值是: 10
}

// ExampleKeyLock_RLock 演示了多个 goroutine 如何同时获取读锁以并发地读取数据。
func ExampleKeyLock_RLock() {
	kl := klock.New()
	sharedResource := "这是一个共享资源"
	// 启动5个 goroutine 并发读取数据
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			kl.RLock("resourceKey")
			fmt.Printf("Goroutine %d 读取资源: %s\n", gid, sharedResource)
			kl.RUnlock("resourceKey")
		}(i)
	}
	wg.Wait()
	// 注意：由于 goroutine 调度顺序不确定，输出的顺序可能不同。
	// 但这个示例的核心是展示它们可以并发执行，而不会像写锁一样互相等待。
}

// ExampleKeyLock_TryLock 演示了如何尝试非阻塞地获取锁。
// 如果锁已被占用，它不会等待，而是立即返回 false。
func ExampleKeyLock_TryLock() {
	kl := klock.New()
	key := "resource_key"
	// 第一次尝试，应该成功
	if kl.TryLock(key) {
		fmt.Println("第一次 TryLock 成功获取锁")
		// 第二次尝试，因为锁已被持有，所以应该失败
		if !kl.TryLock(key) {
			fmt.Println("第二次 TryLock 失败，因为锁已被占用")
		}
		kl.Unlock(key)
	}
	// Output:
	// 第一次 TryLock 成功获取锁
	// 第二次 TryLock 失败，因为锁已被占用
}

// ExampleKeyLock_LockWithTimeout 演示了如何在指定时间内尝试获取锁，避免无限期等待。
func ExampleKeyLock_LockWithTimeout() {
	kl := klock.New()
	key := "task_key"
	// 主 goroutine 先获取锁
	kl.Lock(key)
	fmt.Println("主 goroutine 持有锁")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 尝试在 10ms 内获取锁，此时主 goroutine 仍持有锁，所以会失败
		fmt.Println("Goroutine 尝试在 10ms 内获取锁...")
		if !kl.LockWithTimeout(key, 10*time.Millisecond) {
			fmt.Println("Goroutine 获取锁超时，任务取消")
		}
	}()
	// 等待 20ms 后释放锁
	time.Sleep(20 * time.Millisecond)
	kl.Unlock(key)
	fmt.Println("主 goroutine 释放锁")
	wg.Wait()
	// Output:
	// 主 goroutine 持有锁
	// Goroutine 尝试在 10ms 内获取锁...
	// Goroutine 获取锁超时，任务取消
	// 主 goroutine 释放锁
}

// ExampleKeyLock_Status 演示了如何查询一个键的当前锁定状态。
func ExampleKeyLock_Status() {
	kl := klock.New()
	key := "status_key"
	var wg sync.WaitGroup
	kl.Lock(key)
	// 启动两个 goroutine 在后台等待锁
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kl.Lock(key)
			time.Sleep(1 * time.Millisecond)
			kl.Unlock(key)
		}()
	}
	// 等待一小段时间，确保两个 goroutine 已经处于等待状态
	time.Sleep(10 * time.Millisecond)
	holders, waiters := kl.Status(key)
	fmt.Printf("当前持有者: %d, 等待者: %d\n", holders, waiters)
	kl.Unlock(key)
	wg.Wait()
	holders, waiters = kl.Status(key)
	fmt.Printf("所有任务完成后，持有者: %d, 等待者: %d\n", holders, waiters)
	// Output:
	// 当前持有者: 1, 等待者: 2
	// 所有任务完成后，持有者: 0, 等待者: 0
}
