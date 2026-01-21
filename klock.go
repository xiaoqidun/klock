// Copyright 2025-2026 肖其顿 (XIAO QI DUN)
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

// Package klock 提供了一个基于键的高性能、并发安全的 Go 语言读写锁库。
//
// 它允许为不同的键（如资源ID、任务标识符）创建独立的读写锁，
// 在高并发场景下，通过分片技术减少锁竞争，从而显著提升性能。
// 该库能够自动管理锁对象的生命周期，防止在长时间运行的服务中发生内存泄漏。
//
// 功能完善，支持阻塞式锁定 (Lock/RLock)、非阻塞式锁定 (TryLock/TryRLock)、
// 带超时的锁定 (LockWithTimeout/RLockWithTimeout) 以及锁状态查询 (Status)。
package klock

import (
	"sync"
	"time"
)

// shardCount 是分片数量，必须是2的幂次方以获得最佳性能。
const shardCount uint32 = 256

// fnv-1a 哈希算法使用的常量
const (
	prime32  = 16777619
	offset32 = 2166136261
)

// Config 用于自定义 KeyLock 实例的行为。
// 通过此配置可以调整超时锁定功能的轮询策略，以适应不同的负载场景。
type Config struct {
	// MaxPollInterval 是超时锁定功能中轮询的最大等待间隔。
	MaxPollInterval time.Duration
	// InitialPollInterval 是超时锁定功能中轮询的初始等待间隔。
	InitialPollInterval time.Duration
}

// defaultConfig 返回一个包含推荐默认值的配置。
func defaultConfig() Config {
	return Config{
		MaxPollInterval:     16 * time.Millisecond,
		InitialPollInterval: 1 * time.Millisecond,
	}
}

// KeyLock 提供了基于字符串键的高性能、分片读写锁功能。
type KeyLock struct {
	shards []*keyLockShard
	config Config
}

// lockEntry 包含一个读写锁和两个引用计数器，用于安全地自动清理。
type lockEntry struct {
	rw      sync.RWMutex
	holders int // 持有锁的 goroutine 数量
	waiters int // 正在等待获取锁的 goroutine 数量
}

// keyLockShard 代表一个独立的锁分片。
// 每个分片都有自己的互斥锁来保护其内部的锁映射。
type keyLockShard struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

// New 使用默认配置创建一个新的高性能 KeyLock 实例。
func New() *KeyLock {
	return NewWithConfig(defaultConfig())
}

// NewWithConfig 使用自定义配置创建一个新的高性能 KeyLock 实例。
func NewWithConfig(config Config) *KeyLock {
	kl := &KeyLock{
		shards: make([]*keyLockShard, shardCount),
		config: config,
	}
	for i := uint32(0); i < shardCount; i++ {
		kl.shards[i] = &keyLockShard{
			locks: make(map[string]*lockEntry),
		}
	}
	return kl
}

// hash 使用 FNV-1a 算法计算字符串的32位哈希值，此实现避免了不必要的内存分配。
func hash(s string) uint32 {
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// getShard 根据键的哈希值返回对应的分片。
func (kl *KeyLock) getShard(key string) *keyLockShard {
	return kl.shards[hash(key)&(shardCount-1)]
}

// commitLock 将一个成功获取到锁的 goroutine 从“等待者”状态转换成“持有者”状态。
func (kl *KeyLock) commitLock(key string, le *lockEntry) {
	shard := kl.getShard(key)
	shard.mu.Lock()
	le.waiters--
	le.holders++
	shard.mu.Unlock()
}

// cancelLock 在获取锁失败（如 TryLock 失败或超时）后，安全地撤销“等待”状态。
// 如果撤销后锁不再被需要，则会将其清理。
func (kl *KeyLock) cancelLock(key string, le *lockEntry) {
	shard := kl.getShard(key)
	shard.mu.Lock()
	le.waiters--
	if le.holders == 0 && le.waiters == 0 {
		delete(shard.locks, key)
	}
	shard.mu.Unlock()
}

// prepareLock 获取或创建指定键的锁条目，并增加其等待者计数。
// 这是所有加锁操作的第一步，确保了锁条目在后续操作中不会被意外回收。
func (kl *KeyLock) prepareLock(key string) *lockEntry {
	shard := kl.getShard(key)
	shard.mu.Lock()
	le, ok := shard.locks[key]
	if !ok {
		le = &lockEntry{}
		shard.locks[key] = le
	}
	le.waiters++
	shard.mu.Unlock()
	return le
}

// releaseLock 是一个通用的解锁辅助函数。
// 它处理持有者计数、锁条目的生命周期管理（清理）以及实际的读写锁释放。
func (kl *KeyLock) releaseLock(key string, unlockFunc func(*sync.RWMutex)) {
	shard := kl.getShard(key)
	shard.mu.Lock()
	le, ok := shard.locks[key]
	if !ok {
		shard.mu.Unlock()
		panic("klock: unlock of unlocked key")
	}
	le.holders--
	isLast := le.holders == 0 && le.waiters == 0
	if isLast {
		delete(shard.locks, key)
	}
	shard.mu.Unlock()
	unlockFunc(&le.rw)
}

// Lock 为指定的键获取一个写锁。
// 如果锁已被其他 goroutine 持有，则此调用将阻塞直到锁可用。
func (kl *KeyLock) Lock(key string) {
	le := kl.prepareLock(key)
	le.rw.Lock()
	kl.commitLock(key, le)
}

// TryLock 尝试非阻塞地为指定的键获取写锁。
// 如果成功获取锁，则返回 true；否则立即返回 false。
func (kl *KeyLock) TryLock(key string) bool {
	le := kl.prepareLock(key)
	if le.rw.TryLock() {
		kl.commitLock(key, le)
		return true
	}
	kl.cancelLock(key, le)
	return false
}

// LockWithTimeout 尝试在给定的时间内为指定的键获取写锁。
// 如果在超时前成功获取锁，则返回 true；否则返回 false。
func (kl *KeyLock) LockWithTimeout(key string, timeout time.Duration) bool {
	le := kl.prepareLock(key)
	// 立即尝试一次，以避免在锁可用时产生不必要的延迟。
	if le.rw.TryLock() {
		kl.commitLock(key, le)
		return true
	}
	var acquired bool
	defer func() {
		if !acquired {
			kl.cancelLock(key, le)
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	pollInterval := kl.config.InitialPollInterval
	maxPollInterval := kl.config.MaxPollInterval
	for {
		select {
		case <-timer.C:
			return false
		default:
			time.Sleep(pollInterval)
			if le.rw.TryLock() {
				kl.commitLock(key, le)
				acquired = true
				return true
			}
			pollInterval *= 2
			if pollInterval > maxPollInterval {
				pollInterval = maxPollInterval
			}
		}
	}
}

// RLock 为指定的键获取一个读锁。
// 如果锁已被其他 goroutine 持有写锁，则此调用将阻塞直到锁可用。
// 多个 goroutine 可以同时持有同一个键的读锁。
func (kl *KeyLock) RLock(key string) {
	le := kl.prepareLock(key)
	le.rw.RLock()
	kl.commitLock(key, le)
}

// TryRLock 尝试非阻塞地为指定的键获取读锁。
// 如果成功获取锁，则返回 true；否则立即返回 false。
func (kl *KeyLock) TryRLock(key string) bool {
	le := kl.prepareLock(key)
	if le.rw.TryRLock() {
		kl.commitLock(key, le)
		return true
	}
	kl.cancelLock(key, le)
	return false
}

// RLockWithTimeout 尝试在给定的时间内为指定的键获取读锁。
// 如果在超时前成功获取锁，则返回 true；否则返回 false。
func (kl *KeyLock) RLockWithTimeout(key string, timeout time.Duration) bool {
	le := kl.prepareLock(key)
	// 立即尝试一次
	if le.rw.TryRLock() {
		kl.commitLock(key, le)
		return true
	}
	var acquired bool
	defer func() {
		if !acquired {
			kl.cancelLock(key, le)
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	pollInterval := kl.config.InitialPollInterval
	maxPollInterval := kl.config.MaxPollInterval
	for {
		select {
		case <-timer.C:
			return false
		default:
			time.Sleep(pollInterval)
			if le.rw.TryRLock() {
				kl.commitLock(key, le)
				acquired = true
				return true
			}
			pollInterval *= 2
			if pollInterval > maxPollInterval {
				pollInterval = maxPollInterval
			}
		}
	}
}

// Unlock 释放指定键的写锁。
// 如果对未锁定的键调用 Unlock，将会引发 panic。
func (kl *KeyLock) Unlock(key string) {
	kl.releaseLock(key, (*sync.RWMutex).Unlock)
}

// RUnlock 释放指定键的读锁。
// 如果对未进行读锁定的键调用 RUnlock，将会引发 panic。
func (kl *KeyLock) RUnlock(key string) {
	kl.releaseLock(key, (*sync.RWMutex).RUnlock)
}

// Status 返回指定键的当前锁状态。
// 它返回两个值：持有该锁的 goroutine 数量，以及正在等待获取该锁的 goroutine 数量。
func (kl *KeyLock) Status(key string) (holders int, waiters int) {
	shard := kl.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if le, ok := shard.locks[key]; ok {
		return le.holders, le.waiters
	}
	return 0, 0
}
