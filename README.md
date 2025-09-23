# klock
一个键级别的高性能、并发安全的 Go 语言读写锁

klock 通过为任意“键”创建独立的锁，允许操作不同数据的任务并行处理，从而在高并发场景下获得显著的性能提升

# 安装指南
```shell
go get -u github.com/xiaoqidun/klock
```

# 快速开始
```go
package main

import (
	"fmt"
	"sync"

	"github.com/xiaoqidun/klock"
)

func main() {
	// 1. 创建 klock 实例
	kl := klock.New()
	// 2. 准备一个共享的计数器和用于它的锁的键
	var counter int
	lockKey := "counter_lock"
	// 3. 模拟高并发修改计数器
	var wg sync.WaitGroup
	concurrency := 1000
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			kl.Lock(lockKey)   // 获取锁
			counter++          // 安全修改
			kl.Unlock(lockKey) // 释放锁
		}()
	}
	wg.Wait()
	// 4. 验证结果
	//    如果没有 klock 保护，由于竞态条件，counter 的最终值将是一个小于1000的不确定数字
	//    有了 klock，结果一定是 1000
	fmt.Printf("最终结果: %d\n", counter)
}
```

# 授权协议
本项目使用 [Apache License 2.0](https://github.com/xiaoqidun/klock/blob/main/LICENSE) 授权协议