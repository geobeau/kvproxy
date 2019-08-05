package memcached

import (
	"runtime"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/netflix/rend/common"
)

type MemcachedPool struct {
	workers int
	queueSize int
	getWorkQueue chan MemcachedGetTask
	setWorkQueue chan MemcachedSetTask
}

type MemcachedGetTask struct {
	cmd common.GetRequest
	dataOut chan common.GetResponse
	errorOut chan error
}

type MemcachedSetTask struct {
	cmd common.SetRequest
	errorOut chan error
}

func NewMemcachedPool(workers , queueSize int) MemcachedPool {
	getWorkQueue := make(chan MemcachedGetTask, queueSize)
	setWorkQueue := make(chan MemcachedSetTask, queueSize)
	pool := MemcachedPool{
		workers: workers,
		queueSize: queueSize,
		getWorkQueue: getWorkQueue,
		setWorkQueue: setWorkQueue,
	}
	for i := 0; i < workers; i++ {
		go pool.worker()
	}
	return pool
}

func (p *MemcachedPool) worker(){
	mc := memcache.New("localhost:11211")
	for {
		fmt.Println(runtime.NumGoroutine())
		select {
			case task := <-p.getWorkQueue:
				p.get(task, mc)
			case task := <-p.setWorkQueue:
				p.set(task, mc)
		}
	}
}

func (p *MemcachedPool) get(task MemcachedGetTask, mc *memcache.Client) {
	for idx, key := range task.cmd.Keys {
		item, err := mc.Get(string(key))
		if err == memcache.ErrCacheMiss {
			task.dataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  task.cmd.Quiet[idx],
				Opaque: task.cmd.Opaques[idx],
				Key:    key,
				Data:   nil,
			}			
		} else if err != nil {
			task.errorOut<-err
		} else {
			task.dataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  task.cmd.Quiet[idx],
				Opaque: task.cmd.Opaques[idx],
				Flags:  0,
				Key:    key,
				Data:   item.Value,
			}
		}
	}
	close(task.dataOut)
	close(task.errorOut)
}

func (p *MemcachedPool) set(task MemcachedSetTask, mc *memcache.Client) {
	item := &memcache.Item{
		Key: string(task.cmd.Key),
		Value: task.cmd.Data,
		Flags: task.cmd.Flags,
		Expiration: int32(task.cmd.Exptime),
	}
	task.errorOut<- mc.Set(item)
}