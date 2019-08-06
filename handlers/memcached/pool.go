package memcached

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/netflix/rend/common"
)

// Pool describe a pool for memcached connectors
type Pool struct {
	workers int
	queueSize int
	GetWorkQueue chan GetTask
	SetWorkQueue chan SetTask
}

// GetTask a get task that can be processed by workers
type GetTask struct {
	Cmd common.GetRequest
	DataOut chan common.GetResponse
	ErrorOut chan error
}


// SetTask a set task that can be processed by workers
type SetTask struct {
	Cmd common.SetRequest
	ErrorOut chan error
}

// NewPool return a pool of worker and chans to interact with
func NewPool(memcached string, workers , queueSize int) Pool {
	GetWorkQueue := make(chan GetTask, queueSize)
	SetWorkQueue := make(chan SetTask, queueSize)
	pool := Pool{
		workers: workers,
		queueSize: queueSize,
		GetWorkQueue: GetWorkQueue,
		SetWorkQueue: SetWorkQueue,
	}
	for i := 0; i < workers; i++ {
		mc := memcache.New(memcached)
		go pool.worker(mc)
	}
	return pool
}

func (p *Pool) worker(mc *memcache.Client){
	for {
		select {
			case task := <-p.GetWorkQueue:
				p.get(task, mc)
			case task := <-p.SetWorkQueue:
				p.set(task, mc)
		}
	}
}

func (p *Pool) get(task GetTask, mc *memcache.Client) {
	for idx, key := range task.Cmd.Keys {
		item, err := mc.Get(string(key))
		if err == memcache.ErrCacheMiss {
			task.DataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  task.Cmd.Quiet[idx],
				Opaque: task.Cmd.Opaques[idx],
				Key:    key,
				Data:   nil,
			}			
		} else if err != nil {
			task.ErrorOut<-err
		} else {
			task.DataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  task.Cmd.Quiet[idx],
				Opaque: task.Cmd.Opaques[idx],
				Flags:  0,
				Key:    key,
				Data:   item.Value,
			}
		}
	}
}

func (p *Pool) set(task SetTask, mc *memcache.Client) {
	item := &memcache.Item{
		Key: string(task.Cmd.Key),
		Value: task.Cmd.Data,
		Flags: task.Cmd.Flags,
		Expiration: int32(task.Cmd.Exptime),
	}
	task.ErrorOut<- mc.Set(item)
}