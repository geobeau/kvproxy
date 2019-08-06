package memcached

import (
	"github.com/geobeau/kvproxy/handlers/memcached"

	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

// Handler is an handler for executing memcached requests
type Handler struct {
	cluster Cluster
}

// Cluster represents a map of pool and the ringhash functions
type Cluster struct {
	pools map[string] memcached.Pool
	ring *hashring.HashRing
}

var singleton *Handler

// InitMemcachedCluster init a pool of memcached workers
func InitMemcachedCluster(servers []string) error {
	if singleton == nil {
		cluster, err := bootstrapCluster(servers)
		if err != nil {
			return err
		}
		singleton = &Handler{
			cluster: cluster,
		}
	}
	return nil
}

func bootstrapCluster(servers []string) (Cluster, error) {
	var serverName string
	pools := make(map[string] memcached.Pool)
	for server := range servers {
		serverName = servers[server]
		pools[serverName] = memcached.NewPool(serverName, 4, 1000)
	}
	hasher := murmur3.New32()
	ring, err := hashring.NewWithHash(servers, hasher)
	if err != nil {
		return Cluster{}, err
	}
	return Cluster{
		pools: pools,
		ring: ring,
	}, nil
}

// NewHandler return a memcached handler
func NewHandler() (handlers.Handler, error) {
	return singleton, nil
}

// Close the handler
func (h *Handler) Close() error {
	return nil
}

// Set perform a set request
func (h *Handler) Set(cmd common.SetRequest) error {
	errorOut := make(chan error, 1)
	task := memcached.SetTask{
		Cmd: cmd,
		ErrorOut:  errorOut,
	}
	poolName, _ := h.cluster.ring.GetNode(string(cmd.Key))
	h.cluster.pools[poolName].SetWorkQueue <- task
	return <-errorOut
}

// Add perform an add request: Not implemented
func (h *Handler) Add(cmd common.SetRequest) error {
	return nil
}

// Replace perform a replace: Not implemented
func (h *Handler) Replace(cmd common.SetRequest) error {
	return nil
}

// Append perform an append: Not implemented
func (h *Handler) Append(cmd common.SetRequest) error {
	return nil
}

// Prepend perform a prepend: Not implemented
func (h *Handler) Prepend(cmd common.SetRequest) error {
	return nil
}

// Get perform a get request
func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error, len(cmd.Keys))
	
	// Break multiget into multiple get requests
	// this trade batching for simplicity and parallel requesting
	var poolName string
	var subCmd common.GetRequest
	var task memcached.GetTask
	for i := range cmd.Keys {
		poolName, _ = h.cluster.ring.GetNode(string(cmd.Keys[i]))
		subCmd = common.GetRequest{
			Keys: [][]byte{cmd.Keys[i]},
			Opaques: cmd.Opaques,
			Quiet: cmd.Quiet,
			NoopEnd: cmd.NoopEnd,
			NoopOpaque: cmd.NoopOpaque,
		}
		task = memcached.GetTask{
			Cmd: subCmd,
			DataOut: dataOut,
			ErrorOut:  errorOut,
		}
		h.cluster.pools[poolName].GetWorkQueue <- task
	}
	
	return dataOut, errorOut
}

// GetE perform a gete request: Not implemented
func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error, 1)
	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

// GAT perform a gat request: Not implemented
func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{
		Miss:   true,
		Opaque: cmd.Opaque,
		Key:    cmd.Key,
	}, nil
}

// Delete perform a delete request: Not implemented
func (h *Handler) Delete(cmd common.DeleteRequest) error {
	return nil
}

// Touch perform a touch request: Not implemented
func (h *Handler) Touch(cmd common.TouchRequest) error {
	return nil
}