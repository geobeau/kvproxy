package memcached

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

// Handler is an handler for executing memcached requests
type Handler struct {
	mcPool Pool
}

var singleton *Handler

// InitMemcachedConn init a pool of memcached workers
func InitMemcachedConn(server string) error {
	if singleton == nil {
		singleton = &Handler{
			mcPool: NewPool("localhost:11213", 8, 1000),
		}
	}
	return nil
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
	task := SetTask{
		Cmd: cmd,
		ErrorOut:  errorOut,
	}
	h.mcPool.SetWorkQueue<- task
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
	task := GetTask{
		Cmd: cmd,
		DataOut: dataOut,
		ErrorOut:  errorOut,
	}
	h.mcPool.GetWorkQueue<- task
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