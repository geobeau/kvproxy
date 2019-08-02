package memcached

import (
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type Handler struct {
	mc	*memcache.Client
}

var singleton *Handler

func InitMemecahedConn() error {
	if singleton == nil {
		singleton = &Handler{
			mc: memcache.New("localhost:11211"),
		}	
	}
	return nil

}

func NewHandler() (handlers.Handler, error) {
	return singleton, nil
}

func (h *Handler) Close() error {
	return nil
}

func (h *Handler) Set(cmd common.SetRequest) error {
	item := &memcache.Item{
		Key: string(cmd.Key),
		Value: cmd.Data,
		Flags: cmd.Flags,
		Expiration: int32(cmd.Exptime),
	}
	return h.mc.Set(item)
}

func (h *Handler) Add(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Replace(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Append(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Prepend(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error)

	for idx, key := range cmd.Keys {
		item, err := h.mc.Get(string(key))
		if err == memcache.ErrCacheMiss {
			dataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    key,
				Data:   nil,
			}			
		} else {
			dataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Flags:  0,
				Key:    key,
				Data:   item.Value,
			}
		}
	}
	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error)
	return dataOut, errorOut
}

func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{
		Miss:   true,
		Opaque: cmd.Opaque,
		Key:    cmd.Key,
	}, nil
}

func (h *Handler) Delete(cmd common.DeleteRequest) error {
	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {

	return nil
}