package memcached

import (
	"testing"
	"github.com/netflix/rend/common"
)

func TestInitMemcachedConn(t *testing.T) {
	if singleton != nil {
		t.Errorf("MemcachedConn is initialized too early")
	}
	InitMemecachedConn()
	if singleton == nil {
		t.Errorf("MemcachedConn didn't initialize")
	}	
}

func TestNewHandlerReturnSingleton(t *testing.T) {
	InitMemecachedConn()
	handler, _ := NewHandler()
	if handler != singleton {
		t.Errorf("NewHandler doesn't return the singleton")
	}
}

func TestCloseIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	handler, _ := NewHandler()
	if nil != handler.Close() {
		t.Errorf("Please add tests")
	}
}

func TestAddIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Add(req) {
		t.Errorf("Please add tests")
	}
}

func TestReplaceIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Replace(req) {
		t.Errorf("Please add tests")
	}
}

func TestAppendIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Append(req) {
		t.Errorf("Please add tests")
	}
}

func TestPrependIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Prepend(req) {
		t.Errorf("Please add tests")
	}
}

func TestGATIsNotImplemented(t *testing.T) {
	InitMemecachedConn()
	req := common.GATRequest{}
	handler, _ := NewHandler()
	_, error := handler.GAT(req)
	if nil != error {
		t.Errorf("Please add tests")
	}
}