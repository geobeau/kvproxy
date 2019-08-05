package memcached

import (
	"testing"
	"errors"
	"github.com/netflix/rend/common"
)

func TestInitMemcachedConn(t *testing.T) {
	if singleton != nil {
		t.Errorf("MemcachedConn is initialized too early")
	}
	InitMemcachedConn()
	if singleton == nil {
		t.Errorf("MemcachedConn didn't initialize")
	}	
}

func TestNewHandlerReturnSingleton(t *testing.T) {
	InitMemcachedConn()
	handler, _ := NewHandler()
	if handler != singleton {
		t.Errorf("NewHandler doesn't return the singleton")
	}
}

func TestGetRequestAreSendToPool(t *testing.T) {
	InitMemcachedConn()
	singleton.mcPool.getWorkQueue = make(chan GetTask, 1)

	req := common.GetRequest{
		Keys: [][]byte{[]byte("test")},
	}

	handler, _ := NewHandler()

	resultChan, _ := handler.Get(req)
	task := <-singleton.mcPool.getWorkQueue
	dataPayload := []byte("data")
	task.dataOut<- common.GetResponse{
		Data: dataPayload,
	}

	response := <-resultChan
	if string(response.Data) != string(dataPayload) {
		t.Errorf("Get requests are not properly transmitted to the pool")
	}
}

func TestSetRequestAreSendToPool(t *testing.T) {
	InitMemcachedConn()
	singleton.mcPool.setWorkQueue = make(chan SetTask, 1)
	key := []byte("test")
	data := []byte("datatest")
	req := common.SetRequest{
		Key: key,
		Data: data,
	}

	handler, _ := NewHandler()

	var task SetTask

	go func() {
		task = <-singleton.mcPool.setWorkQueue
		task.errorOut<- nil
	}()

	handler.Set(req)

	if string(task.cmd.Data) != string(data) {
		t.Errorf("Set requests don't properly transmit data to pool")
	}
	if string(task.cmd.Key) != string(key) {
		t.Errorf("Set requests don't properly transmit keys to pool")
	}
}

func TestSetRequestReturnErrorsFromPool(t *testing.T) {
	InitMemcachedConn()
	singleton.mcPool.setWorkQueue = make(chan SetTask, 1)

	req := common.SetRequest{
		Key: []byte("test"),
	}

	handler, _ := NewHandler()

	errorTest := errors.New("errortest")
	go func() {
		task := <-singleton.mcPool.setWorkQueue
		task.errorOut<- errorTest
	}()

	err := handler.Set(req)

	if err != errorTest {
		t.Errorf("Errors are not properly returned")
	}
}

func TestCloseIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	handler, _ := NewHandler()
	if nil != handler.Close() {
		t.Errorf("Please add tests")
	}
}

func TestAddIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Add(req) {
		t.Errorf("Please add tests")
	}
}

func TestReplaceIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Replace(req) {
		t.Errorf("Please add tests")
	}
}

func TestAppendIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Append(req) {
		t.Errorf("Please add tests")
	}
}

func TestPrependIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.SetRequest{}
	handler, _ := NewHandler()
	if nil != handler.Prepend(req) {
		t.Errorf("Please add tests")
	}
}

func TestGATIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.GATRequest{}
	handler, _ := NewHandler()
	_, error := handler.GAT(req)
	if nil != error {
		t.Errorf("Please add tests")
	}
}

func TestTouchIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.TouchRequest{}
	handler, _ := NewHandler()
	error := handler.Touch(req)
	if nil != error {
		t.Errorf("Please add tests")
	}
}

func TestDeleteIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.DeleteRequest{}
	handler, _ := NewHandler()
	error := handler.Delete(req)
	if nil != error {
		t.Errorf("Please add tests")
	}
}

func TestGetEIsNotImplemented(t *testing.T) {
	InitMemcachedConn()
	req := common.GetRequest{}
	handler, _ := NewHandler()
	_, errorOut := handler.GetE(req)
	if nil != <-errorOut {
		t.Errorf("Please add tests")
	}
}