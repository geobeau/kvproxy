package memcached

import (
	"testing"
	"net"
	"github.com/netflix/rend/common"
)

const testServer = "localhost:11213"

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", testServer)
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}
	h := &Handler{
		mcPool: NewMemcachedPool("localhost:11213", 8, 100),
	}
	testWithClient(t, h)
}

func testWithClient(t *testing.T, h *Handler) {
	testGetMissIsProperlySetWithClient(t, h)
	testSetIsWorkingWithoutErrorWithClient(t, h)
}

func testGetMissIsProperlySetWithClient(t *testing.T, h *Handler) {
	getCmd := common.GetRequest{
		Keys: [][]byte{[]byte("test")},
		Opaques: []uint32{0},
		Quiet: []bool{false},
	}
	dataOut := make(chan common.GetResponse, 1)
	errOut := make(chan error, 1)
	task := MemcachedGetTask{
		cmd: getCmd,
		dataOut: dataOut,
		errorOut: errOut,
	}
	h.mcPool.getWorkQueue<-task
	resp := <-dataOut
	if resp.Miss != true {
		t.Errorf("Misses are not properly reported")
	}
}

func testSetIsWorkingWithoutErrorWithClient(t *testing.T, h *Handler) {
	key := []byte("test")
	data := []byte("data")
	setCmd := common.SetRequest{
		Key: key,
		Data: data,
		Exptime: 0,
	}
	errOut := make(chan error, 1)
	task := MemcachedSetTask{
		cmd: setCmd,
		errorOut: errOut,
	}
	h.mcPool.setWorkQueue<-task
	resp := <-errOut
	if resp != nil {
		t.Errorf("Set doesn't work as expected %s", resp)
	}


	getCmd := common.GetRequest{
		Keys: [][]byte{key},
		Opaques: []uint32{0},
		Quiet: []bool{false},
	}
	errOut = make(chan error, 1)
	dataOut := make(chan common.GetResponse, 1)
	getTask := MemcachedGetTask{
		cmd: getCmd,
		dataOut: dataOut,
		errorOut: errOut,
	}
	h.mcPool.getWorkQueue<-getTask
	getResp := <-dataOut
	if getResp.Miss == true {
		t.Errorf("Set didn't work on memcached")
	}
	if string(getResp.Data) != string(data) {
		t.Errorf("Value get from memcached is incorrect")
	}
}
