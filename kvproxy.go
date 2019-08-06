package main

import (
	"os"
	"log"
	"os/signal"
	"syscall"
	"net/http"
	_ "net/http/pprof"

	"github.com/geobeau/kvproxy/orcas"
	mc_cluster "github.com/geobeau/kvproxy/handlers/memcached/cluster"

	"github.com/netflix/rend/server"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/spf13/viper"
)

func initDefaultConfig() {
	log.Println("Initializing configuration")
	viper.SetDefault("ListenPort", 11222)
	viper.SetDefault("HttpPort", 11299)
}

func main() {
	initDefaultConfig()
	err := mc_cluster.InitMemcachedCluster([]string{"localhost:11213", "localhost:11214"})
	if err != nil {
		log.Fatal(err)
	}

	h1 := mc_cluster.NewHandler
	h2 := handlers.NilHandler

	l := server.TCPListener(viper.GetInt("ListenPort"))
	ps := []protocol.Components{binprot.Components, textprot.Components}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Graceful stop
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGKILL)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		_ = <-gracefulStop
		log.Println("[INFO] Gracefully stopping server")
		os.Exit(0)
	}()
	log.Println("Exposing service on", viper.GetInt("ListenPort"))
	server.ListenAndServe(l, ps, server.Default, orcas.Kvproxy, h1, h2)
}