package main

import (
	"github.com/netflix/rend/server"
	"github.com/spf13/viper"
)

func init_default_config() {
	log.Println("Initializing configuration")
	viper.SetDefault("ListenPort", 11221)
	viper.SetDefault("HttpPort", 11299)
}

func main() {
	init_default_config()
}