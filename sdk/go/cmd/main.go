package main

import (
	"github.com/galgotech/heddle-lang/sdk/go/stdplugin"
)

func main() {
	<-stdplugin.Register()
	select {}
}
