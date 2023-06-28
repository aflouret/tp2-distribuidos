package main

import (
	"fmt"
	"time"
	"tp2/replier/common"
)


func main() {

	fmt.Println("Creating replier")

	replier := common.NewReplier()
	err := replier.Run()
	if err != nil {
		fmt.Printf("Error accepting connection: %v\n", err)
		return
	}

	// Working time
	time.Sleep(time.Second * 300)

	// System failre
	err = replier.Stop()
	if err != nil {
		fmt.Printf("Error accepting connection: %v\n", err)
		return
	}

	fmt.Println("Exiting program")

}
