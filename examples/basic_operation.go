package main

import (
	sirius "Sirius"
	"fmt"
)

func main() {
	opts := sirius.DefaultOptions
	opts.DirPath = "/tmp/sirius"
	db, err := sirius.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("hello"), []byte("sirius"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("val: %s\n", val)

	err = db.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}

}
