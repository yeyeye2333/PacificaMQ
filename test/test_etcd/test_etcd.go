package main

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	_, err = cli.Put(ctx, "sample_key", "sample_value")
	cancel()
	if err != nil {
		fmt.Println(1, err)
	}

	var KV clientv3.KV = namespace.NewKV(cli.KV, "my-prefix/")
	_, err = KV.Put(context.Background(), "sample_key", "my-value")
	if err != nil {
		fmt.Println(2, err)
	}

	old_key, err := cli.Get(context.Background(), "sample_key")
	fmt.Println(old_key)
	if err != nil {
		fmt.Println(3, err)
	}

	new_key, err := KV.Get(context.Background(), "sample_key")
	fmt.Println(new_key)
	if err != nil {
		fmt.Println(4, err)
	}
	// result, err := KV.Txn(context.Background()).If(clientv3.Compare(clientv3.Value("sample_key"), "!=", "my-value")).Then(
	// 	clientv3.OpGet("sample_key"),
	// ).Commit()
}
