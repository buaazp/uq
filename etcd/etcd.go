package etcd

import (
	"fmt"
	"log"
	"strings"

	. "github.com/buaazp/uq/utils"
	"github.com/coreos/go-etcd/etcd"
)

func RegisterUq() error {
	// log.Printf("etcdServers: %v", UqConfig.EtcdServer)
	client := etcd.NewClient(UqConfig.EtcdServer)
	resp, err := client.CreateDir(EtcdUqServerListKey, 0)
	if err != nil && strings.HasPrefix(err.Error(), "105") != true {
		return err
	}
	if resp != nil {
		log.Printf("etcd: %v", resp.Node)
	}

	selfAddress := fmt.Sprintf("%s:%d", UqConfig.Ip, UqConfig.Port)
	resp, err = client.AddChild(EtcdUqServerListKey, selfAddress, EtcdTTL)
	if err != nil {
		return err
	}
	log.Printf("etcd: %v", resp.Node)
	resp, err = client.Get(EtcdUqServerListKey, false, true)
	if err != nil {
		return err
	}
	for i, node := range resp.Node.Nodes {
		log.Printf("server-%d : %v", i, node)
	}
	return nil
}

func RegisterTopic(topic string) error {
	client := etcd.NewClient(UqConfig.EtcdServer)
	resp, err := client.CreateDir(topic, 0)
	if err != nil && strings.HasPrefix(err.Error(), "105") != true {
		return err
	}
	if resp != nil {
		log.Printf("etcd: %v", resp.Node)
	}

	selfAddress := fmt.Sprintf("%s:%d", UqConfig.Ip, UqConfig.Port)
	resp, err = client.AddChild(topic, selfAddress, EtcdTTL)
	if err != nil {
		return err
	}
	log.Printf("etcd: %v", resp.Node)
	resp, err = client.Get(topic, false, true)
	if err != nil {
		return err
	}
	for i, node := range resp.Node.Nodes {
		log.Printf("server-%d : %v", i, node)
	}
	return nil
}
