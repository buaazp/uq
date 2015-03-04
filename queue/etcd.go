package queue

import (
	"log"
	"strings"
	"time"
)

const (
	EtcdUqServerListKey string = "uq_servers"
	EtcdTTL             uint64 = 10
	OneSecond           uint64 = uint64(time.Second)
)

func (u *UnitedQueue) etcdRun() {
	//TODO: time duration
	ticker := time.NewTicker(time.Duration(EtcdTTL * OneSecond))
	for {
		select {
		case <-ticker.C:
			err := u.RegisterSelf()
			if err != nil {
				log.Printf("etcd register self error: %s", err)
			}
			err = u.RegisterTopics()
			if err != nil {
				log.Printf("etcd register topics error: %s", err)
			}
		case <-u.etcdStop:
			log.Printf("etcdRun stoping...")
			return
		}
	}
	log.Printf("etcdRun stoped.")
}

func (u *UnitedQueue) RegisterSelf() error {
	// log.Printf("etcdServers: %v", UqConfig.EtcdServer)
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd register self...")

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()

	resp, err := u.etcdClient.CreateDir(EtcdUqServerListKey, 0)
	if err != nil && strings.HasPrefix(err.Error(), "105") != true {
		return err
	}
	if resp != nil {
		log.Printf("etcd: %v", resp.Node)
	}

	resp, err = u.etcdClient.AddChild(EtcdUqServerListKey, u.selfAddr, EtcdTTL)
	if err != nil {
		return err
	}
	// log.Printf("etcd: %v", resp.Node)
	// resp, err = u.etcdClient.Get(EtcdUqServerListKey, false, true)
	// if err != nil {
	// 	return err
	// }
	// for i, node := range resp.Node.Nodes {
	// 	log.Printf("server-%d : %v", i, node)
	// }
	return nil
}

func (u *UnitedQueue) RegisterTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd register topic[%s]...", topic)

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()

	resp, err := u.etcdClient.CreateDir(topic, 0)
	if err != nil && strings.HasPrefix(err.Error(), "105") != true {
		return err
	}
	if resp != nil {
		log.Printf("etcd: %v", resp.Node)
	}

	resp, err = u.etcdClient.AddChild(topic, u.selfAddr, EtcdTTL)
	if err != nil {
		return err
	}
	// log.Printf("etcd: %v", resp.Node)
	// resp, err = u.etcdClient.Get(topic, false, true)
	// if err != nil {
	// 	return err
	// }
	// for i, node := range resp.Node.Nodes {
	// 	log.Printf("server-%d : %v", i, node)
	// }
	return nil
}

func (u *UnitedQueue) RegisterTopics() error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd register topics...")

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()
	u.topicsLock.RLock()
	defer u.topicsLock.RUnlock()

	for topic, _ := range u.topics {
		resp, err := u.etcdClient.CreateDir(topic, 0)
		if err != nil && strings.HasPrefix(err.Error(), "105") != true {
			return err
		}
		if resp != nil {
			log.Printf("etcd: %v", resp.Node)
		}

		resp, err = u.etcdClient.AddChild(topic, u.selfAddr, EtcdTTL)
		if err != nil {
			return err
		}
		// log.Printf("etcd: %v", resp.Node)
		// resp, err = u.etcdClient.Get(topic, false, true)
		// if err != nil {
		// 	return err
		// }
		// for i, node := range resp.Node.Nodes {
		// 	log.Printf("server-%d : %v", i, node)
		// }
	}
	return nil
}
