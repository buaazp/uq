package queue

import (
	"log"
	"time"
)

const (
	EtcdUqServerListKey   string = "UQLIST"
	EtcdUqServerListValue string = "online"
	EtcdTTL               uint64 = 10
	OneSecond             uint64 = uint64(time.Second)
)

func (u *UnitedQueue) etcdRun() {
	//TODO: time duration
	ticker := time.NewTicker(time.Duration(EtcdTTL * OneSecond))
	quit := false
	for !quit {
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
			quit = true
			break
		}
	}
	err := u.UnRegisterSelf()
	if err != nil {
		log.Printf("etcd unregister self error: %s", err)
	}
	err = u.UnRegisterTopics()
	if err != nil {
		log.Printf("etcd unregister topics error: %s", err)
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

	key := EtcdUqServerListKey + "/" + u.selfAddr
	_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
	if err != nil {
		return err
	}
	// if resp != nil {
	// 	log.Printf("etcd: %v", resp.Node)
	// }
	// resp, err = u.etcdClient.Get(EtcdUqServerListKey, false, true)
	// if err != nil {
	// 	return err
	// }
	// for i, node := range resp.Node.Nodes {
	// 	log.Printf("server-%d : %v", i, node)
	// }
	return nil
}

func (u *UnitedQueue) UnRegisterSelf() error {
	// log.Printf("etcdServers: %v", UqConfig.EtcdServer)
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd unregister self...")

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()

	key := EtcdUqServerListKey + "/" + u.selfAddr
	_, err := u.etcdClient.Delete(key, true)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) RegisterTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd register topic[%s]...", topic)

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()

	key := topic + "/" + u.selfAddr
	_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
	if err != nil {
		return err
	}
	// if resp != nil {
	// 	log.Printf("etcd: %v", resp.Node)
	// }
	// resp, err = u.etcdClient.Get(topic, false, true)
	// if err != nil {
	// 	return err
	// }
	// for i, node := range resp.Node.Nodes {
	// 	log.Printf("server-%d : %v", i, node)
	// }
	return nil
}

func (u *UnitedQueue) UnRegisterTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd unregister topic[%s]...", topic)

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()

	key := topic + "/" + u.selfAddr
	_, err := u.etcdClient.Delete(key, true)
	if err != nil {
		return err
	}
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
		key := topic + "/" + u.selfAddr
		_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
		if err != nil {
			return err
		}
		// if resp != nil {
		// 	log.Printf("etcd: %v", resp.Node)
		// }
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

func (u *UnitedQueue) UnRegisterTopics() error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd unregister topics...")

	u.etcdLock.Lock()
	defer u.etcdLock.Unlock()
	u.topicsLock.RLock()
	defer u.topicsLock.RUnlock()

	for topic, _ := range u.topics {
		key := topic + "/" + u.selfAddr
		_, err := u.etcdClient.Delete(key, true)
		if err != nil {
			return err
		}
	}
	return nil
}
