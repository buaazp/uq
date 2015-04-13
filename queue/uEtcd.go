package queue

import (
	// "log"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

const (
	EtcdUqServerListValue string        = "online"
	EtcdTTL               uint64        = 60
	OneSecond             uint64        = uint64(time.Second)
	EtcdWatchDelay        time.Duration = 3 * time.Second
	EtcdRegisterDelay     time.Duration = 3 * time.Second
)

func (u *UnitedQueue) etcdRun() {
	if u.etcdClient == nil {
		return
	}

	u.wg.Add(1)
	defer u.wg.Done()

	u.pullTopics()
	// err := u.pullTopics()
	// if err != nil {
	// 	log.Printf("pull topics error: %s", err)
	// }
	go u.scanRun()

	registerDelay := time.NewTicker(EtcdRegisterDelay)
	select {
	case <-registerDelay.C:
		// log.Printf("entry succ. etcdRun first register...")
		u.register()
	case <-u.etcdStop:
		// log.Printf("entry failed. etcdRun stoping...")
		return
	}

	ticker := time.NewTicker(time.Duration(EtcdTTL * OneSecond))
	quit := false
	for !quit {
		select {
		case <-ticker.C:
			// log.Printf("etcdRun ticked.")
			u.register()
		case <-u.etcdStop:
			// log.Printf("etcdRun stoping...")
			quit = true
			break
		}
	}

	u.unRegister()
	// log.Printf("etcdRun stoped.")
}

func (u *UnitedQueue) pullTopics() error {
	// log.Printf("etcd pull topics start...")

	resp, err := u.etcdClient.Get(u.etcdKey+"/topics", false, true)
	if err != nil {
		return err
	}

	for _, node := range resp.Node.Nodes {
		if node.Dir {
			err := u.nodeCreate(node)
			if err != nil {
				// log.Printf("nodeCreate error: %s", err)
				continue
			}
			for _, nd := range node.Nodes {
				err := u.nodeCreate(nd)
				if err != nil {
					// log.Printf("nodeCreate error: %s", err)
					continue
				}
			}
		}
	}
	return nil
}

// func (u *UnitedQueue) nodeCreate(node *etcd.Node) error {
// 	key := node.Key
// 	log.Printf("etcd key: %s", key)
// 	name := strings.TrimPrefix(key, "/"+u.etcdKey+"/topics/")
// 	log.Printf("name: %s", name)

// 	return u.create(name, "", true)
// }

func (u *UnitedQueue) nodeCreate(node *etcd.Node) error {
	// key: /uq/topics/foo/z
	key := node.Key
	name := strings.TrimPrefix(key, "/"+u.etcdKey+"/topics/")
	recycle := node.Value

	return u.create(name, recycle, true)
}

func (u *UnitedQueue) scanRun() {
	// log.Printf("etcd scanRun start...")

	u.wg.Add(1)
	defer u.wg.Done()

	stopChan := make(chan bool)
	succChan := make(chan bool, 1)
	succChan <- false
	watchDelay := time.NewTicker(EtcdWatchDelay)
	quit := false
	for !quit {
		select {
		case <-watchDelay.C:
			// log.Printf("watchRun ticked.")
			select {
			case succStatus := <-succChan:
				if succStatus == false {
					// log.Printf("watchRun not succ.")
					go u.watchRun(succChan, stopChan)
				}
			default:
				// log.Printf("watchRun succ. just passed.")
			}
		case <-u.etcdStop:
			// log.Printf("scanRun stoping...")
			quit = true
		}
	}

	close(stopChan)
	// log.Printf("scanRun stoped.")
}

func (u *UnitedQueue) watchRun(succChan, stopChan chan bool) {
	// log.Printf("etcd watchRun start...")

	u.wg.Add(1)
	defer u.wg.Done()

	for {
		resp, err := u.etcdClient.Watch(u.etcdKey+"/topics", 0, true, nil, stopChan)
		if err != nil {
			if strings.Contains(err.Error(), "stop channel") {
				close(succChan)
			} else {
				succChan <- false
			}
			// log.Printf("etcd watch error: %v", err)
			break
		}
		// log.Printf("resp: %v", resp)
		if resp.Action == "create" || resp.Action == "set" {
			u.nodeCreate(resp.Node)
			// err := u.nodeCreate(resp.Node)
			// if err != nil {
			// 	log.Printf("nodeCreate error: %s", err)
			// }
		} else if resp.Action == "delete" {
			u.nodeRemove(resp.Node)
			// err := u.nodeRemove(resp.Node)
			// if err != nil {
			// 	log.Printf("nodeRemove error: %s", err)
			// }
		}
	}

	// log.Printf("watchRun stoped.")
}

func (u *UnitedQueue) nodeRemove(node *etcd.Node) error {
	// key: /uq/topics/foo/z
	key := node.Key
	name := strings.TrimPrefix(key, "/"+u.etcdKey+"/topics/")

	return u.remove(name, true)
}

func (u *UnitedQueue) register() error {
	u.registerSelf()
	// err := u.registerSelf()
	// if err != nil {
	// 	log.Printf("etcd register self error: %s", err)
	// }

	return nil
}

func (u *UnitedQueue) unRegister() error {
	u.unRegisterSelf()
	// err := u.unRegisterSelf()
	// if err != nil {
	// 	log.Printf("etcd unregister self error: %s", err)
	// }

	return nil
}

func (u *UnitedQueue) registerSelf() error {
	// log.Printf("etcd register self...")

	key := u.etcdKey + "/servers/" + u.selfAddr
	_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) unRegisterSelf() error {
	// log.Printf("etcd unregister self...")

	key := u.etcdKey + "/servers/" + u.selfAddr
	_, err := u.etcdClient.Delete(key, true)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) registerTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	// log.Printf("etcd register topic[%s]...", topic)

	topicKey := u.etcdKey + "/topics/" + topic
	_, err := u.etcdClient.CreateDir(topicKey, 0)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) unRegisterTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	// log.Printf("etcd unregister topic[%s]...", topic)

	topicKey := u.etcdKey + "/topics/" + topic
	_, err := u.etcdClient.Delete(topicKey, true)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) registerLine(topic, line, recycle string) error {
	if u.etcdClient == nil {
		return nil
	}
	// log.Printf("etcd register topic[%s]...", topic)

	lineKey := u.etcdKey + "/topics/" + topic + "/" + line
	_, err := u.etcdClient.Set(lineKey, recycle, 0)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) unRegisterLine(topic, line string) error {
	if u.etcdClient == nil {
		return nil
	}
	// log.Printf("etcd register topic[%s]...", topic)

	lineKey := u.etcdKey + "/topics/" + topic + "/" + line
	_, err := u.etcdClient.Delete(lineKey, false)
	if err != nil {
		return err
	}
	return nil
}
