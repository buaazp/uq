package queue

import (
	"errors"
	"log"
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

	err := u.pullTopics()
	if err != nil {
		log.Printf("pull topics error: %s", err)
	}
	go u.scanRun()

	registerDelay := time.NewTicker(EtcdRegisterDelay)
	select {
	case <-registerDelay.C:
		log.Printf("entry succ. etcdRun first register...")
		u.register()
	case <-u.etcdStop:
		log.Printf("entry failed. etcdRun stoping...")
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
			log.Printf("etcdRun stoping...")
			quit = true
			break
		}
	}

	u.unRegister()
	log.Printf("etcdRun stoped.")
}

func (u *UnitedQueue) pullTopics() error {
	log.Printf("etcd pull topics start...")

	resp, err := u.etcdClient.Get(u.etcdKey+"/topics", false, true)
	if err != nil {
		return err
	}

	for _, node := range resp.Node.Nodes {
		if node.Dir {
			err := u.nodeTopic(node)
			if err != nil {
				log.Printf("nodeTopic error: %s", err)
				continue
			}
			for _, nd := range node.Nodes {
				err := u.nodeLine(nd)
				if err != nil {
					log.Printf("nodeTopic error: %s", err)
					continue
				}
			}
		}
	}
	return nil
}

func (u *UnitedQueue) nodeTopic(node *etcd.Node) error {
	key := node.Key
	parts := strings.Split(key, "/")
	log.Printf("parts: %v len: %d", parts, len(parts))
	if len(parts) != 4 {
		return errors.New(key + " parts illegal")
	}
	topic := parts[3]
	log.Printf("topic: %s", topic)
	_, ok := u.topics[topic]
	if ok {
		return nil
	}

	return u.createTopic(topic, true)
}

func (u *UnitedQueue) nodeLine(node *etcd.Node) error {
	key := node.Key
	parts := strings.Split(key, "/")
	log.Printf("parts: %v len: %d", parts, len(parts))
	if len(parts) != 5 {
		return errors.New(key + " parts illegal")
	}
	topic := parts[3]
	log.Printf("topic: %s", topic)
	line := parts[4]
	log.Printf("line: %s", line)
	value := node.Value
	log.Printf("recycle: %s", value)
	recycle, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("parse duration error: %s", err)
		recycle = 0
	}

	t, ok := u.topics[topic]
	if !ok {
		return errors.New(topic + " not existed")
	}
	_, ok = t.lines[line]
	if ok {
		return nil
	}

	return t.createLine(line, recycle, true)
}

func (u *UnitedQueue) scanRun() {
	log.Printf("etcd scanRun start...")

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
			log.Printf("scanRun stoping...")
			quit = true
		}
	}

	close(stopChan)
	log.Printf("scanRun stoped.")
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
			log.Printf("etcd watch error: %v", err)
			break
		}
		log.Printf("resp: %v", resp)
		if resp.Action == "create" {
			err := u.nodeTopic(resp.Node)
			if err != nil {
				log.Printf("nodeTopic error: %s", err)
			}
		} else if resp.Action == "set" {
			err := u.nodeLine(resp.Node)
			if err != nil {
				log.Printf("nodeTopic error: %s", err)
			}
		}
	}

	// log.Printf("watchRun stoped.")
}

func (u *UnitedQueue) register() error {
	err := u.registerSelf()
	if err != nil {
		log.Printf("etcd register self error: %s", err)
	}
	// err = u.registerTopics()
	// if err != nil {
	// 	log.Printf("etcd register topics error: %s", err)
	// }
	return nil
}

func (u *UnitedQueue) unRegister() error {
	err := u.unRegisterSelf()
	if err != nil {
		log.Printf("etcd unregister self error: %s", err)
	}
	// err = u.unRegisterTopics()
	// if err != nil {
	// 	log.Printf("etcd unregister topics error: %s", err)
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
	log.Printf("etcd unregister self...")

	key := u.etcdKey + "/servers/" + u.selfAddr
	_, err := u.etcdClient.Delete(key, true)
	if err != nil {
		return err
	}
	return nil
}

// func (u *UnitedQueue) registerTopics() error {
// 	// log.Printf("etcd register topics...")

// 	u.topicsLock.RLock()
// 	defer u.topicsLock.RUnlock()

// 	for topic, _ := range u.topics {
// 		key := topic + "/" + u.selfAddr
// 		_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (u *UnitedQueue) unRegisterTopics() error {
// 	log.Printf("etcd unregister topics...")

// 	u.topicsLock.RLock()
// 	defer u.topicsLock.RUnlock()

// 	for topic, _ := range u.topics {
// 		key := topic + "/" + u.selfAddr
// 		_, err := u.etcdClient.Delete(key, true)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func (u *UnitedQueue) registerTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	// log.Printf("etcd register topic[%s]...", topic)

	key := topic + "/" + u.selfAddr
	_, err := u.etcdClient.Set(key, EtcdUqServerListValue, EtcdTTL)
	if err != nil {
		return err
	}

	topicKey := u.etcdKey + "/topics/" + topic
	_, err = u.etcdClient.CreateDir(topicKey, 0)
	if err != nil {
		return err
	}
	return nil
}

func (u *UnitedQueue) unRegisterTopic(topic string) error {
	if u.etcdClient == nil {
		return nil
	}
	log.Printf("etcd unregister topic[%s]...", topic)

	key := topic + "/" + u.selfAddr
	_, err := u.etcdClient.Delete(key, true)
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
