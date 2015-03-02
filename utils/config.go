package utils

import "strings"

var UqConfig *Config = new(Config)

type Config struct {
	Ip         string
	Port       int
	EtcdServer []string
}

func SetIp(ip string) {
	UqConfig.Ip = ip
}

func SetPort(port int) {
	UqConfig.Port = port
}

func SetEtcdServer(etcdServer string) {
	if etcdServer == "" {
		return
	}
	etcdServers := strings.Split(etcdServer, ",")
	UqConfig.EtcdServer = etcdServers
}
