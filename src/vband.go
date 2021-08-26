package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/kreuzwerker/gva"
	"fmt"
	"log"
	"time"
	"flag"
	"github.com/spf13/viper"
	"os"
)

var (
	config_file = flag.String("c", "", "YML config file")
	config Config
)

type Config struct {
	LogFile string `mapstructure:"log_file"`
	RedisUrl string `mapstructure:"redis_url"`
	VarnishServers []*VarnishServer `mapstructure:"varnish_servers"`
}

type VarnishServer struct {
	Host string `mapstructure:"host"`
	Port uint16 `mapstructure:"port"`
	Secret string `mapstructure:"secret"`

	Test string
	In chan string
	socket *gva.Connection
}

func (v *VarnishServer) Start () {
	v.In = make(chan string, 1024)

	for {
		err := v.StartOne()
		if err != nil {
			fmt.Printf("type=varnish host=%s err=\"%s\"\n", v.Host, err)
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
	}
}

func (v *VarnishServer) StartOne () (err error) {
	v.socket, err = v.connect()
	if err != nil {
		return err
	}

	for {
		cmd := <- v.In
		response, e := v.socket.Cmd(cmd)
		if e != nil {
			log.Printf("cmd=\"%s\" status= err=%v\n", cmd, e)
			v.In <- cmd
			return e
		}

		log.Printf("cmd=\"%s\" status=%d body=\"%s\" err=\n", cmd, response.Status, response.Body)
	}
}

func (v *VarnishServer) connect () (socket *gva.Connection, err error) {
	socket, err = gva.NewConnection(v.Host, v.Port, &v.Secret)
	if err != nil {
		return
	}
	socket.Keepalive(1 * time.Second)

	return
}

func redisListenLoop(queue string, servers []*VarnishServer) (err error) {
	c, e := redis.DialURL(config.RedisUrl)
	if e != nil {
		return e
	}
	defer c.Close()

	for {
		data, e := redis.Values(c.Do("BLPOP", "queue:go:" + queue, 0))
		if e != nil {
			return e
		}
		cmd, err := redis.String(data[1], err)

		if err != nil {
			return e
		}

		for _, varnish := range servers {
			select {
			case varnish.In <- cmd:
				// All OK
			default:
				log.Printf("cmd=\"%s\" status= body= err=\"Queue for %s is full\"", cmd, varnish.Host)
			}
		}
	}
}

func main() {
	flag.Parse()

	viper.SetConfigFile(*config_file)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Issue while reading config file: %s\n", err)
	}
	viper.Unmarshal(&config)
	if config.LogFile != "" {
		f, err := os.OpenFile(config.LogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0660)
		if err != nil { log.Fatal("error opening log file: ", err) }

		log.SetOutput(f)
		defer f.Close()
	}

	for idx, server := range config.VarnishServers {
		server.Test = fmt.Sprintf("main%d", idx)
		go server.Start()
	}

	for {
		err := redisListenLoop("varnish", config.VarnishServers)
		if (err != nil) {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("type=redis err=\"%s\"\n", err)
		} else {
			return
		}
	}
}
