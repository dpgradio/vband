package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kreuzwerker/gva"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"log"
	"os"
	"strings"
	"time"
)

var (
	config_file = flag.String("c", "", "YML config file")
	config      Config
)

type Config struct {
	LogFile        string           `mapstructure:"log_file"`
	RedisUrl       string           `mapstructure:"redis_url"`
	RedisSentinels []string         `mapstructure:"redis_sentinels"`
	VarnishServers []*VarnishServer `mapstructure:"varnish_servers"`
}

type VarnishServer struct {
	Host   string `mapstructure:"host"`
	Port   uint16 `mapstructure:"port"`
	Secret string `mapstructure:"secret"`

	Test   string
	In     chan string
	socket *gva.Connection
}

func (v *VarnishServer) Start() {
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

func (v *VarnishServer) StartOne() (err error) {
	v.socket, err = v.connect()
	if err != nil {
		return err
	}

	for {
		cmd := <-v.In
		response, e := v.socket.Cmd(cmd)
		if e != nil {
			log.Printf("cmd=\"%s\" status= err=%v\n", cmd, e)
			v.In <- cmd
			return e
		}

		log.Printf("cmd=\"%s\" status=%d body=\"%s\" err=\n", cmd, response.Status, response.Body)
	}
}

func (v *VarnishServer) connect() (socket *gva.Connection, err error) {
	socket, err = gva.NewConnection(v.Host, v.Port, &v.Secret)
	if err != nil {
		return
	}
	socket.Keepalive(1 * time.Second)

	return
}

func newRedisClient(c Config) *redis.Client {
	opts, e := redis.ParseURL(c.RedisUrl)
	if e != nil {
		panic(e)
	}

	if len(c.RedisSentinels) > 0 {
		// Get the hostname from host:port
		masterName := strings.Split(opts.Addr, ":")[0]

		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: c.RedisSentinels,
			DB:            opts.DB,
		})
	} else {
		return redis.NewClient(opts)
	}
}

func redisListenLoop(queue string, servers []*VarnishServer) (err error) {
	rdb := newRedisClient(config)
	ctx := context.Background()

	for {
		data, e := rdb.BLPop(ctx, 0, "queue:go:"+queue).Result()
		if e != nil {
			return e
		}

		cmd := data[1]

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
		f, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
		if err != nil {
			log.Fatal("error opening log file: ", err)
		}

		log.SetOutput(f)
		defer f.Close()
	}

	for idx, server := range config.VarnishServers {
		server.Test = fmt.Sprintf("main%d", idx)
		go server.Start()
	}

	for {
		err := redisListenLoop("varnish", config.VarnishServers)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("type=redis err=\"%s\"\n", err)
		} else {
			return
		}
	}
}
