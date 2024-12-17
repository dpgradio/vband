package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/kreuzwerker/gva"
	"github.com/redis/go-redis/v9"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	cruntimeconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Config struct {
	LogFile       string
	RedisUrl      string
	ServiceName   string
	Namespace     string
	Port          uint64
	VarnishSecret string
}

type VarnishServer struct {
	Host   string
	Port   uint16
	Secret string
	In     chan string
	socket *gva.Connection
}

var (
	redisClient    *redis.Client
	varnishServers map[string]*VarnishServer
	serverLock     sync.Mutex
)

func parseFlags() Config {
	var config Config

	flag.StringVar(&config.LogFile, "log_file", "", "Path to log file (default: stdout)")
	flag.StringVar(&config.RedisUrl, "redis_url", "", "URL for Redis server (e.g., redis://localhost:6379/0)")
	flag.StringVar(&config.ServiceName, "varnish_service", "varnish", "Kubernetes Service name for Varnish endpoints")
	flag.StringVar(&config.Namespace, "namespace", "default", "Kubernetes namespace for Varnish service (default: default)")
	flag.Uint64Var(&config.Port, "varnish_port", 6082, "Varnish port")
	flag.StringVar(&config.VarnishSecret, "varnish_secret", "", "Secret to contact the varnish admin port (default: empty)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	// Validate required flags
	if config.RedisUrl == "" || config.ServiceName == "" {
		flag.Usage()
		log.Fatal("Error: redis_url is a required parameters.")
	} else {
		log.Printf("Starting with config:` %o", config)
	}

	return config
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

func (v *VarnishServer) StartOne() error {
	var err error
	v.socket, err = v.connect()
	if err != nil {
		return err
	}

	for {
		cmd := <-v.In
		response, err := v.socket.Cmd(cmd)
		if err != nil {
			log.Printf("cmd=\"%s\" status= err=%v\n", cmd, err)
			v.In <- cmd
			return err
		}

		log.Printf("cmd=\"%s\" status=%d body=\"%s\" err=\n", cmd, response.Status, response.Body)
	}
}

func (v *VarnishServer) connect() (*gva.Connection, error) {
	socket, err := gva.NewConnection(v.Host, v.Port, &v.Secret)
	if err != nil {
		return nil, err
	}
	socket.Keepalive(1 * time.Second)
	return socket, nil
}

func newRedisClient(connecturi string) *redis.Client {
	opts, err := redis.ParseURL(connecturi)
	if err != nil {
		panic(err)
	}

	return redis.NewClient(opts)
}

func redisListenLoop(queue string, rdb *redis.Client) error {
	ctx := context.Background()

	for {
		data, err := rdb.BLPop(ctx, 0, "queue:go:"+queue).Result()
		if err != nil {
			return err
		}

		cmd := data[1]

		serverLock.Lock()
		copiedServers := make(map[string]*VarnishServer)
		for k, v := range varnishServers {
			copiedServers[k] = v
		}
		serverLock.Unlock()

		for _, varnish := range copiedServers {
			select {
			case varnish.In <- cmd:
			default:
				log.Printf("cmd=\"%s\" status= body= err=\"Queue for %s is full\"", cmd, varnish.Host)
			}
		}
	}
}

func main() {
	config := parseFlags()

	// Log setup based on the command line argument
	if config.LogFile != "" {
		f, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
		if err != nil {
			log.Fatal("error opening log file: ", err)
		}
		log.SetOutput(f)
		defer f.Close()
	}

	varnishServers = make(map[string]*VarnishServer)

	configureKubernetesClient(config)
	redisClient = newRedisClient(config.RedisUrl)

	for {
		err := redisListenLoop("varnish", redisClient)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("type=redis err=\"%s\"\n", err)
		} else {
			return
		}
	}
}

func configureKubernetesClient(c Config) {
	clientset := kubernetes.NewForConfigOrDie(cruntimeconfig.GetConfigOrDie())

	factory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Minute, informers.WithNamespace(c.Namespace))
	endpointsInformer := factory.Core().V1().Endpoints().Informer()

	endpointsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			log.Println("Calling AddFunc")
			updateVarnishServers(obj, c)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			log.Println("Calling UpdateFunc")
			updateVarnishServers(newObj, c)
		},
		DeleteFunc: func(obj interface{}) {
			log.Println("Calling DeleteFunc")
			removeVarnishServers(obj, c)
		},
	})

	// Start the informer
	stopCh := make(chan struct{})
	defer close(stopCh)
	go endpointsInformer.Run(stopCh)

	log.Println("Started watching endpoints...")

	select {}
}

func updateVarnishServers(obj interface{}, config Config) {
	endpoints, ok := obj.(*v1.Endpoints)
	if !ok || endpoints.Name != config.ServiceName {
		return
	}

	serverLock.Lock()
	defer serverLock.Unlock()

	// fill array with all keys
	newKeys := []string{}
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			newKeys = append(newKeys, address.IP)
		}
	}

	for _, serverKey := range newKeys {
		if _, exists := varnishServers[serverKey]; !exists { // not in the slice, add new one
			server := &VarnishServer{Host: serverKey, Port: uint16(config.Port), In: make(chan string, 1024), Secret: config.VarnishSecret}
			varnishServers[serverKey] = server
			go server.Start()
			log.Printf("Added new endpoint:` %s", serverKey)
		}
	}

	for serverKey, varnishServer := range varnishServers {
		if !slices.Contains(newKeys, serverKey) { // serverkey isn't in the new array, remove the endpoint
			close(varnishServer.In)
			delete(varnishServers, serverKey)
			log.Printf("Removed endpoint:` %s", serverKey)
		}
	}

	log.Printf("Updated endpoints: %v", varnishServers)
}

func removeVarnishServers(obj interface{}, config Config) {
	endpoints, ok := obj.(*v1.Endpoints)
	if !ok || endpoints.Name != config.ServiceName {
		return
	}

	serverLock.Lock()
	defer serverLock.Unlock()

	for serverKey, varnishServer := range varnishServers {
		close(varnishServer.In)
		delete(varnishServers, serverKey)
		log.Printf("Removed endpoint:` %s", serverKey)
	}

	log.Printf("Wiped endpoints: %v", varnishServers)
}
