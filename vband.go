package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/kreuzwerker/gva"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	cruntimeconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Config struct {
	LogFile       string
	LogLevel      string
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
	log            *zap.SugaredLogger
)

func parseFlags() Config {
	var config Config

	flag.StringVar(&config.LogFile, "log_file", "", "Path to log file (default: stdout)")
	flag.StringVar(&config.LogLevel, "log_level", "info", "loglevel (default: info)")
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
	if config.RedisUrl == "" {
		flag.Usage()
		fmt.Println("Error: redis_url is a required parameter")
		os.Exit(1)
	}

	return config
}

func (v *VarnishServer) Start() {
	log.Infow("Starting varnish endpoint", "host", v.Host)
	v.In = make(chan string, 1024)
	for {
		err := v.StartOne()
		if err != nil {
			log.Warnw("StartOne failed", "host", v.Host, "err", err)
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
			log.Warnw("Sending command failed, re-queueing", "cmd", cmd, "err", err)
			v.In <- cmd
			return err
		}

		log.Debugw("Command sent", "cmd", cmd, "status", response.Status, "body", response.Body)
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
		log.Fatalw("Error parsing Redis URI", "uri", connecturi, "err", err)
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
				log.Warnw("Queue is full when sending command", "cmd", cmd, "host", varnish.Host)
			}
		}
	}
}

func main() {
	config := parseFlags()

	// Logging setup - structured logging is hard, but nice
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(config.LogLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level '%s', defaulting to info\n", config.LogLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	cfg := zap.Config{
		Level:             level,
		Development:       false,
		Encoding:          "json", // You can use "console" for human-readable output.
		EncoderConfig:     zap.NewProductionEncoderConfig(),
		OutputPaths:       []string{},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     false,
		DisableStacktrace: false,
	}

	// Decide the output based on the logfile flag.
	if config.LogFile == "" || config.LogFile == "stdout" || config.LogFile == "-" {
		cfg.OutputPaths = []string{"stdout"}
	} else {
		cfg.OutputPaths = []string{config.LogFile}
	}

	logger, err := cfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	log = logger.Sugar()
	log.Infow("Starting up", "config", config)

	varnishServers = make(map[string]*VarnishServer)

	go configureKubernetesClient(config)

	redisClient = newRedisClient(config.RedisUrl)
	go readRedisQueue(redisClient)

	select {}
}

func readRedisQueue(redisClient *redis.Client) {
	for {
		err := redisListenLoop("varnish", redisClient)
		if err != nil {
			time.Sleep(1 * time.Second)
			log.Warn("Failed to read from redis queue", "err", err)
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
			log.Debugw("Calling AddFunc", "obj", obj)
			updateVarnishServers(obj, c)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			log.Debugw("Calling UpdateFunc", "oldObj", oldObj, "newObj", newObj)
			updateVarnishServers(newObj, c)
		},
		DeleteFunc: func(obj interface{}) {
			log.Debugw("Calling DeleteFunc", "obj", obj)
			removeVarnishServers(obj, c)
		},
	})

	// Start the informer
	stopCh := make(chan struct{})
	defer close(stopCh)

	log.Info("Started watching endpoints...")
	endpointsInformer.Run(stopCh)

	log.Info("Done watching endpoints...")
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
			log.Debugw("Added new endpoint", "endpoint", serverKey)
		}
	}

	for serverKey, varnishServer := range varnishServers {
		if !slices.Contains(newKeys, serverKey) { // serverkey isn't in the new array, remove the endpoint
			close(varnishServer.In)
			delete(varnishServers, serverKey)
			log.Debugw("Removed endpoint", "endpoint", serverKey)
		}
	}

	log.Infow("Updated endpoints", "endpoints", varnishServers)
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
		log.Debugw("Removed endpoint", "endpoint", serverKey)
	}

	log.Info("Wiped endpoints")
}
