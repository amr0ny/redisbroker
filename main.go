package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"redisbroker/service"
	"redisbroker/store"
	"strconv"
	"strings"
)

const defaultPort = "127.0.0.1:50000"
const defaultPprofPort = "8080"

var log *logrus.Logger
var ctx = context.Background()

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func initLogging() {
	log = logrus.New()
	log.Level = logrus.WarnLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyFile:  "./logs/service.log",
		},
	}
}

func initPprof(pprofPort string) {
	addr := fmt.Sprintf("localhost:%v", pprofPort)
	go func() {
		log.Infoln("Starting pprof on %v", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("failed running pprof: %v", err)
		}
	}()
}

func main() {
	initLogging()
	if strings.ToLower(getEnv("DEBUG", "")) == "true" {
		log.Level = logrus.DebugLevel
	}

	if strings.ToLower(getEnv("PROFILING_DISABLE", "TRUE")) == "true" {
		pprofPort := getEnv("PPROF_PORT", defaultPprofPort)
		initPprof(pprofPort)
	}
	redisAddr := getEnv("REDIS_ADDR", "127.0.0.1:6379")
	redisUser := getEnv("REDIS_USER", "")
	redisPass := getEnv("REDIS_PASS", "")
	redisPoolSize, err := strconv.Atoi(getEnv("REDIS_POOL_SIZE", "10"))
	if err != nil {
		log.Errorf("redis pool size should be an integer: %v", err)
	}
	rs, err := store.NewRedisStore(ctx, redisAddr, redisUser, redisPass, redisPoolSize)

	port := getEnv("GRPC_PORT", defaultPort)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()
	svc := service.NewQueueService(rs)

	service.RegisterServiceServer(srv, svc)
	reflection.Register(srv)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
