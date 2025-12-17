package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/eventserver"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/logcollector/runtime"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func main() {
	var role string
	var runtimeClassName string
	var rayClusterName string
	var rayClusterId string
	var rayRootDir string
	var logBatching int
	var eventsPort int
	var pushInterval time.Duration
	var runtimeClassConfigPath string

	flag.StringVar(&role, "role", "Worker", "")
	flag.StringVar(&runtimeClassName, "runtime-class-name", "", "")
	flag.StringVar(&rayClusterName, "ray-cluster-name", "", "")
	flag.StringVar(&rayClusterId, "ray-cluster-id", "default", "")
	flag.StringVar(&rayRootDir, "ray-root-dir", "", "")
	flag.IntVar(&logBatching, "log-batching", 1000, "")
	flag.IntVar(&eventsPort, "events-port", 8080, "")
	flag.StringVar(&runtimeClassConfigPath, "runtime-class-config-path", "", "") //"/var/collector-config/data"
	flag.DurationVar(&pushInterval, "push-interval", time.Minute, "")

	flag.Parse()

	sessionDir, err := utils.GetSessionDir()
	if err != nil {
		logrus.Errorf("Failed to get session dir: %v", err)
		os.Exit(1)
	}

	rayNodeId, err := utils.GetRayNodeID()
	if err != nil {
		logrus.Errorf("Failed to get ray node id: %v", err)
		os.Exit(1)
	}

	sessionName := path.Base(sessionDir)

	jsonData := make(map[string]interface{})
	if runtimeClassConfigPath != "" {
		data, err := os.ReadFile(runtimeClassConfigPath)
		if err != nil {
			logrus.Errorf("Failed to read runtime class config: %v", err)
			os.Exit(1)
		}
		err = json.Unmarshal(data, &jsonData)
		if err != nil {
			logrus.Errorf("Failed to parse runtime class config: %v", err)
			os.Exit(1)
		}
	}

	registry := collector.GetWriterRegistry()
	factory, ok := registry[runtimeClassName]
	if !ok {
		logrus.Errorf("Not supported runtime class name: %s for role: %s", runtimeClassName, role)
		os.Exit(1)
	}

	globalConfig := types.RayCollectorConfig{
		RootDir:        rayRootDir,
		SessionDir:     sessionDir,
		RayNodeName:    rayNodeId,
		Role:           role,
		RayClusterName: rayClusterName,
		RayClusterID:   rayClusterId,
		PushInterval:   pushInterval,
		LogBatching:    logBatching,
	}
	logrus.Info("Using collector config: ", globalConfig)

	writer, err := factory(&globalConfig, jsonData)
	if err != nil {
		logrus.Errorf("Failed to create writer for runtime class name: %s for role: %s: %v", runtimeClassName, role, err)
		os.Exit(1)
	}

	// Create and initialize EventServer
	eventServer := eventserver.NewEventServer(writer, rayRootDir, sessionDir, rayNodeId, rayClusterName, rayClusterId, sessionName)
	eventServer.InitServer(eventsPort)

	logCollector := runtime.NewCollector(&globalConfig, writer)
	_ = logCollector.Start(context.TODO().Done())

	eventStop := eventServer.WaitForStop()
	logStop := logCollector.WaitForStop()
	<-eventStop
	logrus.Info("Event server shutdown")
	<-logStop
	logrus.Info("Log server shutdown")
	logrus.Info("All servers shutdown")
}
