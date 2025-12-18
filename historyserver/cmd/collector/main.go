package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/logcollector/runtime"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func main() {
	var role string
	var storageProvider string
	var rayClusterName string
	var rayClusterId string
	var rayRootDir string
	var logBatching int
	var pushInterval time.Duration
	var storageProviderConfigPath string

	flag.StringVar(&role, "role", "Worker", "")
	flag.StringVar(&storageProvider, "storage-provider", "", "")
	flag.StringVar(&rayClusterName, "ray-cluster-name", "", "")
	flag.StringVar(&rayClusterId, "ray-cluster-id", "default", "")
	flag.StringVar(&rayRootDir, "ray-root-dir", "", "")
	flag.IntVar(&logBatching, "log-batching", 1000, "")
	flag.StringVar(&storageProviderConfigPath, "storage-provider-config-path", "", "") //"/var/collector-config/data"
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

	jsonData := make(map[string]interface{})
	if storageProviderConfigPath != "" {
		data, err := os.ReadFile(storageProviderConfigPath)
		if err != nil {
			logrus.Errorf("Failed to read storage provider config: %v", err)
			os.Exit(1)
		}
		err = json.Unmarshal(data, &jsonData)
		if err != nil {
			logrus.Errorf("Failed to parse storage provider config: %v", err)
			os.Exit(1)
		}
	}

	registry := collector.GetWriterRegistry()
	factory, ok := registry[storageProvider]
	if !ok {
		logrus.Errorf("Not supported storage provider: %s for role: %s", storageProvider, role)
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
		logrus.Errorf("Failed to create writer for storage provider: %s for role: %s: %v", storageProvider, role, err)
		os.Exit(1)
	}

	// Create and initialize LogCollector
	logCollector := runtime.NewCollector(&globalConfig, writer)
	_ = logCollector.Start(context.TODO().Done())

	logStop := logCollector.WaitForStop()
	<-logStop
	logrus.Info("Log server shutdown")
}
