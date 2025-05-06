package manager

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tsuru/rate-limit-control-plane/internal/aggregator"
	"github.com/tsuru/rate-limit-control-plane/internal/config"
	"github.com/tsuru/rate-limit-control-plane/internal/ratelimit"
)

const administrativePort = 8800

// Update RpaasInstanceSyncWorker to use GoroutineManager for managing pod workers
type RpaasInstanceSyncWorker struct {
	sync.Mutex
	RpaasInstanceName    string
	Zones                []string
	RpaasInstanceSignals RpaasInstanceSignals
	PodWorkerManager     *GoroutineManager
	Ticker               *time.Ticker
	zoneDataChan         chan Optional[ratelimit.Zone]
	notify               chan ratelimit.RpaasZoneData
	fullZones            map[string]map[ratelimit.FullZoneKey]*ratelimit.RateLimitEntry
}

type RpaasInstanceSignals struct {
	StartRpaasPodWorker chan [2]string
	StopRpaasPodWorker  chan string
	StopChan            chan struct{}
}

func NewRpaasInstanceSyncWorker(rpaasInstanceName string, zones []string, notify chan ratelimit.RpaasZoneData) *RpaasInstanceSyncWorker {
	signals := RpaasInstanceSignals{
		StartRpaasPodWorker: make(chan [2]string),
		StopRpaasPodWorker:  make(chan string),
		StopChan:            make(chan struct{}),
	}
	ticker := time.NewTicker(config.Spec.ControllerMinutesInternval)

	return &RpaasInstanceSyncWorker{
		RpaasInstanceName:    rpaasInstanceName,
		Zones:                zones,
		RpaasInstanceSignals: signals,
		PodWorkerManager:     NewGoroutineManager(),
		Ticker:               ticker,
		zoneDataChan:         make(chan Optional[ratelimit.Zone]),
		notify:               notify,
		fullZones:            make(map[string]map[ratelimit.FullZoneKey]*ratelimit.RateLimitEntry),
	}
}

func (w *RpaasInstanceSyncWorker) Work() {
	for {
		select {
		case <-w.Ticker.C:
			w.processTick()
		case <-w.RpaasInstanceSignals.StopChan:
			w.cleanup()
			return
		}
	}
}

func (w *RpaasInstanceSyncWorker) processTick() {
	rpaasZoneData := ratelimit.RpaasZoneData{
		RpaasName: w.RpaasInstanceName,
		Data:      []ratelimit.Zone{},
	}
	for _, zone := range w.Zones {
		w.Lock()
		podWorkers := w.PodWorkerManager.ListWorkerIDs()
		workerCount := len(podWorkers)

		if workerCount == 0 {
			w.Unlock()
			continue
		}

		// Process each zone for all pod workers
		w.PodWorkerManager.ForEachWorker(func(worker Worker) {
			if podWorker, ok := worker.(*RpaasPodWorker); ok {
				podWorker.ReadZoneChan <- zone
			}
		})

		// Collect zone data from all pod workers
		zoneData := []ratelimit.Zone{}
		for i := 0; i < workerCount; i++ {
			result := <-w.zoneDataChan
			if result.Error != nil {
				log.Printf("Error getting zone data: %v", result.Error)
				continue
			}
			zoneData = append(zoneData, result.Value)
		}

		if len(zoneData) == 0 {
			w.Unlock()
			continue
		}

		// Aggregate zone data
		aggregatedZone, newFullZone := aggregator.AggregateZones(zoneData, w.fullZones[zone])
		w.fullZones[zone] = newFullZone
		w.Unlock()

		rpaasZoneData.Data = append(rpaasZoneData.Data, aggregatedZone)

		// Write aggregated data back to pod workers
		w.PodWorkerManager.ForEachWorker(func(worker Worker) {
			if podWorker, ok := worker.(*RpaasPodWorker); ok {
				podWorker.WriteZoneChan <- aggregatedZone
			}
		})
	}
	w.notify <- rpaasZoneData
}

func (w *RpaasInstanceSyncWorker) cleanup() {
	// Stop all pod workers
	w.PodWorkerManager.ForEachWorker(func(worker Worker) {
		worker.Stop()
	})

	// Close all channels
	close(w.RpaasInstanceSignals.StopChan)
	close(w.zoneDataChan)
	w.Ticker.Stop()
}

func (w *RpaasInstanceSyncWorker) Start() {
	go w.Work()
}

func (w *RpaasInstanceSyncWorker) Stop() {
	if w.RpaasInstanceSignals.StopChan != nil {
		w.RpaasInstanceSignals.StopChan <- struct{}{}
	}
}

func (w *RpaasInstanceSyncWorker) GetID() string {
	return w.RpaasInstanceName
}

func (w *RpaasInstanceSyncWorker) AddPodWorker(podIP, podName string) {
	podURL := fmt.Sprintf("http://%s:%d", podIP, administrativePort)
	podWorker := NewRpaasPodWorker(podURL, podName, w.zoneDataChan)
	w.PodWorkerManager.AddWorker(podWorker)
}

func (w *RpaasInstanceSyncWorker) RemovePodWorker(podName string) error {
	if ok := w.PodWorkerManager.RemoveWorker(podName); !ok {
		return fmt.Errorf("pod worker not found: %s", podName)
	}
	return nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
