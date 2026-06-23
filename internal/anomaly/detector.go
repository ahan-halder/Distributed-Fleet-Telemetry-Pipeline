package anomaly

import (
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
)

// Detector defines an interface for anomaly detection.
type Detector interface {
	IsAnomaly(frame *telemetryv1.MetricFrame) bool
}

// SimpleThresholdDetector implements a basic threshold-based anomaly detector.
type SimpleThresholdDetector struct {
	CPUThresholdPct float64
	MemThresholdPct float64
}

// NewSimpleThresholdDetector creates a new SimpleThresholdDetector.
func NewSimpleThresholdDetector(cpu, mem float64) *SimpleThresholdDetector {
	return &SimpleThresholdDetector{
		CPUThresholdPct: cpu,
		MemThresholdPct: mem,
	}
}

// IsAnomaly returns true if any metric exceeds the threshold.
func (d *SimpleThresholdDetector) IsAnomaly(frame *telemetryv1.MetricFrame) bool {
	if frame.System == nil {
		return false
	}

	if frame.System.CpuUtilizationPct > d.CPUThresholdPct {
		return true
	}

	if frame.System.MemoryTotalBytes > 0 {
		memPct := float64(frame.System.MemoryUsedBytes) / float64(frame.System.MemoryTotalBytes) * 100
		if memPct > d.MemThresholdPct {
			return true
		}
	}

	return false
}
