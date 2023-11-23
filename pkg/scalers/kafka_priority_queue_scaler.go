/*
Copyright 2023 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This scaler is based on sarama library.
// It lacks support for AWS MSK. For AWS MSK please see: apache-kafka scaler.

package scalers

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"strconv"
	"strings"
)

type kafkaPQScaler struct {
	pqKafkaScalers  []kafkaScaler
	baseKafkaScaler kafkaScaler
}

type customPriorityQueueMetadata struct {
	topic         string
	group         string
	maxAllowedLag string
}

// NewKafkaPQScaler creates a new kafkaPQScaler
func NewKafkaPQScaler(config *ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "kafka_pq_scaler")

	baseMeta, pqMetas, err := parseKafkaPQMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing priority queue kafka metadata: %w", err)
	}

	client, admin, err := getKafkaClients(baseMeta)
	if err != nil {
		return nil, err
	}

	scaler := kafkaPQScaler{}
	for _, pqMeta := range pqMetas {
		pqScaler := kafkaScaler{
			client:          client,
			admin:           admin,
			metricType:      "",
			metadata:        pqMeta,
			logger:          logger,
			previousOffsets: make(map[string]map[int32]int64),
		}
		scaler.pqKafkaScalers = append(scaler.pqKafkaScalers, pqScaler)
	}

	scaler.baseKafkaScaler = kafkaScaler{
		client:          client,
		admin:           admin,
		metricType:      metricType,
		metadata:        baseMeta,
		logger:          logger,
		previousOffsets: make(map[string]map[int32]int64),
	}

	return &scaler, nil
}

func parseKafkaPQMetadata(config *ScalerConfig, logger logr.Logger) (kafkaMetadata, []kafkaMetadata, error) {
	baseMeta := kafkaMetadata{}
	var pqMetas []kafkaMetadata

	originalConfig := *config
	baseMeta, err := parseKafkaMetadata(&originalConfig, logger)
	if err != nil {
		return baseMeta, pqMetas, fmt.Errorf("error parsing kafka base queue metadata: %w", err)
	}

	priorityQueuesStr := config.TriggerMetadata["priorityQueues"]
	customPQMetas, err := getCustomPQMetadata(priorityQueuesStr)
	if err != nil {
		return baseMeta, pqMetas, fmt.Errorf("error while parsing priority queue config (%s): %w", priorityQueuesStr, err)
	}

	for _, cpqMeta := range customPQMetas {
		pqConfig := *config
		pqConfig.TriggerMetadata["consumerGroupFromEnv"] = cpqMeta.group
		pqConfig.TriggerMetadata["consumerGroup"] = cpqMeta.group
		pqConfig.TriggerMetadata["topicFromEnv"] = cpqMeta.topic
		pqConfig.TriggerMetadata["topic"] = cpqMeta.topic
		pqConfig.TriggerMetadata["lagThreshold"] = cpqMeta.maxAllowedLag
		pqMeta, err := parseKafkaMetadata(config, logger)
		if err != nil {
			return baseMeta, pqMetas, fmt.Errorf("error parsing kafka priority queue (%v) metadata: %w", pqMeta, err)
		}

		pqMetas = append(pqMetas, pqMeta)
	}

	return baseMeta, pqMetas, nil
}

func getCustomPQMetadata(priorityQueuesStr string) ([]customPriorityQueueMetadata, error) {
	var pqMetaList []customPriorityQueueMetadata

	if priorityQueuesStr == "" {
		return pqMetaList, errors.New("no priority queues config provided")
	}

	priorityQueuesSplits := strings.Split(priorityQueuesStr, ",")
	for _, pqStr := range priorityQueuesSplits {
		pqSplits := strings.Split(pqStr, ":")
		if len(pqSplits) != 3 {
			return pqMetaList, errors.New("priority queue config should is invalid")
		}

		pqTopic := strings.TrimSpace(pqSplits[0])
		if pqTopic == "" {
			return pqMetaList, errors.New("no priority queue topic provided")
		}

		pqGroup := strings.TrimSpace(pqSplits[1])
		if pqGroup == "" {
			return pqMetaList, errors.New("no priority queue consumer group provided")
		}

		maxAllowedLagStr := strings.TrimSpace(pqSplits[2])
		maxAllowedLagInt, err := strconv.ParseInt(maxAllowedLagStr, 10, 64)
		if err != nil || maxAllowedLagInt < 0 {
			return pqMetaList, errors.New("max allowed lag on priority queue cant be empty or less than 0")
		}

		pqMeta := customPriorityQueueMetadata{
			topic:         pqTopic,
			group:         pqGroup,
			maxAllowedLag: maxAllowedLagStr,
		}

		pqMetaList = append(pqMetaList, pqMeta)
	}

	return pqMetaList, nil
}

func (s *kafkaPQScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	baseScaler := s.baseKafkaScaler
	var metricName string
	if baseScaler.metadata.topic != "" {
		metricName = fmt.Sprintf("kafka-%s", baseScaler.metadata.topic)
	} else {
		metricName = fmt.Sprintf("kafka-%s-topics", baseScaler.metadata.group)
	}

	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(baseScaler.metadata.scalerIndex, kedautil.NormalizeString(metricName)),
		},
		Target: GetMetricTarget(baseScaler.metricType, baseScaler.metadata.lagThreshold),
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: kafkaMetricType}
	baseScaler.logger.V(1).Info(fmt.Sprintf("metric spec: %v", metricSpec))
	return []v2.MetricSpec{metricSpec}
}

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *kafkaPQScaler) GetMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	priorityScalers := s.pqKafkaScalers
	baseScaler := s.baseKafkaScaler
	var externalMetricsValues []external_metrics.ExternalMetricValue
	descaleRequired := false

	for _, ps := range priorityScalers {
		pqMetricName := metricName + "-pq-" + ps.metadata.topic + ps.metadata.group
		totalLag, totalLagWithPersistent, err := ps.getTotalLag()
		if err != nil {
			baseScaler.logger.V(1).Error(err, "[IGNORE] error get lag from priority scaler topic [%s] for consumer [%s] : %w", ps.metadata.topic, ps.metadata.group, err)
			metric := GenerateMetricInMili(pqMetricName, float64(0))
			externalMetricsValues = append(externalMetricsValues, metric)
			continue
		}

		metric := GenerateMetricInMili(pqMetricName, float64(totalLag))
		pqCritical := totalLagWithPersistent > ps.metadata.activationLagThreshold

		if pqCritical {
			descaleRequired = true
			baseScaler.logger.V(1).Info(fmt.Sprintf("[DESCALING] lag [%d] in priority scaler topic [%s] for consumer [%s] is high", totalLagWithPersistent, ps.metadata.topic, ps.metadata.group))
			externalMetricsValues = append(externalMetricsValues, metric)
		}
		baseScaler.logger.V(1).Info(fmt.Sprintf("[IGNORE] lag [%d] in priority scaler topic [%s] for consumer [%s] in check", totalLagWithPersistent, ps.metadata.topic, ps.metadata.group))
	}

	// scale if lag is high in base queue and low in priority queues
	totalLag, totalLagWithPersistent, err := baseScaler.getTotalLag()
	if err != nil {
		baseScaler.logger.V(1).Error(err, fmt.Sprintf("[IGNORE] error get lag from base scaler topic [%s] for consumer [%s] : %w", baseScaler.metadata.topic, baseScaler.metadata.group, err))
		return externalMetricsValues, false, err
	}

	if descaleRequired {
		totalLag = 0
		totalLagWithPersistent = 0
	}

	metric := GenerateMetricInMili(metricName, float64(totalLag))
	externalMetricsValues = append(externalMetricsValues, metric)
	scaleRequired := totalLagWithPersistent < baseScaler.metadata.activationLagThreshold
	baseScaler.logger.V(1).Info(fmt.Sprintf("[SCALING? %t] lag [%d] in base scaler topic [%s] for consumer [%s]", scaleRequired, totalLagWithPersistent, baseScaler.metadata.topic, baseScaler.metadata.group))
	baseScaler.logger.V(1).Info(fmt.Sprintf("external metrics values: %v", externalMetricsValues))

	return externalMetricsValues, scaleRequired, nil
}

// Close closes the kafka admin and client
func (s *kafkaPQScaler) Close(ctx context.Context) error {
	priorityScalers := s.pqKafkaScalers
	baseScaler := s.baseKafkaScaler

	for _, pqS := range priorityScalers {
		err := pqS.Close(ctx)
		if err != nil {
			baseScaler.logger.V(1).Error(err, fmt.Sprintf("error while closing priority scaler %w", err))
			return err
		}
	}

	err := baseScaler.Close(ctx)
	if err != nil {
		baseScaler.logger.V(1).Error(err, fmt.Sprintf("error while closing priority scaler %w", err))
		return err
	}
	return nil
}
