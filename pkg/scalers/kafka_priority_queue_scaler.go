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
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
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
	logger.Info(fmt.Sprintf("Parsed. baseMeta: %+v, pqMetas: %+v", baseMeta, pqMetas))

	client, admin, err := getKafkaClients(baseMeta)
	if err != nil {
		return nil, err
	}

	scaler := kafkaPQScaler{}
	for _, pqMeta := range pqMetas {
		pqScaler := kafkaScaler{
			client:          client,
			admin:           admin,
			metricType:      v2.ValueMetricType,
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
		pqConfig.TriggerMetadata["consumerGroupFromEnv"] = ""
		pqConfig.TriggerMetadata["consumerGroup"] = cpqMeta.group
		pqConfig.TriggerMetadata["topicFromEnv"] = ""
		pqConfig.TriggerMetadata["topic"] = cpqMeta.topic
		pqConfig.TriggerMetadata["lagThreshold"] = cpqMeta.maxAllowedLag
		pqConfig.TriggerMetadata["activationLagThreshold"] = cpqMeta.maxAllowedLag
		pqMeta, err := parseKafkaMetadata(&pqConfig, logger)
		if err != nil {
			return baseMeta, pqMetas, fmt.Errorf("error parsing kafka priority queue (%+v) metadata: %w", pqConfig, err)
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
			return pqMetaList, errors.New("priority queue config is invalid")
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
	priorityScalers := s.pqKafkaScalers
	var metricSpecs []v2.MetricSpec

	var baseMetricName string
	if baseScaler.metadata.topic != "" {
		baseMetricName = fmt.Sprintf("kafka-%s", baseScaler.metadata.topic)
	} else {
		baseMetricName = fmt.Sprintf("kafka-%s-topics", baseScaler.metadata.group)
	}

	baseExternalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(baseScaler.metadata.scalerIndex, kedautil.NormalizeString(baseMetricName)),
		},
		Target: GetMetricTarget(baseScaler.metricType, baseScaler.metadata.lagThreshold),
	}
	baseMetricSpec := v2.MetricSpec{External: baseExternalMetric, Type: kafkaMetricType}

	for _, ps := range priorityScalers {
		metricName := fmt.Sprintf("kafka-pq-%s", ps.metadata.topic)
		externalMetric := &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: GenerateMetricNameWithIndex(ps.metadata.scalerIndex, kedautil.NormalizeString(metricName)),
			},
			Target: GetMetricTarget(ps.metricType, ps.metadata.lagThreshold),
		}
		metricSpec := v2.MetricSpec{External: externalMetric, Type: kafkaMetricType}
		metricSpecs = append(metricSpecs, metricSpec)
	}

	metricSpecs = append(metricSpecs, baseMetricSpec)
	baseScaler.logger.Info(fmt.Sprintf("metric specs: %+v", metricSpecs))
	return metricSpecs
}

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *kafkaPQScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	priorityScalers := s.pqKafkaScalers
	baseScaler := s.baseKafkaScaler
	var externalMetricsValues []external_metrics.ExternalMetricValue

	baseMetricName := fmt.Sprintf("kafka-%s-topics", baseScaler.metadata.group)
	if baseScaler.metadata.topic != "" {
		baseMetricName = fmt.Sprintf("kafka-%s", baseScaler.metadata.topic)
	}
	baseMetricName = GenerateMetricNameWithIndex(baseScaler.metadata.scalerIndex, kedautil.NormalizeString(baseMetricName))
	if baseMetricName == metricName {
		baseScaler.logger.Info(fmt.Sprintf("requested metrics and activity for base scaler: %s", metricName))
		return s.GetBaseMetricsAndActivity(ctx, metricName)
	}

	var priorityScaler kafkaScaler
	isPriorityMetric := false
	for _, ps := range priorityScalers {
		pqMetricName := fmt.Sprintf("kafka-pq-%s", ps.metadata.topic)
		pqMetricName = GenerateMetricNameWithIndex(ps.metadata.scalerIndex, kedautil.NormalizeString(pqMetricName))

		if pqMetricName == metricName {
			isPriorityMetric = true
			priorityScaler = ps
			break
		}
	}

	if isPriorityMetric {
		baseScaler.logger.Info(fmt.Sprintf("requested metrics and activity for priority scaler: %s", metricName))
		return priorityScaler.GetPQMetricsAndActivity(ctx, metricName)
	}

	baseScaler.logger.Info(fmt.Sprintf("requested metrics and activity for unknown scaler: %s", metricName))
	return externalMetricsValues, false, nil
}

func (s *kafkaPQScaler) GetBaseMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	priorityScalers := s.pqKafkaScalers
	baseScaler := s.baseKafkaScaler
	var externalMetricsValues []external_metrics.ExternalMetricValue
	descaleRequired := false

	baseScaler.logger.Info(fmt.Sprintf("requested metrics and activity for base scaler: %s", metricName))

	for _, ps := range priorityScalers {
		pqMetricName := fmt.Sprintf("kafka-pq-%s", ps.metadata.topic)
		pqMetricName = GenerateMetricNameWithIndex(ps.metadata.scalerIndex, kedautil.NormalizeString(pqMetricName))

		//totalLag, totalLagWithPersistent, err := ps.getTotalLag()
		_, totalLagWithPersistent, err := ps.getTotalLag()
		if err != nil {
			baseScaler.logger.V(1).Error(err, "[IGNORE] error get lag from priority scaler topic [%s] for consumer [%s] : %w", ps.metadata.topic, ps.metadata.group, err)
			continue
		}

		pqCritical := totalLagWithPersistent > ps.metadata.activationLagThreshold
		baseScaler.logger.Info(fmt.Sprintf("priority scaler topic critical [%t] for topic [%s] is high. totalLagWithPersistent: %d, activationLag: %d", pqCritical, ps.metadata.topic, totalLagWithPersistent, ps.metadata.activationLagThreshold))
		if pqCritical {
			descaleRequired = true
			baseScaler.logger.Info(fmt.Sprintf("[DESCALING] lag [%d] in priority scaler topic [%s] for consumer [%s] is high", totalLagWithPersistent, ps.metadata.topic, ps.metadata.group))
		} else {
			baseScaler.logger.Info(fmt.Sprintf("[IGNORE] lag [%d] in priority scaler topic [%s] for consumer [%s] in check", totalLagWithPersistent, ps.metadata.topic, ps.metadata.group))
		}
	}

	if descaleRequired {
		metric := GenerateMetricInMili(metricName, float64(0))
		baseScaler.logger.Info(fmt.Sprintf("metric calculated on base scaler [%s]: %+v", metricName, externalMetricsValues))
		return []external_metrics.ExternalMetricValue{metric}, true, nil
	}

	// scale if lag is high in base queue and low in priority queues
	totalLag, totalLagWithPersistent, err := baseScaler.getTotalLag()
	if err != nil {
		baseScaler.logger.V(1).Error(err, "[IGNORE] error geting lag from base scaler topic [%s] for consumer [%s] : %v", baseScaler.metadata.topic, baseScaler.metadata.group, err)
		return []external_metrics.ExternalMetricValue{}, false, err
	}

	metric := GenerateMetricInMili(metricName, float64(totalLag))
	externalMetricsValues = append(externalMetricsValues, metric)
	scaleRequired := totalLagWithPersistent > baseScaler.metadata.activationLagThreshold
	baseScaler.logger.Info(fmt.Sprintf("[SCALING? %t] lag [%d] in base scaler topic [%s] for consumer [%s]", scaleRequired, totalLagWithPersistent, baseScaler.metadata.topic, baseScaler.metadata.group))
	baseScaler.logger.Info(fmt.Sprintf("External Metrics Values: %+v", externalMetricsValues))

	baseScaler.logger.Info(fmt.Sprintf("metric calculated on base scaler [%s]: %+v", metricName, externalMetricsValues))
	return externalMetricsValues, scaleRequired, nil
}

func (s *kafkaScaler) GetPQMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	var externalMetricsValues []external_metrics.ExternalMetricValue
	totalLag, totalLagWithPersistent, err := s.getTotalLag()

	if err != nil {
		s.logger.V(1).Error(err, "[IGNORE] error get lag from priority scaler topic [%s] for consumer [%s] : %w", s.metadata.topic, s.metadata.group, err)
		return externalMetricsValues, false, err
	}

	s.logger.Info(fmt.Sprintf("got lag on priority scaler scaler [%s] topic [%s] for consumer [%s] | totalLag:  %d, totalLagWithPersistent: %d", metricName, s.metadata.topic, s.metadata.group, totalLag, totalLagWithPersistent))

	metric := GenerateMetricInMili(metricName, float64(0))
	if totalLag > 0 {
		metric = GenerateMetricInMili(metricName, float64(1/totalLag))
	}

	externalMetricsValues = append(externalMetricsValues, metric)
	s.logger.Info(fmt.Sprintf("metric calculated on priority scaler [%s]: %+v", metricName, externalMetricsValues))

	return externalMetricsValues, false, nil
}

// Close closes the kafka admin and client
func (s *kafkaPQScaler) Close(ctx context.Context) error {
	baseScaler := s.baseKafkaScaler

	err := baseScaler.Close(ctx)
	if err != nil {
		baseScaler.logger.V(1).Error(err, "error while closing base scaler %v", err)
		return err
	}
	return nil
}
