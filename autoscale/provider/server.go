package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	pb "provider/externalscaler/proto"

	"github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultPulsarLagThreshold = 10
	lagThresholdMetricName    = "lagThreshold"
)

type pulsarMetadata struct {
	adminURL     string
	topic        utils.TopicName // fqdn
	lagThreshold int64
	subscription string
}

func parsePulsarMetadata(data map[string]string) (meta *pulsarMetadata, err error) {
	meta = &pulsarMetadata{
		lagThreshold: defaultPulsarLagThreshold,
	}
	switch {
	case data["adminURL"] != "":
		meta.adminURL = data["adminURL"]
	default:
		return nil, fmt.Errorf("invalid admin url: %v", data)
	}

	tn, err := utils.GetTopicName(data["topic"])
	if err != nil {
		return nil, fmt.Errorf("invalid topic name: %v", data)
	}
	meta.topic = *tn

	switch {
	case data["subscription"] != "":
		meta.subscription = data["subscription"]
	default:
		return nil, fmt.Errorf("invalid subscription name: %v", data)
	}

	if val, ok := data[lagThresholdMetricName]; ok {
		t, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return meta, fmt.Errorf("error parsing %s: %s", lagThresholdMetricName, err)
		}
		meta.lagThreshold = t
	}
	return meta, nil
}

func getTopicSubLag(meta *pulsarMetadata) (int64, error) {
	cfg := &common.Config{
		WebServiceURL: meta.adminURL,
	}

	admin, err := pulsar.New(cfg)
	if err != nil {
		return 0, fmt.Errorf("failed to init pulsar client: %v", err)
	}

	stats, err := admin.Topics().GetStats(meta.topic)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch topic stats: %v", err)
	}
	zap.S().Debugw("fetch topic stats", "topic", meta.topic.String(), "stats", stats)
	subStats, ok := stats.Subscriptions[meta.subscription]
	if !ok {
		return 0, fmt.Errorf("subscription %v not found in stats response", meta.subscription)
	}
	return subStats.MsgBacklog, nil
}

type pulsarScaler struct {
}

func (p *pulsarScaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	zap.S().Infow("IsActive method call start", "req", ref)
	meta, err := parsePulsarMetadata(ref.GetScalerMetadata())
	if err != nil {
		return nil, err
	}
	lag, err := getTopicSubLag(meta)
	if err != nil {
		return nil, err
	}

	out := new(pb.IsActiveResponse)
	out.Result = lag > 0
	zap.S().Infow("IsActive method call end", "resp", out)
	return out, nil
}

func (p *pulsarScaler) StreamIsActive(ref *pb.ScaledObjectRef, server pb.ExternalScaler_StreamIsActiveServer) error {
	zap.S().Info("stream is active is called.")
	return nil
}

func (p *pulsarScaler) GetMetricSpec(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	zap.S().Infow("GetMetricSpec method call start", "req", ref)
	meta, err := parsePulsarMetadata(ref.GetScalerMetadata())
	if err != nil {
		return nil, err
	}

	m := new(pb.MetricSpec)
	m.MetricName = NormalizeString(fmt.Sprintf("pulsar-%s-%s-%s", meta.adminURL,
		meta.topic.String(), meta.subscription))
	m.TargetSize = meta.lagThreshold

	out := new(pb.GetMetricSpecResponse)
	out.MetricSpecs = make([]*pb.MetricSpec, 1)

	out.MetricSpecs[0] = m
	zap.S().Infow("GetMetricSpec method call end", "resp", out)
	return out, nil
}

func (p *pulsarScaler) GetMetrics(ctx context.Context, request *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	zap.S().Infow("GetMetrics method call start", "req", request)

	meta, err := parsePulsarMetadata(request.GetScaledObjectRef().GetScalerMetadata())
	if err != nil {
		return nil, err
	}
	lag, err := getTopicSubLag(meta)
	if err != nil {
		return nil, err
	}

	m := new(pb.MetricValue)
	m.MetricName = request.MetricName
	m.MetricValue = lag

	out := new(pb.GetMetricsResponse)

	out.MetricValues = make([]*pb.MetricValue, 1)
	out.MetricValues[0] = m
	zap.S().Infow("GetMetrics method call end", "resp", out)
	return out, nil
}

func newPulsarScaler() (*pulsarScaler, error) {
	return &pulsarScaler{}, nil
}

var (
	port int
)

func init() {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zl, err := cfg.Build()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	zap.ReplaceGlobals(zl)
}

func main() {
	flag.IntVar(&port, "port", 9898, "The server port")
	flag.Parse()

	scaler, err := newPulsarScaler()
	if err != nil {
		zap.S().Fatalf("failed to init pulsar scaler: %v", err)
	}

	zap.S().Infof("starting grpc service at port %d", port)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		zap.S().Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	reflection.Register(grpcServer)
	pb.RegisterExternalScalerServer(grpcServer, scaler)
	grpcServer.Serve(lis)
}

func NormalizeString(s string) string {
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, ".", "-")
	s = strings.ReplaceAll(s, ":", "-")
	s = strings.ReplaceAll(s, "%", "-")
	return s
}
