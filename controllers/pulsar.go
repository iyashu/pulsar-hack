package controllers

import (
	"os"

	"github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
)

type PulsarClient struct {
	pulsar.Client
}

func NewPulsarClient() (*PulsarClient, error) {
	cfg := &common.Config{
		WebServiceURL: os.Getenv("PULSAR_ADMIN_URL"),
	}

	admin, err := pulsar.New(cfg)
	return &PulsarClient{
		Client: admin,
	}, err
}
