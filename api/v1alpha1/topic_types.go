/*
Copyright 2021.

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

package v1alpha1

import (
	"fmt"

	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	TopicStateUnknown   string = "Unknown"
	TopicStateSynced    string = "Synced"
	TopicStateOutOfSync string = "OutOfSync"
)

// TopicSpec defines the desired state of Topic
type TopicSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Tenant string `json:"tenant,omitempty"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace,omitempty"`

	// +kubebuilder:default=persistent
	Domain string `json:"domain,omitempty"`

	// +kubebuilder:default=0
	Partitions int `json:"partitions"`
}

// TopicStatus defines the observed state of Topic
type TopicStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// +kubebuilder:validation:Enum=Unknown;Synced;OutOfSync
	State string `json:"state,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenant`,description="Pulsar cluster tenant"
//+kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.spec.namespace`,description="Pulsar cluster namespace"
//+kubebuilder:printcolumn:name="Domain",type=string,JSONPath=`.spec.domain`,description="Pulsar cluster domain"
//+kubebuilder:printcolumn:name="Partitions",type=string,JSONPath=`.spec.partitions`,description="Pulsar cluster partitions"
//+kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.state`,description="Topic sync status"

// Topic is the Schema for the topics API
type Topic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopicSpec   `json:"spec,omitempty"`
	Status TopicStatus `json:"status,omitempty"`
}

func (t *Topic) GetFQTopicName() utils.TopicName {
	var domain string
	if t.Spec.Domain == "" {
		domain = "persistent://"
	} else {
		domain = t.Spec.Domain + "://"
	}

	tn, _ := utils.GetTopicName(fmt.Sprintf("%s%s/%s/%s", domain, t.Spec.Tenant, t.Spec.Namespace, t.GetName()))
	return *tn
}

//+kubebuilder:object:root=true

// TopicList contains a list of Topic
type TopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Topic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topic{}, &TopicList{})
}
