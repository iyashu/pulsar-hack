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

package controllers

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pulsarv1alpha1 "github.com/iyashu/pulsar-hack/api/v1alpha1"
)

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme

	Pulsar *PulsarClient
}

const TopicFinalizer = "pulsar.apache.org/topic-finalizer"

//+kubebuilder:rbac:groups=pulsar.apache.org,resources=topics,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=pulsar.apache.org,resources=topics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=pulsar.apache.org,resources=topics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Topic object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.8.3/pkg/reconcile
func (r *TopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("topic", req.NamespacedName)
	log.Info("reconciling pulsar topic")

	topic := &pulsarv1alpha1.Topic{}
	err := r.Get(ctx, req.NamespacedName, topic)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			log.Info("topic resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "failed to get topic resource")
		return ctrl.Result{}, err
	}

	// Check if the Topic instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	markedAsDeleted := topic.GetDeletionTimestamp() != nil
	if markedAsDeleted {
		if controllerutil.ContainsFinalizer(topic, TopicFinalizer) {
			// Run finalization logic for topicFinalizer. If the
			// finalization logic fails, don't remove the finalizer so
			// that we can retry during the next reconciliation.
			if err := r.finalizeTopic(log, topic); err != nil {
				return ctrl.Result{}, err
			}

			// Remove topicFinalizer. Once all finalizers have been
			// removed, the object will be deleted.
			controllerutil.RemoveFinalizer(topic, TopicFinalizer)
			err := r.Update(ctx, topic)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer for this CR
	if !controllerutil.ContainsFinalizer(topic, TopicFinalizer) {
		controllerutil.AddFinalizer(topic, TopicFinalizer)
		err = r.Update(ctx, topic)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check if the topic already exists, if not create a new one.
	found, err := r.lookupTopic(log, topic)
	if err != nil {
		log.Error(err, "failed to get pulsar topic")
		return ctrl.Result{}, err
	} else if !found {
		if topic.Status.State != pulsarv1alpha1.TopicStateOutOfSync {
			topic.Status.State = pulsarv1alpha1.TopicStateOutOfSync
			if updateErr := r.Status().Update(ctx, topic); updateErr != nil {
				log.Error(updateErr, "failed to update k8s topic resource")
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{Requeue: true}, nil
		}

		// create a new topic
		tn := topic.GetFQTopicName()
		log.Info("creating pulsar topic", "name", tn)
		if createErr := r.Pulsar.Topics().Create(tn, 0); createErr != nil {
			log.Error(createErr, "failed to create pulsar topic")
			return ctrl.Result{}, createErr
		}
		// topic created successfully - return and requeue
		return ctrl.Result{Requeue: true}, nil
	}

	if topic.Status.State != pulsarv1alpha1.TopicStateSynced {
		topic.Status.State = pulsarv1alpha1.TopicStateSynced
		if updateErr := r.Status().Update(ctx, topic); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pulsarv1alpha1.Topic{}).
		Complete(r)
}

func (r *TopicReconciler) finalizeTopic(log logr.Logger, topic *pulsarv1alpha1.Topic) error {
	log.Info("deleting topic from pulsar cluster", "fqdn", topic.GetFQTopicName())
	return r.Pulsar.Topics().Delete(topic.GetFQTopicName(), false, true)
}

func (r *TopicReconciler) lookupTopic(log logr.Logger, topic *pulsarv1alpha1.Topic) (bool, error) {
	ns, err := utils.GetNameSpaceName(topic.Spec.Tenant, topic.Spec.Namespace)
	if err != nil {
		return false, err
	}

	p, np, err := r.Pulsar.Topics().List(*ns)
	if err != nil {
		return false, err
	}
	log.Info("topics listed", "partitioned", p, "non-partitioned", np)
	expectedTopicFQName := topic.GetFQTopicName()
	for _, v := range np {
		tn, err := utils.GetTopicName(v)
		if err != nil {
			log.Error(err, "failed to parse topic fqdn", "name", v)
			return false, err
		}
		if expectedTopicFQName.String() == tn.String() {
			log.Info("topic already exists", "fqdn", tn.String())
			return true, nil
		}
	}
	return false, nil
}
