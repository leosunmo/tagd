package tagd

import (
	"fmt"
	"reflect"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

type PVWatcher struct {
	k8sClient  *kubernetes.Clientset
	pvInformer cache.SharedIndexInformer
	log        *zap.Logger
}

func NewPVWatcher(k8sClient *kubernetes.Clientset, logger *zap.Logger) *PVWatcher {
	pvw := &PVWatcher{
		k8sClient: k8sClient,
		log:       logger,
	}
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(k8sClient, time.Second*30)
	pvInformer := kubeInformerFactory.Core().V1().PersistentVolumes().Informer()

	pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pvw.handleCreate(obj)
		},
		DeleteFunc: func(obj interface{}) {
			pvw.handleDelete(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pvw.handleUpdate(oldObj, newObj)
		},
	})

	pvw.pvInformer = pvInformer

	return pvw
}

// Run starts the PV Informer and blocks until stopCh is closed.
func (pvw *PVWatcher) Run(stopCh <-chan struct{}) {
	pvw.pvInformer.Run(stopCh)
}

// handleCreate processes PVs that have just been created.
func (pvw *PVWatcher) handleCreate(obj interface{}) {
	pvw.log.Debug("PV added")

	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		pvw.log.Error(fmt.Sprintf("pv created, failed to assert persistent volume of type: %T", obj))
		return
	}
	pvw.log.Debug(fmt.Sprintf("PV %s created, labels: %s", pv.ObjectMeta.Name, pv.ObjectMeta.Labels))
}

// handleUpdate processes PVs that have been updated and the labels have changed
func (pvw *PVWatcher) handleUpdate(oldObj, newObj interface{}) {
	pvw.log.Debug(fmt.Sprintf("PV updated:\n\told: %s\n\tnew: %s", oldObj, newObj))

	oldPv, ok := oldObj.(*v1.PersistentVolume)
	if !ok {
		pvw.log.Error(fmt.Sprintf("pv updated, failed to assert persistent volume of type: %T", oldObj))
		return
	}

	newPv, ok := newObj.(*v1.PersistentVolume)
	if !ok {
		pvw.log.Error(fmt.Sprintf("pv updated, failed to assert persistent volume of type: %T", newObj))
		return
	}

	oldLabels := oldPv.ObjectMeta.GetLabels()
	newLabels := newPv.ObjectMeta.GetLabels()

	if reflect.DeepEqual(oldLabels, newLabels) {
		pvw.log.Debug(fmt.Sprintf("PV %s updated, but labels didn't change", oldPv.ObjectMeta.Name))
		return
	}

	pvw.log.Debug(fmt.Sprintf("PV %s labels changed, old: %s new: %s", oldPv.ObjectMeta.Name, oldPv.ObjectMeta.Labels, newPv.ObjectMeta.Labels))
}

// handleDelete processes PVs that have been deleted, tagging the disk with a deleted tag, if it still exists in AWS.
// This would allow you to detect if a disk wasn't properly removed whose corresponding PV is gone.
func (pvw *PVWatcher) handleDelete(obj interface{}) {
	pvw.log.Debug(fmt.Sprintf("PV deleted: %s", obj))

	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		pvw.log.Error(fmt.Sprintf("pv deleted, failed to assert persistent volume of type: %T", obj))
		return
	}

	if AWSspec := pv.Spec.AWSElasticBlockStore; AWSspec == nil {
		pvw.log.Error(fmt.Sprintf("PV %s does not have AWSBlockStore spec", pv.ObjectMeta.Name))
		return
	}
	volID := pv.Spec.AWSElasticBlockStore.VolumeID
	if volID == "" {
		pvw.log.Error(fmt.Sprintf("PV %s AWS volume Id is empty", pv.ObjectMeta.Name))
		return
	}

	pvw.Handle(pv.ObjectMeta.Labels, volID)

}

func (pvw *PVWatcher) Handle(pvLabels map[string]string, volID string) {
	return
}
