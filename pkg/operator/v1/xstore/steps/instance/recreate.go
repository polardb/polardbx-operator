package instance

import (
	"context"
	"errors"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var DeleteIpInvalidPod = xstorev1reconcile.NewStepBinder("DeleteIpInvalidPod",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore pods")
		}
		invalidCount := 0
		for _, pod := range pods {
			if k8shelper.IsPodScheduled(&pod) && pod.Status.PodIP != "" && len(pod.Annotations) > 0 {
				if flushIp, ok := pod.Annotations[xstoremeta.AnnotationFlushIp]; ok {
					if pod.Status.PodIP != flushIp {
						invalidCount += 1
					}
				}
			}
		}
		xstore := rc.MustGetXStore()
		candPodCount := len(pods) - invalidCount
		if candPodCount <= len(xstore.Status.BoundVolumes)/2 {
			for _, pod := range pods {
				err := rc.Client().Delete(rc.Context(), &pod)
				if err != nil {
					flow.Logger().Error(err, "failed to delete pod", "pod", pod.Name)
				}
			}
		}
		return flow.Continue("DeleteIpInvalidPod continue")
	})

var CreateResetMetaIndicate = xstorev1reconcile.NewStepBinder("CreateResetMetaIndicate",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore, err := rc.GetXStore()
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore")
		}
		hpfsClient, err := rc.GetHpfsClient()
		if err != nil {
			return flow.RetryErr(err, "failed to get hpfs client")
		}
		for _, v := range xstore.Status.BoundVolumes {
			err := hpfsWriteResetMetaIndicate(rc.Context(), hpfsClient, v)
			if err != nil {
				return flow.RetryErr(err, "failed to write reset meta indicate")
			}
		}
		return flow.Continue("CreateResetMetaIndicate continue")
	})

func hpfsWriteResetMetaIndicate(ctx context.Context, hpfsClient hpfs.HpfsServiceClient, vol *polardbxv1xstore.HostPathVolume) error {
	createFileRequest := &hpfs.CreateFileRequest{
		Host:    &hpfs.Host{NodeName: vol.Host},
		Options: &hpfs.CreateFileOptions{OverwriteIfExists: true},
		File:    &hpfs.FileRequest{Path: filepath.Join(vol.HostPath, "handle_indicate"), Mode: 0x0666},
		Content: []byte(xstoremeta.HandleIndicateResetClusterInfo),
	}
	response, err := hpfsClient.CreateFile(ctx, createFileRequest)
	if err != nil {
		return err
	}
	if response.Status.Code != hpfs.Status_OK {
		return errors.New("status not ok: " + response.Status.Code.String())
	}
	return nil
}

func WhenNoPod(binders ...control.BindFunc) control.BindFunc {
	return xstorev1reconcile.NewStepIfBinder("WhenNoRightPod",
		func(rc *xstorev1reconcile.Context, log logr.Logger) (bool, error) {
			xstore := rc.MustGetXStore()
			if xstore.Spec.Readonly {
				return false, nil
			}
			pods, err := rc.GetXStorePods()
			if err != nil {
				return false, err
			}
			if len(pods) > 0 {
				return false, nil
			}
			return true, nil
		},
		binders...,
	)
}
