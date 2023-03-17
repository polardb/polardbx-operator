/*
Copyright 2021 Alibaba Group Holding Limited.

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

package polardbxbackup

import (
	"context"
	"errors"
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
)

type Validator struct {
	client.Reader
	logr.Logger
	configLoader func() config.Config
	client       *filestream.FileClient
}

const magicString = "polardbx-filestream-validation"

func (v *Validator) getFilestreamClient() (*filestream.FileClient, error) {
	if v.client == nil {
		hostPort := strings.SplitN(v.configLoader().Store().FilestreamServiceEndpoint(), ":", 2)
		if len(hostPort) < 2 {
			return nil, errors.New("invalid filestream endpoint, please check config of controller: " +
				v.configLoader().Store().FilestreamServiceEndpoint())
		}
		port, err := strconv.Atoi(hostPort[1])
		if err != nil {
			return nil, errors.New("invalid filestream port, please check config of controller: " + hostPort[1])
		}
		v.client = filestream.NewFileClient(hostPort[0], port, nil)
	}
	return v.client, nil
}

func (v *Validator) ValidateCreate(ctx context.Context, obj runtime.Object) error {
	pxcBackup := obj.(*v1.PolarDBXBackup)

	// validate storage configure
	if pxcBackup.Spec.StorageProvider.StorageName == "" {
		return field.Required(field.NewPath("spec", "storageProvider", "storageName"),
			"storage name must be provided")
	}
	if pxcBackup.Spec.StorageProvider.Sink == "" {
		return field.Required(field.NewPath("spec", "storageProvider", "sink"),
			"sink must be provided")
	}
	filestreamAction, err := polardbx.NewBackupStorageFilestreamAction(pxcBackup.Spec.StorageProvider.StorageName)
	if err != nil {
		return field.Invalid(field.NewPath("spec", "storageProvider", "storageName"),
			pxcBackup.Spec.StorageProvider.StorageName, "unsupported storage")
	}

	// validate whether storage is available
	fsClient, err := v.getFilestreamClient()
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	actionMetadata := filestream.ActionMetadata{
		Action:    filestreamAction.Upload,
		Sink:      pxcBackup.Spec.StorageProvider.Sink,
		RequestId: uuid.New().String(),
		Filename:  magicString,
	}
	sentBytes, err := fsClient.Upload(strings.NewReader(magicString), actionMetadata)
	if err != nil || sentBytes == 0 {
		return field.Invalid(field.NewPath("spec", "storageProvider"), pxcBackup.Spec.StorageProvider,
			"invalid storage, please check configuration of both backup and hpfs")
	}

	return nil
}

func (v *Validator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) error {
	oldBackup, newBackup := oldObj.(*v1.PolarDBXBackup), newObj.(*v1.PolarDBXBackup)
	if oldBackup.Name != newBackup.Name {
		return field.Forbidden(field.NewPath("metadata", "name"), "immutable field")
	}
	if oldBackup.Spec.Cluster != newBackup.Spec.Cluster {
		return field.Forbidden(field.NewPath("spec", "cluster"), "immutable field")
	}
	if oldBackup.Spec.StorageProvider != newBackup.Spec.StorageProvider {
		return field.Forbidden(field.NewPath("spec", "storageProvider"), "immutable field")
	}
	return nil
}

func (v *Validator) ValidateDelete(ctx context.Context, obj runtime.Object) error {
	return nil
}

func NewPolarDBXBackupValidator(r client.Reader, logger logr.Logger, configLoader func() config.Config) extension.CustomValidator {
	return &Validator{
		Reader:       r,
		Logger:       logger,
		configLoader: configLoader,
	}
}
