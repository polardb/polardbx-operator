/*
Copyright 2022 Alibaba Group Holding Limited.

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

package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type CustomMutator interface {
	MutateOnCreate(ctx context.Context, obj runtime.Object) error
	MutateOnUpdate(ctx context.Context, oldObj, newObj runtime.Object) error
}

func WithCustomMutator(obj runtime.Object, mutator CustomMutator) *admission.Webhook {
	return &admission.Webhook{
		Handler: &mutatorForType{object: obj, mutator: mutator},
	}
}

type mutatorForType struct {
	mutator CustomMutator
	object  runtime.Object
	decoder *admission.Decoder
}

func (h *mutatorForType) InjectDecoder(decoder *admission.Decoder) error {
	h.decoder = decoder
	return nil
}

var _ admission.DecoderInjector = &mutatorForType{}

func (h *mutatorForType) Handle(ctx context.Context, req admission.Request) admission.Response {
	if h.mutator == nil {
		panic("mutator should never be nil")
	}
	if h.object == nil {
		panic("object should never be nil")
	}

	// Get the object in the request
	obj := h.object.DeepCopyObject()

	var err error
	switch req.Operation {
	case admissionv1.Create:
		if err := h.decoder.Decode(req, obj); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		err = h.mutator.MutateOnCreate(ctx, obj)
	case admissionv1.Update:
		oldObj := obj.DeepCopyObject()
		if err := h.decoder.DecodeRaw(req.Object, obj); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		if err := h.decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		err = h.mutator.MutateOnUpdate(ctx, oldObj, obj)
	case admissionv1.Delete:
		return admission.Allowed("")
	default:
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("unknown operation request %s", req.Operation))
	}

	// Check the error message first.
	if err != nil {
		var apiStatus apierrors.APIStatus
		if errors.As(err, &apiStatus) {
			return validationResponseFromStatus(false, apiStatus.Status())
		}
		return admission.Denied(err.Error())
	}

	// Create the patch.
	marshalled, err := json.Marshal(obj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshalled)
}
