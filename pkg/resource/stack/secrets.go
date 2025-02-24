// Copyright 2016-2019, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stack

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/pulumi/pulumi/pkg/secrets"
	"github.com/pulumi/pulumi/pkg/secrets/b64"
	"github.com/pulumi/pulumi/pkg/secrets/cloud"
	"github.com/pulumi/pulumi/pkg/secrets/passphrase"
	"github.com/pulumi/pulumi/pkg/secrets/service"
)

// DefaultSecretsProvider is the default SecretsProvider to use when deserializing deployments.
var DefaultSecretsProvider SecretsProvider = &defaultSecretsProvider{}

// SecretsProvider allows for the creation of secrets managers based on a well-known type name.
type SecretsProvider interface {
	// OfType returns a secrets manager for the given type, initialized with its previous state.
	OfType(ty string, state json.RawMessage) (secrets.Manager, error)
}

// defaultSecretsProvider implements the secrets.ManagerProviderFactory interface. Essentially
// it is the global location where new secrets managers can be registered for use when
// decrypting checkpoints.
type defaultSecretsProvider struct{}

// OfType returns a secrets manager for the given secrets type. Returns an error
// if the type is uknown or the state is invalid.
func (defaultSecretsProvider) OfType(ty string, state json.RawMessage) (secrets.Manager, error) {
	var sm secrets.Manager
	var err error
	switch ty {
	case b64.Type:
		sm = b64.NewBase64SecretsManager()
	case passphrase.Type:
		sm, err = passphrase.NewPassphaseSecretsManagerFromState(state)
	case service.Type:
		sm, err = service.NewServiceSecretsManagerFromState(state)
	case cloud.Type:
		sm, err = cloud.NewCloudSecretsManagerFromState(state)
	default:
		return nil, errors.Errorf("no known secrets provider for type %q", ty)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "constructing secrets manager of type %q", ty)
	}

	return sm, nil
}
