//go:build unit
// +build unit

/*
Copyright 2024 Google LLC

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

package adapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetOrRefreshSession(t *testing.T) {
	t.Cleanup(ResetGrpcFuncs())
	MockCreateSessionGrpc("refreshed-session")
	ctx := context.Background()

	tests := []struct {
		name            string
		initialSession  session
		refreshInterval time.Duration
		wantSession     session
	}{
		{
			name: "Session exists and is valid",
			initialSession: session{
				name:       "valid-session",
				createTime: time.Now(),
			},
			refreshInterval: 10 * time.Hour,
			wantSession:     session{name: "valid-session"},
		},
		{
			name: "Session needs refresh",
			initialSession: session{
				name:       "about-to-expire-session",
				createTime: time.Now().Add(-time.Hour),
			},
			refreshInterval: 1 * time.Second,
			wantSession:     session{name: "refreshed-session"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SessionRefreshTimeInterval = tt.refreshInterval
			cl, err := newAdapterClient(context.Background(), Options{
				DatabaseUri:   "test",
				GoogleApiOpts: SkipAuthOpts,
			})
			assert.NoError(t, err)
			cl.session = tt.initialSession
			gotSession, err := cl.getOrRefreshSession(ctx)
			assert.NoError(t, err)
			if gotSession.name != tt.wantSession.name {
				t.Errorf(
					"getOrRefreshSession() session name = %v, want %v",
					gotSession.name,
					tt.wantSession.name,
				)
			}

		})
	}
}

func TestGetAllClientOpts(t *testing.T) {
	t.Parallel()
	opts := Options{}
	clientOpts, err := getAllClientOpts(opts)
	assert.NoError(t, err)
	assert.NotEmpty(t, clientOpts)

	opts.SpannerEndpoint = "some.endpoint"
	clientOpts, err = getAllClientOpts(opts)
	assert.NoError(t, err)
	assert.NotEmpty(t, clientOpts)

	opts.UsePlainText = true
	clientOpts, err = getAllClientOpts(opts)
	assert.NoError(t, err)
	assert.NotEmpty(t, clientOpts)

	opts.UsePlainText = false
	opts.ExperimentalHost = true
	clientOpts, err = getAllClientOpts(opts)
	assert.NoError(t, err)
	assert.NotEmpty(t, clientOpts)
}

func TestCreateExperimentalHostNoCredentials(t *testing.T) {
	t.Parallel()
	creds, err := createExperimentalHostCredentials("", "", "")
	assert.NoError(t, err)
	assert.Nil(t, creds)
}

func createDummyCerts(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "client.crt")
	keyFile = filepath.Join(dir, "client.key")

	// Simplified dummy content, not valid certs
	assert.NoError(t, os.WriteFile(caFile, []byte("CA CERT"), 0644))
	assert.NoError(t, os.WriteFile(certFile, []byte("CLIENT CERT"), 0644))
	assert.NoError(t, os.WriteFile(keyFile, []byte("CLIENT KEY"), 0644))
	return
}

func TestCreateExperimentalHostCredentials(t *testing.T) {
	caFile, certFile, keyFile := createDummyCerts(t)

	tests := []struct {
		name       string
		caCert     string
		clientCert string
		clientKey  string
		wantErr    bool
		expectTLS  bool
		expectMTLS bool
	}{
		{
			name:    "No certs",
			wantErr: false,
		},
		{
			name:    "CA cert only - invalid content",
			caCert:  caFile, // Will fail to parse as PEM
			wantErr: true,
		},
		{
			name:       "Client cert without key",
			caCert:     caFile,
			clientCert: certFile,
			wantErr:    true,
		},
		{
			name:      "Client key without cert",
			caCert:    caFile,
			clientKey: keyFile,
			wantErr:   true,
		},
		{
			name:       "All certs - invalid content",
			caCert:     caFile,
			clientCert: certFile,
			clientKey:  keyFile,
			wantErr:    true, // Will fail to load key pair
		},
		// Success cases are hard to test without valid PEM data
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt, err := createExperimentalHostCredentials(tt.caCert, tt.clientCert, tt.clientKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("createExperimentalHostCredentials() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.caCert != "" {
				assert.NotNil(t, opt, "Expected a ClientOption to be returned")
			} else if !tt.wantErr && tt.caCert == "" {
				assert.Nil(t, opt, "Expected nil option when no CA cert")
			}
		})
	}
}
