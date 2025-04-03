/*
Copyright 2025 Google LLC

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

// Options for configuring the adapter.
type Options struct {
	// Spanner database uri to connect to.
	DatabaseUri string
	// Optional Spanner service endpoint.
	SpannerEndpoint string
	// Protocol type (ie: cassandra).
	Protocol Protocol
	// Number of channels when dial grpc connection.
	NumGrpcChannels int
	// Optional Endpoint to start TCP server. If not specified, defaults to
	// 127.0.0.1:9042
	TCPEndpoint string
	// Whether to disable automatic grpc retry for AdaptMessage API
	DisableAdaptMessageRetry bool
}
