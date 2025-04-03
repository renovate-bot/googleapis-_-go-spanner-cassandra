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

const (
	// Maximum size of local singleton state maintained across all requests.
	// ~100mb
	maxGlobalStateSize = 1e8 / 256
	// prefix for prepared query id state updates
	preparedQueryIdAttachmentPrefix = "pqid/"
)
