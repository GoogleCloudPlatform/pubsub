/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package flow_control

type FlowController interface {
	// Request to start another operation, returns the channel to wait on.
	// A value received on the channel is the number of allowed operations.
	Start() <-chan int
	// Inform the flow controller that an operation has finished
	InformFinished(wasSuccessful bool)
}
