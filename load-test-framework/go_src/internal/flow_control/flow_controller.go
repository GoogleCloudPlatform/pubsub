package flow_control

import "go/types"

type FlowController interface {
	// Request to start another operation, returns the channel to wait on
	Start() <-chan types.Nil
	// Inform the flow controller that an operation has finished
	InformFinished(wasSuccessful bool)
}
