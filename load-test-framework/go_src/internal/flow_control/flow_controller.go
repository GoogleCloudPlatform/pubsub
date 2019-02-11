package flow_control

type FlowController interface {
	// Request to start another operation, returns the channel to wait on.
	// A value received on the channel is the number of allowed operations.
	Start() <-chan int
	// Inform the flow controller that an operation has finished
	InformFinished(wasSuccessful bool)
}
