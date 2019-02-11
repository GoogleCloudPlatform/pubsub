class FlowController {
    constructor() {
    }

    // Get a promise for when to start the next message.
    // The value of the promise is the number of allowed messages.
    async requestStart() {
        throw new Error('Unimplemented.');
    }

    // Inform the flow controller of a completion.
    informFinished(wasSuccessful) {
        throw new Error('Unimplemented.');
    }
}

module.exports = FlowController;
