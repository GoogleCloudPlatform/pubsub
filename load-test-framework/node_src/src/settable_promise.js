module.exports = class SettablePromise {
    constructor() {
        this.promise = new Promise(resolve => {
            this.resolve = resolve;
        });
    }

    set() {
        this.resolve(null);
    }
};