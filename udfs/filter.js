/*
 * Returns true if the message should be filtered out
 *
 * @param {Object} data - The message to filter.
 */
function filter(data) {
    // Filter out messages that are not from US region.
    return data['region'] !== "US";
}