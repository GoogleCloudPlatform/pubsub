/*
 * This UDF is used to filter messages based on a custom condition defined in
 * the filter function.
 *
 * @param {Object} message - The message to filter.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message if it meets the custom condition, otherwise null.
 */
function filter_custom(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);

    // Filter out messages that match the filter function.
    if (filter(data)) {
        return null; // Return null to filter out the message
    }

    // Return the original message if not filtering
    return message;
}