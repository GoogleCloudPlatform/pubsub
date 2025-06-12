/*
 * This UDF is used to filter messages based on a custom condition.
 * 
 * @param {Object} message - The message to filter.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message if it meets the custom condition, otherwise null.
 */
function filter_custom(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Filter out from the message
    const region = data['region'];
    
    // Filter out messages that are not from US region
    if (region !== "US") {
        return null; // Return null to filter out the message
    }
    
    // Return the original message if region is US
    return message;
}