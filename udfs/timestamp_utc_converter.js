/*
 * This UDF is used to convert a timestamp field to GMT.
 * 
 * @param {Object} message - The message containing the timestamp to convert.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with the converted timestamp.
 */
function timestamp_utc_converter(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Check if timestamp field exists
    if (data['timestamp']) {
        // Convert timestamp to GMT
        const date = new Date(data['timestamp']);
        data['gmt_timestamp'] = date.toUTCString();
    }
    
    // Update the message with converted timestamp
    message.data = JSON.stringify(data);
    
    return message;
}
