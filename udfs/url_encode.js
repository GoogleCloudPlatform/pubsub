/*
 * This UDF is used to URL encode specific fields within a message.
 * 
 * @param {Object} message - The message containing fields to URL encode.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with URL encoded fields.
 */
function url_encode(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // URL encode the specified field
    if (data['url'] !== undefined) {
        data['url'] = encodeURIComponent(data['url']);
    }
    
    // Update the message with encoded data
    message.data = JSON.stringify(data);
    
    return message;
}
