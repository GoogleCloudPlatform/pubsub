/*
 * This UDF is used to URL decode specific fields within a message.
 * 
 * @param {Object} message - The message containing fields to URL decode.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with URL decoded fields.
 */
function url_decode(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // URL decode the specified field (e.g. encodedString)
    if (data['encodedString'] !== undefined) {
        data['decodedString'] = decodeURIComponent(data['encodedString']);
    }
    
    // Update the message with decoded data
    message.data = JSON.stringify(data);
    
    return message;
}
