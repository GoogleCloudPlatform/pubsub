/*
 * This UDF is used to remove extra whitespaces from fields in a message.
 * 
 * @param {Object} message - The message containing fields to clean.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with cleaned fields.
 */
function remove_whitespaces(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Function to clean whitespaces from a value
    function cleanWhitespace(value) {
        if (typeof value === 'string') {
            // Replace multiple spaces with single space and trim
            return value.replace(/\s+/g, ' ').trim();
        }
        return value;
    }
    
    // Clean whitespaces from all string fields
    Object.keys(data).forEach(key => {
        data[key] = cleanWhitespace(data[key]);
    });
    
    // Update the message with cleaned data
    message.data = JSON.stringify(data);
    
    return message;
}
