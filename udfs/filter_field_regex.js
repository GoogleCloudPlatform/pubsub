/*
 * This UDF is used to filter a field in the message data using a regular expression.
 * 
 * @param {Object} message - The message containing the field to filter.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with filtered field.
 */
function filter_field_regex(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define the field to filter and the regex pattern
    const fieldToFilter = 'text_field';
    const regexPattern = /^[A-Za-z0-9]+$/;  // Only allows alphanumeric characters
    
    // Check if the field exists and matches the regex pattern
    if (data[fieldToFilter] && typeof data[fieldToFilter] === 'string') {
        if (!regexPattern.test(data[fieldToFilter])) {
            // If the field doesn't match the pattern, remove it
            delete data[fieldToFilter];
        }
    }
    
    // Update the message with filtered data
    message.data = JSON.stringify(data);
    
    return message;
}

