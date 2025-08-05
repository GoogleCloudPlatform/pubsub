/*
 * This UDF is used to mask sensitive fields in a message.
 * 
 * @param {Object} message - The message to mask.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The masked message.
 */
function mask(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define fields to mask
    const fieldsToMask = ['field1', 'field2', 'field3'];
    
    // Mask each field by replacing with asterisks
    fieldsToMask.forEach(field => {
        if (data[field]) {
            data[field] = '*'.repeat(data[field].length);
        }
    });
    
    // Update the message with masked data
    message.data = JSON.stringify(data);
    
    return message;
}
