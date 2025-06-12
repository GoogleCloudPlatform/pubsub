/*
 * This UDF is used to extract a field from metadata and add it to the message data.
 * 
 * @param {Object} message - The message to modify.
 * @param {Object} metadata - The metadata containing the field to extract.
 * @returns {Object} The message with the extracted field added to data.
 */
function upgrade_attribute(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define the field to extract from metadata
    const fieldToExtract = 'attribute_to_upgrade';
    
    // Check if field exists in metadata
    if (metadata[fieldToExtract] !== undefined) {
        // Add field to data
        data[fieldToExtract] = metadata[fieldToExtract];
        
        // Remove field from metadata
        delete metadata[fieldToExtract];
    }
    
    // Update the message with modified data
    message.data = JSON.stringify(data);
    
    return message;
}
