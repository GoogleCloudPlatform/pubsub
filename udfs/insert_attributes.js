/*
 * This UDF is used to insert three new attributes into the message metadata.
 * 
 * @param {Object} message - The message to insert attributes into.
 * @param {Object} metadata - The metadata to modify.
 * @returns {Object} The message with new attributes in metadata.
 */
function insert_attributes(message, metadata) {
   
    // Define the new attributes to insert
    const newAttributes = {
        'attribute1': 'value1',
        'attribute2': 'value2',
        'attribute3': 'value3'
    };
    
    // Add new attributes to metadata
    Object.assign(metadata, newAttributes);
    
    return message;
}
