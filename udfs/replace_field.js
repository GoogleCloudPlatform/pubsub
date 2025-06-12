/*
 * This UDF is used to rename fields within a message.
 * 
 * @param {Object} message - The message containing fields to rename.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with renamed fields.
 */
function replace_field(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define field mapping (old field name -> new field name)
    const fieldMapping = {
        'old_field1': 'new_field1',
        'old_field2': 'new_field2',
        'old_field3': 'new_field3'
    };
    
    // Rename fields according to the mapping
    Object.entries(fieldMapping).forEach(([oldField, newField]) => {
        if (data[oldField] !== undefined) {
            // Copy value to new field name
            data[newField] = data[oldField];
            // Remove old field
            delete data[oldField];
        }
    });
    
    // Update the message with renamed fields
    message.data = JSON.stringify(data);
    
    return message;
}
