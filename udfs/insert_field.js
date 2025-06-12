/*
 * This UDF is used to insert a new field into the message data.
 * 
 * @param {Object} message - The message to insert the field into.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with the inserted field.
 */
function insertField(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define the field to insert and its value
    const newField = 'inserted_field';
    const newValue = 'default_value';
    
    // Insert the new field
    data[newField] = newValue;
    
    // Update the message with inserted field
    message.data = JSON.stringify(data);
    
    return message;
}
