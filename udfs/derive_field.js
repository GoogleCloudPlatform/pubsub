/*
 * This UDF is used to derive a composite field from existing fields in the message data.
 * 
 * @param {Object} message - The message containing the fields to combine.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with the new composite field.
 */
function derive_field(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define the fields to combine and the new field name
    const fieldsToCombine = ['field1', 'field2', 'field3'];
    const newFieldName = 'composite_field';
    
    // Create composite field by combining values - custom logic goes here
    const compositeValue = fieldsToCombine
        .map(field => data[field] || '')
        .filter(value => value !== '')
        .join('_');
    
    // Add the composite field to the data
    data[newFieldName] = compositeValue;
    
    // Update the message with new data
    message.data = JSON.stringify(data);
    
    return message;
}
