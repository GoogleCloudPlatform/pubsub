/*
 * This UDF is used to insert three new attributes into the message metadata.
 * 
 * @param {Object} message - The message to insert attributes into.
 * @param {Object} metadata - The metadata to modify.
 * @returns {Object} The message with new attributes in metadata.
 */
function insert_attributes(message, metadata) {
   
    // Parse the message
    const data = JSON.parse(message.data);
    const attributes = message.attributes;

    // Add new attributes to insert
    attributes["key1"] = 'value1';
    attributes["key2"] = 'value2';
    attributes["key3"] = 'value3'; 

    // Update the message with modified data
    message.data = JSON.stringify(data);
        
    return message;
}
