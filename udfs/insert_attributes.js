/*
 * This UDF is used to insert three new attributes into the message.
 * 
 * @param {Object} message - The message to insert attributes into.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with new attributes.
 */
function insert_attributes(message, metadata) {
   
    const attributes = message.attributes;

    // Add new attributes
    attributes["key1"] = 'value1';
    attributes["key2"] = 'value2';
    attributes["key3"] = 'value3';

    return message;
}
