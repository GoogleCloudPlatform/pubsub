/*
 * This UDF is used to cast fields within a message to specified types.
 * 
 * @param {Object} message - The message containing fields to cast.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with casted fields.
 */
function cast(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);

    // Define type casting map
    const typeMap = {
        'field1': String,
        'field2': String,
        'field3': String,
    };

    // Cast each field according to the type map
    Object.entries(typeMap).forEach(([field, type]) => {
        if (data[field] !== undefined) {
            data[field] = type(data[field]);
        }
    });

    // Update the message with new data
    message.data = JSON.stringify(data);
    
    return message;
}
