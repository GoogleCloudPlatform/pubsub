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
        'field1': 'number',
        'field2': 'boolean',
        'field3': 'string'
    };
    
    // Cast each field according to the type map
    Object.entries(typeMap).forEach(([field, type]) => {
        if (data[field] !== undefined) {
            switch (type) {
                case 'number':
                    data[field] = Number(data[field]);
                    break;
                case 'boolean':
                    data[field] = Boolean(data[field]);
                    break;
                case 'string':
                    data[field] = String(data[field]);
                    break;
                default:
                    console.warn(`Unsupported type: ${type} for field: ${field}`);
            }
        }
    });
    
    // Update the message with casted data
    message.data = JSON.stringify(data);
    
    return message;
}
