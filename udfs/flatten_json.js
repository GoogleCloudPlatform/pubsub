/*
 * This UDF is used to flatten nested JSON objects in a message.
 * 
 * @param {Object} message - The message containing nested JSON to flatten.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with flattened JSON.
 */
function flatten_json(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Function to flatten nested objects
    function flatten(obj, prefix = '') {
        return Object.keys(obj).reduce((acc, key) => {
            const pre = prefix.length ? `${prefix}_` : '';
            
            if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
                Object.assign(acc, flatten(obj[key], `${pre}${key}`));
            } else {
                acc[`${pre}${key}`] = obj[key];
            }
            
            return acc;
        }, {});
    }
    
    // Flatten the data
    const flattenedData = flatten(data);
    
    // Update the message with flattened data
    message.data = JSON.stringify(flattenedData);
    
    return message;
}