/*
 * This UDF is used to convert a timestamp field to a specified format.
 * 
 * @param {Object} message - The message containing the timestamp to convert.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with the converted timestamp.
 */
function timestamp_converter(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Define the timestamp field and target format
    const timestampField = 'timestamp';

    // Target format
    const targetFormat = 'YYYY-MM-DD HH:mm:ss';
    
    // Check if timestamp field exists
    if (data[timestampField]) {
        // Convert timestamp to Date object
        const date = new Date(data[timestampField]);
        
        // Format the date according to target format
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        const seconds = String(date.getSeconds()).padStart(2, '0');
        
        // Create formatted timestamp
        const formattedTimestamp = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        
        // Add formatted timestamp to data
        data['formatted_timestamp'] = formattedTimestamp;
    }
    
    // Update the message with converted timestamp
    message.data = JSON.stringify(data);
    
    return message;
}
