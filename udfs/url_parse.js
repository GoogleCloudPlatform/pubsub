/*
 * This UDF is used to extract URL parameters from a field in a message.
 * 
 * @param {Object} message - The message containing the URL to parse.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with extracted URL parameters.
 */
function url_parse(message, metadata) {
    
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Get the URL from the specified field
    const url = data['url_field'];
    
    // Create URL object and extract parameters
    const urlObj = new URL(url);
    const params = {};
    
    // Extract all URL parameters
    urlObj.searchParams.forEach((value, key) => {
        params[key] = value;
    });
    
    // Add extracted parameters to the message data
    data['url_parameters'] = params;
    
    // Update the message with new data
    message.data = JSON.stringify(data);
    
    return message;
}