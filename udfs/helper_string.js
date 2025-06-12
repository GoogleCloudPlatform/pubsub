/*
 * This UDF provides various string manipulation helper functions.
 * 
 * @param {Object} message - The message containing string fields to manipulate.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with manipulated string fields.
 */
function helper_string(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Helper function to capitalize first letter of each word
    const capitalizeWords = (str) => {
        return str.split(' ')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
            .join(' ');
    };
    
    // Helper function to remove special characters
    const removeSpecialChars = (str) => {
        return str.replace(/[^a-zA-Z0-9\s]/g, '');
    };
    
    // Helper function to truncate string with ellipsis
    const truncateString = (str, maxLength) => {
        return str.length > maxLength ? str.substring(0, maxLength) + '...' : str;
    };
    
    // Helper function to normalize whitespace
    const normalizeWhitespace = (str) => {
        return str.replace(/\s+/g, ' ').trim();
    };
    
    // Apply transformations to specified fields
    data['name'] = capitalizeWords(data['name']);
    data['description'] = removeSpecialChars(data['description']);
    data['description'] = truncateString(data['description'], 100);
    data['notes'] = normalizeWhitespace(data['notes']);
    
    // Update the message with transformed data
    message.data = JSON.stringify(data);
    
    return message;
}



