/*
 * This UDF is used to redact PII fields from a message.
 * 
 * @param {Object} message - The message to redact.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The redacted message.
 */
function redact(message, metadata) {
    // parse the messagae
    const data = JSON.parse(message.data);
    
    // redact PII fields
    delete data['field1'];
    delete data['field2'];
    delete data['field3'];
  
    message.data = JSON.stringify(data);
  
    return message;
}