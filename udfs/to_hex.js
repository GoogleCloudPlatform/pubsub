/*
 * This UDF is used to convert a string field within a message to hex.
 * 
 * @param {Object} message - The message to convert to hex.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with the hex value.
 */
function to_hex(message, metadata) {
    // parse the messagae
    const data = JSON.parse(message.data);
    
    // convert to field1 to hex
    let hex = '';
    for (let i = 0; i < data['field1'].length; i++) {
      let charCode = data['field1'].charCodeAt(i);
      let hexValue = charCode.toString(16);
      hex += (hexValue.length < 2) ? '0' + hexValue : hexValue;
    }
    data['field1'] = hex;
  
    message.data = JSON.stringify(data);
  
    return message;
}