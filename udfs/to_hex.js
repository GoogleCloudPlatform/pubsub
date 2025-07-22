/*
 * This UDF is used to convert a string field within a message to hex.
 * 
 * @param {Object} message - The message to convert to hex.
 * @param {Object} metadata - The metadata of the message.
  * @returns {number[]} An array of hex byte values.
 */
function to_hex(message, metadata) {
    // parse the messagae
    const data = JSON.parse(message.data);

    // get the string to convert
    const str = data['field1'];

    // convert to hex
    const bytes = stringToHexByteArray(str);

    // update the message with the hex value
    data['field1'] = bytes;
  
    message.data = JSON.stringify(data);
  
    return message;
}

function stringToHexByteArray(str) {
    // Converts a string to an array of hex byte values
    const bytes = [];
    for (let i = 0; i < str.length; i++) {
        bytes.push(str.charCodeAt(i).toString(16).padStart(2, '0'));
    }
    return bytes;
}