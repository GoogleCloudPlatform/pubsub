/*
 * This UDF is used to validate a message.
 * 
 * @param {Object} message - The message to validate.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The validated message.
 */
function validate(message, metadata) {
    
    const data = JSON.parse(message.data);


    if (data["field1"] < 0) {
      throw new Error("field1 is invalid");
    }
    
    return message;
}