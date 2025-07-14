/**
 * Hoists the 'event_time' field from message.data to the top level of the message object, if present.
 * @param {Object} message - The message object with a JSON string in message.data.
 * @returns {Object} The message with event_time at the top level if it existed in data.
 */
function hoist(message, metadata) {

   // Parse the message
   const data = JSON.parse(message.data);

    // check if the event_time field exists
    if (data && data.hasOwnProperty('event_time')) {
        message.event_time = data['event_time'];
    }

    // Update the message with new data
    message.data = JSON.stringify(data);
    
    return message;
}