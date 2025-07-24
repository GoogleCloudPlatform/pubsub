/*
 * This UDF is used to convert distances between metric and imperial systems.
 * 
 * @param {Object} message - The message containing distance values to convert.
 * @param {Object} metadata - The metadata of the message.
 * @returns {Object} The message with converted distance values.
 */
function unit_convert_distance(message, metadata) {
    // Parse the message
    const data = JSON.parse(message.data);
    
    // Conversion factors
    const METERS_TO_FEET = 3.28084;
    const FEET_TO_METERS = 0.3048;
    const KILOMETERS_TO_MILES = 0.621371;
    const MILES_TO_KILOMETERS = 1.60934;
    
    // Convert metric to imperial if metric field exists
    if (data['metric_distance']) {
        const meters = parseFloat(data['metric_distance']);
        data['imperial_distance'] = (meters * METERS_TO_FEET).toFixed(2);
        data['imperial_unit'] = 'feet';
    }
    
    // Convert imperial to metric if imperial field exists
    if (data['imperial_distance']) {
        const feet = parseFloat(data['imperial_distance']);
        data['metric_distance'] = (feet * FEET_TO_METERS).toFixed(2);
        data['metric_unit'] = 'meters';
    }
    
    // Convert kilometers to miles if kilometers field exists
    if (data['kilometers']) {
        const km = parseFloat(data['kilometers']);
        data['miles'] = (km * KILOMETERS_TO_MILES).toFixed(2);
    }
    
    // Convert miles to kilometers if miles field exists
    if (data['miles']) {
        const miles = parseFloat(data['miles']);
        data['kilometers'] = (miles * MILES_TO_KILOMETERS).toFixed(2);
    }
    
    // Update the message with converted data
    message.data = JSON.stringify(data);
    
    return message;
}
