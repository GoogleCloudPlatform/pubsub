function filter_custom(message, metadata) {

    region = extractField(message, "region");
    
    if (region === "US") {
      return message;
    }
    
    return null;
  }