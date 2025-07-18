#!/usr/bin/env python3
"""
Test script for url_parse.js UDF function
This script simulates the JavaScript UDF behavior and tests various scenarios
"""

import json
import urllib.parse
from urllib.parse import urlparse, parse_qs
import sys

def simulate_url_parse(message, metadata=None):
    """
    Simulate the JavaScript url_parse UDF function behavior
    """
    try:
        # Parse the message data
        if isinstance(message['data'], str):
            data = json.loads(message['data'])
        else:
            data = message['data']
        
        # Get the URL from the specified field
        url = data.get('url_field')
        if not url:
            raise ValueError("url_field is missing or empty")
        
        # Parse URL and extract parameters
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL format: {url}")
        
        # Extract query parameters
        params = {}
        if parsed_url.query:
            # Parse query string
            query_params = parse_qs(parsed_url.query)
            # Convert lists to single values (like JavaScript URLSearchParams)
            for key, values in query_params.items():
                params[key] = values[-1] if values else ""  # Take last value for duplicates
        
        # Add extracted parameters to the message data
        data['url_parameters'] = params
        
        # Update the message with new data
        message['data'] = json.dumps(data)
        
        return message
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in message data: {e}")
    except Exception as e:
        raise ValueError(f"Error processing URL: {e}")

# Test cases
test_cases = [
    {
        "name": "Basic URL with query parameters",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path?param1=value1&param2=value2&param3=123"
            })
        },
        "expected": {
            "param1": "value1",
            "param2": "value2", 
            "param3": "123"
        }
    },
    {
        "name": "URL with no parameters",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path"
            })
        },
        "expected": {}
    },
    {
        "name": "URL with empty parameters",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path?param1=&param2=value2"
            })
        },
        "expected": {
            "param1": "",
            "param2": "value2"
        }
    },
    {
        "name": "URL with special characters in parameters",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path?name=John%20Doe&email=john@example.com&query=hello+world"
            })
        },
        "expected": {
            "name": "John Doe",
            "email": "john@example.com",
            "query": "hello world"
        }
    },
    {
        "name": "URL with duplicate parameters (should keep last value)",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path?param1=first&param1=second&param2=value2"
            })
        },
        "expected": {
            "param1": "second",
            "param2": "value2"
        }
    },
    {
        "name": "URL with complex parameter values",
        "input": {
            "data": json.dumps({
                "url_field": "https://api.example.com/search?q=javascript&page=1&limit=10&sort=name&order=asc&filter=active"
            })
        },
        "expected": {
            "q": "javascript",
            "page": "1",
            "limit": "10",
            "sort": "name",
            "order": "asc",
            "filter": "active"
        }
    },
    {
        "name": "URL with array-like parameters",
        "input": {
            "data": json.dumps({
                "url_field": "https://example.com/path?tags=tag1&tags=tag2&tags=tag3"
            })
        },
        "expected": {
            "tags": "tag3"  # JavaScript URLSearchParams takes last value
        }
    }
]

# Error test cases
error_test_cases = [
    {
        "name": "Invalid URL format",
        "input": {
            "data": json.dumps({
                "url_field": "not-a-valid-url"
            })
        },
        "should_throw": True
    },
    {
        "name": "Missing url_field",
        "input": {
            "data": json.dumps({
                "other_field": "https://example.com"
            })
        },
        "should_throw": True
    },
    {
        "name": "Empty url_field",
        "input": {
            "data": json.dumps({
                "url_field": ""
            })
        },
        "should_throw": True
    },
    {
        "name": "Invalid JSON in message data",
        "input": {
            "data": "invalid-json"
        },
        "should_throw": True
    }
]

def run_tests():
    """Run all test cases"""
    print("🧪 Testing url_parse UDF function (Python simulation)\n")
    
    passed_tests = 0
    total_tests = len(test_cases) + len(error_test_cases)
    
    # Test normal cases
    print("📋 Testing normal cases:")
    for i, test_case in enumerate(test_cases, 1):
        try:
            result = simulate_url_parse(test_case["input"])
            result_data = json.loads(result["data"])
            extracted_params = result_data["url_parameters"]
            
            # Compare results
            is_match = extracted_params == test_case["expected"]
            
            if is_match:
                print(f"✅ Test {i}: {test_case['name']}")
                passed_tests += 1
            else:
                print(f"❌ Test {i}: {test_case['name']}")
                print(f"   Expected: {test_case['expected']}")
                print(f"   Got: {extracted_params}")
        except Exception as error:
            print(f"❌ Test {i}: {test_case['name']} - Unexpected error: {error}")
    
    # Test error cases
    print("\n🚨 Testing error cases:")
    for i, test_case in enumerate(error_test_cases, 1):
        try:
            result = simulate_url_parse(test_case["input"])
            if test_case["should_throw"]:
                print(f"❌ Error Test {i}: {test_case['name']} - Expected error but got result")
            else:
                print(f"✅ Error Test {i}: {test_case['name']}")
                passed_tests += 1
        except Exception as error:
            if test_case["should_throw"]:
                print(f"✅ Error Test {i}: {test_case['name']} - Correctly threw: {error}")
                passed_tests += 1
            else:
                print(f"❌ Error Test {i}: {test_case['name']} - Unexpected error: {error}")
    
    print(f"\n📊 Test Results: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 All tests passed! The UDF function logic is working correctly.")
    else:
        print("⚠️  Some tests failed. Please review the implementation.")
    
    return passed_tests == total_tests

def manual_test():
    """Interactive manual testing"""
    print("🔧 Manual Test Mode")
    print("Enter a URL to test (or 'quit' to exit):")
    
    while True:
        try:
            user_input = input("URL: ").strip()
            if user_input.lower() == 'quit':
                break
            
            test_message = {
                "data": json.dumps({
                    "url_field": user_input
                })
            }
            
            result = simulate_url_parse(test_message)
            result_data = json.loads(result["data"])
            
            print(f"\n📤 Input URL: {user_input}")
            print(f"📥 Extracted parameters: {json.dumps(result_data['url_parameters'], indent=2)}")
            print()
            
        except Exception as error:
            print(f"❌ Error: {error}")
            print()

def analyze_javascript_code():
    """Analyze the JavaScript UDF code for potential issues"""
    print("🔍 Analyzing JavaScript UDF code...\n")
    
    # Read the JavaScript file
    try:
        with open('url_parse.js', 'r') as f:
            js_code = f.read()
        
        print("📄 JavaScript UDF Code Analysis:")
        print("=" * 50)
        
        # Check for potential issues
        issues = []
        
        # Check if URL constructor is used correctly
        if 'new URL(url)' in js_code:
            print("✅ Uses URL constructor correctly")
        else:
            issues.append("❌ Should use URL constructor for parsing")
        
        # Check if searchParams.forEach is used
        if 'searchParams.forEach' in js_code:
            print("✅ Uses searchParams.forEach for parameter extraction")
        else:
            issues.append("❌ Should use searchParams.forEach for parameter extraction")
        
        # Check if JSON.parse is used for message.data
        if 'JSON.parse(message.data)' in js_code:
            print("✅ Correctly parses message.data as JSON")
        else:
            issues.append("❌ Should parse message.data as JSON")
        
        # Check if url_parameters field is added
        if "url_parameters" in js_code:
            print("✅ Adds url_parameters field to output")
        else:
            issues.append("❌ Should add url_parameters field to output")
        
        # Check if JSON.stringify is used for output
        if 'JSON.stringify(data)' in js_code:
            print("✅ Correctly stringifies output data")
        else:
            issues.append("❌ Should stringify output data")
        
        # Check for error handling
        if 'try' in js_code or 'catch' in js_code:
            print("✅ Has error handling")
        else:
            issues.append("⚠️  Consider adding error handling")
        
        if issues:
            print("\n⚠️  Potential Issues:")
            for issue in issues:
                print(f"   {issue}")
        else:
            print("\n🎉 No obvious issues found in the JavaScript code!")
        
        print("\n📋 Code Structure:")
        print("- Function signature: url_parse(message, metadata)")
        print("- Input: Expects message.data to be JSON string with url_field")
        print("- Output: Returns message with url_parameters added")
        print("- URL parsing: Uses native URL constructor")
        print("- Parameter extraction: Uses searchParams.forEach")
        
    except FileNotFoundError:
        print("❌ Could not find url_parse.js file")
    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        manual_test()
    elif len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        analyze_javascript_code()
    else:
        run_tests()
        print("\n" + "="*60)
        analyze_javascript_code() 