/*
 * Test file for url_parse.js UDF
 * Tests various scenarios for URL parameter extraction
 */

// Import the UDF function
const { url_parse } = require('./url_parse.js');

// Test data and expected results
const testCases = [
    {
        name: "Basic URL with query parameters",
        input: {
            data: JSON.stringify({
                url_field: "https://example.com/path?param1=value1&param2=value2&param3=123"
            })
        },
        expected: {
            param1: "value1",
            param2: "value2", 
            param3: "123"
        }
    },
    {
        name: "URL with no parameters",
        input: {
            data: JSON.stringify({
                url_field: "https://example.com/path"
            })
        },
        expected: {}
    },
    {
        name: "URL with empty parameters",
        input: {
            data: JSON.stringify({
                url_field: "https://example.com/path?param1=&param2=value2"
            })
        },
        expected: {
            param1: "",
            param2: "value2"
        }
    },
    {
        name: "URL with special characters in parameters",
        input: {
            data: JSON.stringify({
                url_field: "https://example.com/path?name=John%20Doe&email=john@example.com&query=hello+world"
            })
        },
        expected: {
            name: "John Doe",
            email: "john@example.com",
            query: "hello world"
        }
    },
    {
        name: "URL with duplicate parameters (should keep last value)",
        input: {
            data: JSON.stringify({
                url_field: "https://example.com/path?param1=first&param1=second&param2=value2"
            })
        },
        expected: {
            param1: "second",
            param2: "value2"
        }
    },
    {
        name: "URL with complex parameter values",
        input: {
            data: JSON.stringify({
                url_field: "https://api.example.com/search?q=javascript&page=1&limit=10&sort=name&order=asc&filter=active"
            })
        },
        expected: {
            q: "javascript",
            page: "1",
            limit: "10",
            sort: "name",
            order: "asc",
            filter: "active"
        }
    }
];

// Error test cases
const errorTestCases = [
    {
        name: "Invalid URL format",
        input: {
            data: JSON.stringify({
                url_field: "not-a-valid-url"
            })
        },
        shouldThrow: true
    },
    {
        name: "Missing url_field",
        input: {
            data: JSON.stringify({
                other_field: "https://example.com"
            })
        },
        shouldThrow: true
    },
    {
        name: "Empty url_field",
        input: {
            data: JSON.stringify({
                url_field: ""
            })
        },
        shouldThrow: true
    },
    {
        name: "Invalid JSON in message data",
        input: {
            data: "invalid-json"
        },
        shouldThrow: true
    }
];

// Test runner function
function runTests() {
    console.log("🧪 Testing url_parse UDF function\n");
    
    let passedTests = 0;
    let totalTests = testCases.length + errorTestCases.length;
    
    // Test normal cases
    console.log("📋 Testing normal cases:");
    testCases.forEach((testCase, index) => {
        try {
            const result = url_parse(testCase.input, {});
            const resultData = JSON.parse(result.data);
            const extractedParams = resultData.url_parameters;
            
            // Compare results
            const isMatch = JSON.stringify(extractedParams) === JSON.stringify(testCase.expected);
            
            if (isMatch) {
                console.log(`✅ Test ${index + 1}: ${testCase.name}`);
                passedTests++;
            } else {
                console.log(`❌ Test ${index + 1}: ${testCase.name}`);
                console.log(`   Expected: ${JSON.stringify(testCase.expected)}`);
                console.log(`   Got: ${JSON.stringify(extractedParams)}`);
            }
        } catch (error) {
            console.log(`❌ Test ${index + 1}: ${testCase.name} - Unexpected error: ${error.message}`);
        }
    });
    
    console.log("\n🚨 Testing error cases:");
    errorTestCases.forEach((testCase, index) => {
        try {
            const result = url_parse(testCase.input, {});
            if (testCase.shouldThrow) {
                console.log(`❌ Error Test ${index + 1}: ${testCase.name} - Expected error but got result`);
            } else {
                console.log(`✅ Error Test ${index + 1}: ${testCase.name}`);
                passedTests++;
            }
        } catch (error) {
            if (testCase.shouldThrow) {
                console.log(`✅ Error Test ${index + 1}: ${testCase.name} - Correctly threw: ${error.message}`);
                passedTests++;
            } else {
                console.log(`❌ Error Test ${index + 1}: ${testCase.name} - Unexpected error: ${error.message}`);
            }
        }
    });
    
    console.log(`\n📊 Test Results: ${passedTests}/${totalTests} tests passed`);
    
    if (passedTests === totalTests) {
        console.log("🎉 All tests passed! The UDF function is working correctly.");
    } else {
        console.log("⚠️  Some tests failed. Please review the implementation.");
    }
}

// Manual test function for interactive testing
function manualTest() {
    console.log("🔧 Manual Test Mode");
    console.log("Enter a URL to test (or 'quit' to exit):");
    
    const readline = require('readline');
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });
    
    rl.on('line', (input) => {
        if (input.toLowerCase() === 'quit') {
            rl.close();
            return;
        }
        
        try {
            const testMessage = {
                data: JSON.stringify({
                    url_field: input
                })
            };
            
            const result = url_parse(testMessage, {});
            const resultData = JSON.parse(result.data);
            
            console.log("\n📤 Input URL:", input);
            console.log("📥 Extracted parameters:", JSON.stringify(resultData.url_parameters, null, 2));
            console.log("\nEnter another URL (or 'quit' to exit):");
        } catch (error) {
            console.log("❌ Error:", error.message);
            console.log("\nEnter another URL (or 'quit' to exit):");
        }
    });
}

// Export functions for use in other test files
module.exports = {
    runTests,
    manualTest,
    testCases,
    errorTestCases
};

// Run tests if this file is executed directly
if (require.main === module) {
    const args = process.argv.slice(2);
    
    if (args.includes('--manual')) {
        manualTest();
    } else {
        runTests();
    }
} 