from chatgpt_analyzer import analyze_document  # text-based Claude wrapper
import json

test_text = """
Florida Public Utilities
Account Number: 200000352910
Service Address: 3047 Sea Marsh Rd
Bill Date: 03/11/2026
Due Date: 03/31/2026
Amount Due: $253.92
"""

result = analyze_document(test_text)

print(json.dumps(result, indent=2))