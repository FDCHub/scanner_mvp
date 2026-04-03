from new_doc_detector import is_new_document

print(is_new_document("Florida Public Utilities", "200000352910", True))   # expect False
print(is_new_document("Florida Public Utilities", "", True))               # expect True
print(is_new_document("", "", False))                                      # expect True
print(is_new_document("Unknown Vendor", "12345", False))                   # expect True