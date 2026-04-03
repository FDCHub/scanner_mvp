from confidence_scorer import score_document

print(score_document(True, True, True, True, False, False, False, False, False))   # expect auto
print(score_document(False, True, True, False, False, False, False, False, False)) # expect review
print(score_document(True, True, True, True, True, False, False, False, False))    # expect review
print(score_document(True, True, True, True, False, False, True, False, False))    # expect review