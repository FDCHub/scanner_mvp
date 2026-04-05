import base64,os
c=base64.b64decode(open(r'fix_index.py').read().split('BASE64:')[1].strip())
open(r'D:\\document_ai_system\\scanner_mvp\\templates\\index.html','wb').write(c)
print('Done',os.path.getsize(r'D:\\document_ai_system\\scanner_mvp\\templates\\index.html'))