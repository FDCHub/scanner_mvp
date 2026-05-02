import base64,os
c=base64.b64decode(open(r'fix_index.py').read().split('BASE64:')[1].strip())
open(r'templates\\index.html','wb').write(c)
print('Done',os.path.getsize(r'templates\\index.html'))