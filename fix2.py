dst = r'templates\index.html'

with open(dst, encoding='utf-8') as f:
    content = f.read()

# Remove everything from openDocumentDetail to end of deleteRecord
start = content.find('async function openDocumentDetail')
end = content.find('</script>')
content = content[:start] + content[end:]

# Insert both clean functions before </script>
clean = '''async function openDocumentDetail(i) {
  const r = await fetch('/api/master-log/'+i).then(function(x){return x.json();}).catch(function(){return null;});
  if (!r || r.error) { alert('Could not load record'); return; }
  const fields = ['document_type','vendor_name','vendor_category','account_number',
    'property','unit','service_address','document_date','due_date','amount_due',
    'current_charges','previous_balance','payments_received','late_fees',
    'payment_status','source_file','final_storage_path','confidence_score','timestamp'];
  var tbody = '';
  fields.forEach(function(k) {
    tbody += '<tr style="border-bottom:1px solid #f0f0e8">'
      + '<td style="padding:6px 10px;color:#888;font-size:11px;font-weight:600;text-transform:uppercase;width:160px">'
      + k.replace(/_/g,' ') + '</td>'
      + '<td style="padding:6px 10px;font-size:13px">' + (r[k]||'-') + '</td></tr>';
  });
  var overlay = document.createElement('div');
  overlay.id = 'doc-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:999;display:flex;align-items:flex-start;justify-content:center;padding:30px 16px;overflow-y:auto';
  var box = document.createElement('div');
  box.style.cssText = 'background:#fff;border-radius:12px;width:620px;max-width:100%;box-shadow:0 20px 60px rgba(0,0,0,0.3)';
  var hdr = '<div style="padding:16px 20px;border-bottom:1px solid #e5e5e0;display:flex;justify-content:space-between;align-items:center">'
    + '<div><h2 style="font-size:15px;font-weight:500">' + (r.vendor_name||'Document') + '</h2>'
    + '<div style="font-size:12px;color:#888">' + (r.document_date||'') + ' ' + (r.amount_due?'$'+r.amount_due:'') + ' ' + (r.property||'') + '</div></div>'
    + '<button id="doc-close-btn" style="background:none;border:none;cursor:pointer;font-size:22px;color:#888">x</button></div>';
  var bdy = '<div style="padding:20px"><table style="width:100%;border-collapse:collapse">' + tbody + '</table></div>';
  var ftr = '<div style="padding:14px 20px;border-top:1px solid #e5e5e0;display:flex;gap:8px;justify-content:flex-end">'
    + '<button id="doc-delete-btn" style="padding:7px 14px;border-radius:6px;border:1px solid #991b1b;background:#991b1b;color:#fff;cursor:pointer;font-size:13px">Delete record + file</button>'
    + '<button id="doc-close-btn2" style="padding:7px 14px;border-radius:6px;border:1px solid #d0d0c8;background:#fff;cursor:pointer;font-size:13px">Close</button></div>';
  box.innerHTML = hdr + bdy + ftr;
  overlay.appendChild(box);
  document.body.appendChild(overlay);
  document.getElementById('doc-close-btn').onclick = function() { overlay.remove(); };
  document.getElementById('doc-close-btn2').onclick = function() { overlay.remove(); };
  document.getElementById('doc-delete-btn').onclick = function() { deleteRecord(i, overlay); };
}

async function deleteRecord(i, overlay) {
  if (!confirm('Delete this record AND its filed PDF permanently?')) return;
  const res = await fetch('/api/master-log/'+i, {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({delete_file: true})
  }).then(function(x){return x.json();}).catch(function(){return null;});
  if (res && res.ok) {
    overlay.remove();
    refreshRecent();
    refreshActivity();
  } else {
    alert('Delete failed');
  }
}
</script>'''

content = content.replace('</script>', clean, 1)

with open(dst, 'w', encoding='utf-8') as f:
    f.write(content)

print('Done! Lines:', sum(1 for _ in open(dst, encoding='utf-8')))
