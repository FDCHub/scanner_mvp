from pathlib import Path
from pdf2image import convert_from_path
import pytesseract

pdf_path = Path(r"D:\Scans\Working")

pdf_files = list(pdf_path.glob("*.pdf"))

if not pdf_files:
    print("No PDF files found in D:\\Scans\\Working")
    raise SystemExit

target_pdf = pdf_files[0]
print(f"Testing OCR on: {target_pdf.name}")

images = convert_from_path(str(target_pdf), dpi=300, first_page=1, last_page=1)

text = pytesseract.image_to_string(images[0])

print("\n--- OCR TEXT START ---\n")
print(text[:2000])
print("\n--- OCR TEXT END ---\n")