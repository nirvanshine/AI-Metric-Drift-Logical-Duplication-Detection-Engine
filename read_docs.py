import zipfile
import xml.etree.ElementTree as ET

def extract_docx(file_path):
    try:
        with zipfile.ZipFile(file_path) as docx:
            xml_content = docx.read('word/document.xml')
            tree = ET.XML(xml_content)
            
            WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
            PARA = WORD_NAMESPACE + 'p'
            TEXT = WORD_NAMESPACE + 't'
            
            paragraphs = []
            for paragraph in tree.iter(PARA):
                texts = [node.text
                         for node in paragraph.iter(TEXT)
                         if node.text]
                if texts:
                    paragraphs.append(''.join(texts))
            
            return '\n'.join(paragraphs)
    except Exception as e:
        return f"Error reading {file_path}: {e}"

req_file = r'C:\AI-Metric-Drift-Logical-Duplication-Detection-Engine\Requirements\AI Metric Drift & Logical Duplication Detection Accelerator- Requirements.docx'
trd_file = r'C:\AI-Metric-Drift-Logical-Duplication-Detection-Engine\Requirements\AI Metric Drift & Logical Duplication Detection Accelerator - TRD.docx'

with open('extracted_docs.txt', 'w', encoding='utf-8') as f:
    f.write("=== REQUIREMENTS DOC ===\n")
    f.write(extract_docx(req_file))
    f.write("\n\n=== TRD DOC ===\n")
    f.write(extract_docx(trd_file))
