import os
import pandas as pd
import pdfplumber
import re
from pathlib import Path

def extract_state_name(filename):
    """Extract state name from filename"""
    base_name = Path(filename).stem
    special_cases = {
        'South_Dakota': 'South Dakota',
        'West_Virginia': 'West Virginia',
        'New_Mexico': 'New Mexico',
        'New_Hampshire': 'New Hampshire',
        'New_Jersey': 'New Jersey',
        'New_York': 'New York',
        'North_Carolina': 'North Carolina',
        'North_Dakota': 'North Dakota',
        'Rhode_Island': 'Rhode Island',
        'South_Carolina': 'South Carolina'
    }
    return special_cases.get(base_name, base_name.replace('_', ' ').title())

def find_partner_indicators():
    """Define patterns that indicate partner tables"""
    return {
        'start_patterns': [
            re.compile(r'PARTNER', re.IGNORECASE),
            re.compile(r'Table.*Partner', re.IGNORECASE),
            re.compile(r'Partners.*Description', re.IGNORECASE),
            re.compile(r'Name.*Description', re.IGNORECASE),
            re.compile(r'Organization.*Role', re.IGNORECASE),
            re.compile(r'TABLE\s+\d+:?\s*PARTNER', re.IGNORECASE),  # "TABLE 6: PARTNERS"
        ],
        'stop_patterns': [
            re.compile(r'TABLE\s+\d+(?!\s*:?\s*Partner)', re.IGNORECASE),  # Don't stop on "Table X: Partners"
            re.compile(r'Asset.*Inventory', re.IGNORECASE),
            re.compile(r'^\s*3\.\d+', re.MULTILINE),  # Section numbers at start of line
            re.compile(r'^\s*4\.\d+', re.MULTILINE),
            re.compile(r'Exhibit\s+\d+(?!\s*:?\s*Partner)', re.IGNORECASE),  # Don't stop on partner exhibits
        ],
        'continue_patterns': [
            re.compile(r'Partners.*Description', re.IGNORECASE),
            re.compile(r'Partners.*Role', re.IGNORECASE),
            re.compile(r'PARTNER', re.IGNORECASE),
        ]
    }

def extract_partners_from_pdf(pdf_path):
    """Extract partners from all tables with 2+ columns"""
    state_name = extract_state_name(pdf_path)
    print(f"\nProcessing {state_name}...")
    
    all_rows = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables()
                page_found_data = False
                
                if tables:
                    print(f"Page {page_num}: Found {len(tables)} tables")
                    
                    for table_idx, tbl in enumerate(tables):
                        if not tbl or len(tbl) < 2:
                            continue
                        
                        # Check if table has at least 2 columns
                        if len(tbl[0]) < 2:
                            continue
                        
                        rows_added = 0
                        
                        # Process all rows, skipping obvious headers
                        for row_idx, row in enumerate(tbl):
                            if not row or len(row) < 2:
                                continue
                            
                            # Handle None values
                            partner = str(row[0]).strip() if row[0] else ""
                            description = str(row[1]).strip() if row[1] else ""
                            
                            # Skip if either is empty
                            if not partner or not description:
                                continue
                            
                            # Skip header-like rows
                            if (partner.lower() in ['partner', 'partners', 'name', 'organization', 'entity'] or 
                                description.lower() in ['description', 'role', 'current', 'planned']):
                                continue
                            
                            # More lenient validation
                            if len(partner) >= 2 and len(description) >= 2:
                                all_rows.append([partner, description])
                                rows_added += 1
                                page_found_data = True
                        
                        if rows_added > 0:
                            print(f"  Table {table_idx + 1}: Added {rows_added} partners")
                
                # If no data found from tables, try text extraction
                if not page_found_data:
                    text_page = page.extract_text() or ""
                    if text_page.strip():  # Only try if there's actual text
                        text_partners = extract_partners_from_text_structured(text_page)
                        if text_partners:
                            all_rows.extend(text_partners)
                            print(f"Page {page_num}: Added {len(text_partners)} partners from text")
                
    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")
        return []
    
    print(f"Total raw entries: {len(all_rows)}")
    return all_rows

def extract_partners_from_text_structured(text):
    """Extract partners from text when table extraction fails"""
    partners = []
    lines = text.split('\n')
    
    # Look for partner-description pairs in text
    current_partner = None
    current_desc = []
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 5:
            continue
        
        # Skip section headers and table headers
        if any(word in line.lower() for word in ['table', 'section', 'partnerships', 'description of current']):
            continue
        
        # Check if this looks like an organization name
        if is_likely_organization_name(line):
            # Save previous partner if we have one
            if current_partner and current_desc:
                partners.append([current_partner, ' '.join(current_desc)])
            
            current_partner = line
            current_desc = []
            
        elif current_partner and len(line) > 20:  # Looks like description text
            current_desc.append(line)
            
            # Don't let descriptions get too long
            if len(' '.join(current_desc)) > 500:
                partners.append([current_partner, ' '.join(current_desc)])
                current_partner = None
                current_desc = []
    
    # Don't forget the last one
    if current_partner and current_desc:
        partners.append([current_partner, ' '.join(current_desc)])
    
    return partners

def extract_partners_from_text(text_page, found_table):
    """Extract partners from text when tables aren't available"""
    partners = []
    
    # Only try text extraction if we haven't found tables yet or if text looks structured
    if not found_table or has_structured_text(text_page):
        lines = text_page.split('\n')
        
        # Look for organization-like names
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Skip short lines and obvious headers
            if len(line) < 10 or line.lower().startswith(('table', 'page', 'description')):
                continue
            
            # Check if line looks like an organization name
            if is_likely_organization_name(line):
                # Try to get description from next line(s)
                description = ""
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if not is_likely_organization_name(next_line) and len(next_line) > 20:
                        description = next_line
                
                partners.append([line, description])
    
    return partners

def has_structured_text(text):
    """Check if text appears to have structured partner information"""
    # Look for common organizational patterns
    org_indicators = ['association', 'department', 'commission', 'authority', 'council', 
                     'bureau', 'agency', 'corporation', 'university', 'college']
    
    org_count = sum(1 for indicator in org_indicators if indicator in text.lower())
    return org_count >= 3

def is_likely_organization_name(text):
    """Check if text looks like an organization name"""
    if len(text) < 5 or len(text) > 150:
        return False
    
    # Organization indicators
    org_words = ['association', 'department', 'commission', 'authority', 'council', 
                 'bureau', 'agency', 'corporation', 'university', 'college', 'institute',
                 'foundation', 'center', 'network', 'partnership', 'alliance', 'cooperative',
                 'company', 'group', 'board', 'office', 'system', 'district']
    
    text_lower = text.lower()
    
    # Check for organizational words
    if any(word in text_lower for word in org_words):
        return True
    
    # Check for acronyms in parentheses
    if re.search(r'\([A-Z]{2,}\)', text):
        return True
    
    # Check for title case pattern (but not all caps)
    words = text.split()
    if (len(words) >= 2 and len(words) <= 10 and
        sum(1 for word in words if word[0].isupper()) >= len(words) * 0.7 and
        not text.isupper()):
        return True
    
    return False

def process_pdf(pdf_path, output_folder):
    """Process a single PDF file"""
    state_name = extract_state_name(pdf_path)
    
    # Extract partners
    partner_rows = extract_partners_from_pdf(pdf_path)
    
    if not partner_rows:
        print(f"No partners found in {state_name}")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(partner_rows, columns=["Partner", "Description"])
    
    # Clean up data like in Alabama script
    df["Partner"] = df["Partner"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["Description"] = df["Description"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['Partner'])
    
    print(f"Extracted {len(df)} partners from {state_name}")
    
    # Save to Excel
    output_file = os.path.join(output_folder, f"{state_name.replace(' ', '_')}_Partners.xlsx")
    df.to_excel(output_file, index=False, sheet_name='Partners')
    
    print(f"Saved: {output_file}")
    return len(df)

def main():
    input_folder = "pdf_files"
    output_folder = "extracted_partners"
    
    # Create output folder
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Find PDF files
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in {input_folder}")
        return
    
    print(f"Found {len(pdf_files)} PDF files")
    
    results = []
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_folder, pdf_file)
        try:
            partner_count = process_pdf(pdf_path, output_folder)
            state_name = extract_state_name(pdf_file)
            
            if partner_count:
                results.append({
                    'State': state_name,
                    'Partners_Found': partner_count,
                    'Status': 'Success'
                })
            else:
                results.append({
                    'State': state_name,
                    'Partners_Found': 0,
                    'Status': 'No partners found'
                })
                
        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            results.append({
                'State': extract_state_name(pdf_file),
                'Partners_Found': 'Error',
                'Status': f'Error: {str(e)}'
            })
    
    # Create summary
    summary_df = pd.DataFrame(results)
    summary_file = os.path.join(output_folder, 'Processing_Summary.xlsx')
    summary_df.to_excel(summary_file, index=False)
    
    print(f"\n--- SUMMARY ---")
    print(f"Total files: {len(pdf_files)}")
    successful = len([r for r in results if r['Status'] == 'Success'])
    print(f"Successful: {successful}")
    
    total_partners = sum(r['Partners_Found'] for r in results if isinstance(r['Partners_Found'], int))
    print(f"Total partners extracted: {total_partners}")
    print(f"Summary saved: {summary_file}")

if __name__ == "__main__":
    main()