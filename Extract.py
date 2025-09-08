#!/usr/bin/env python3
"""
BEAD partners table extractor (single best-match, description-header strict)

- Column 2 header MUST contain the word "Description" (case-insensitive).
- Scans every table in the PDF, scores candidates, picks best one.
- Extracts rows from that table and any consecutive page continuations.
- Special-case handler for Nevada-style Provider/Award/Locations tables.
- Outputs Excel files to ./partners_xlsx/<state>_partners.xlsx
"""
import os
import re
import glob
import logging
from typing import List, Tuple, Dict

import pdfplumber
import pandas as pd

# Optional fuzzy matcher — used only as a fallback to detect partner header variants.
try:
    from fuzzywuzzy import fuzz
except Exception:
    fuzz = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bead-partners-extractor")

# Config
INPUT_DIR = "."
OUTPUT_DIR = "./partners_xlsx"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Strict requirement: Column 2 header must contain 'description'
DESC_HDR_RE = re.compile(r"\bdescription\b", re.IGNORECASE)

# Acceptable partner header patterns (explicit); fuzzy fallback if available
PARTNER_HDR_PATTERNS = [
    re.compile(r"\bpartner(s)?\b", re.IGNORECASE),
    re.compile(r"\bpartnership(s)?\b", re.IGNORECASE),
    re.compile(r"\bprovider(s)?\b", re.IGNORECASE),
]

# Nevada-style header check words (for the special handler)
NEV_HDR_WORDS = ["provider", "award", "locations"]

# Helper: normalize text
def norm(x):
    s = "" if x is None else str(x)
    s = re.sub(r"[\u00A0\u2007\u202F\u2009\u200B]", " ", s)           # NBSP variants
    s = re.sub(r"(?<=\w)-\s+(?=\w)", "", s)                         # hyphenation join
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Find candidate tables across whole PDF
def find_candidate_tables(pdf) -> List[Dict]:
    """
    For each table in the document, if header row contains a Description header
    and a Partner-like header, compute a small score and return candidates.
    """
    candidates = []
    for pno, page in enumerate(pdf.pages, start=1):
        try:
            tables = page.extract_tables() or []
        except Exception as e:
            logger.debug(f"Page {pno} extract_tables() failed: {e}")
            tables = []
        for t_idx, table in enumerate(tables):
            if not table or len(table) < 2:
                continue
            header = table[0]
            # require header to be a list-like of column strings
            # find description index (strict)
            desc_idx = None
            for i, cell in enumerate(header):
                if not cell:
                    continue
                if DESC_HDR_RE.search(norm(cell)):
                    desc_idx = i
                    break
            if desc_idx is None:
                # strict rule: skip this table if there's no "Description" header
                continue

            # find partner-like header index (more flexible)
            partner_idx = None
            # first try exact regex patterns
            for i, cell in enumerate(header):
                if not cell:
                    continue
                text = norm(cell)
                for pat in PARTNER_HDR_PATTERNS:
                    if pat.search(text):
                        partner_idx = i
                        break
                if partner_idx is not None:
                    break

            # fuzzy fallback (optional) — only used when exact patterns don't match
            if partner_idx is None and fuzz is not None:
                for i, cell in enumerate(header):
                    if not cell:
                        continue
                    txt = norm(cell).lower()
                    # check a few words
                    for target in ("partner", "partners", "partnership", "provider"):
                        if fuzz.ratio(txt, target) >= 75:
                            partner_idx = i
                            break
                    if partner_idx is not None:
                        break

            if partner_idx is None:
                # no partner header found — skip (must have both)
                continue

            # compute content_count (# of non-empty data rows) as small signal
            content_count = 0
            for row in table[1:]:
                if not row:
                    continue
                left = norm(row[partner_idx]) if partner_idx < len(row) else ""
                right = norm(row[desc_idx]) if desc_idx < len(row) else ""
                if left or right:
                    content_count += 1

            # header closeness / confidence
            header_score = 10  # desc required -> base high
            header_text = " ".join([norm(c) for c in header if c])
            if re.search(r"\bpartners?\b", header_text, re.I):
                header_score += 2
            if re.search(r"\bpartnerships?\b", header_text, re.I):
                header_score += 1

            # final score: header_score + small content bonus
            score = header_score + (content_count * 0.01)

            candidates.append({
                "page": pno,
                "table_idx": t_idx,
                "table": table,
                "partner_col": partner_idx,
                "desc_col": desc_idx,
                "ncols": len(header),
                "content_count": content_count,
                "score": score,
            })
    return candidates

def extract_rows_from_table(table, partner_col, desc_col) -> List[Tuple[str,str]]:
    rows = []
    for row in table[1:]:
        if not row:
            continue
        partner = norm(row[partner_col]) if partner_col < len(row) else ""
        description = norm(row[desc_col]) if desc_col < len(row) else ""
        # sometimes description spills into cols to the right — attempt to append if blank
        if not description and len(row) > desc_col:
            # fallback: join all later cells
            desc_parts = [norm(cell) for cell in row[desc_col:] if cell]
            if desc_parts:
                description = " ".join(desc_parts)
        if partner.strip() or description.strip():
            rows.append((partner, description))
    return rows

def extract_consecutive_continuations(pdf, start_page:int, best_ncols:int, best_partner_col:int, best_desc_col:int) -> Tuple[List[Tuple[str,str]], List[int]]:
    """
    Starting at start_page, extract rows from that page's best table (already handled elsewhere),
    then look at subsequent pages for continuation tables. We require continuations to be on
    consecutive pages and to look structurally similar (same or >= needed columns), or to have
    a repeated header with Description in it.
    """
    all_rows = []
    pages_used = []
    num_pages = len(pdf.pages)
    # extract initial page's matching table(s) — caller has already extracted the chosen table on start_page.
    # Here we'll check pages start_page+1, start_page+2, ... until a gap.
    page = start_page + 1
    while page <= num_pages:
        page_obj = pdf.pages[page-1]
        try:
            tables = page_obj.extract_tables() or []
        except Exception as e:
            logger.debug(f"Page {page} extract_tables() failed: {e}")
            break

        # find a table on this page that looks like continuation
        found = False
        for table in tables:
            if not table or len(table) < 1:
                continue
            header = table[0]
            # if header contains Description (repeated header), accept as continuation (and use mapped columns)
            header_text = " ".join([norm(h) for h in header if h]).lower()
            if DESC_HDR_RE.search(header_text):
                # find desc_col in this header row — try to align to best_desc_col if possible
                local_desc = None
                local_partner = None
                for i,cell in enumerate(header):
                    if DESC_HDR_RE.search(norm(cell)):
                        local_desc = i
                        break
                # find partner column index via explicit patterns or fuzzy fallback
                for i,cell in enumerate(header):
                    if any(p.search(norm(cell)) for p in PARTNER_HDR_PATTERNS):
                        local_partner = i
                        break
                if local_partner is None and fuzz is not None:
                    for i,cell in enumerate(header):
                        txt = norm(cell).lower()
                        for target in ("partner","provider","partnership"):
                            if fuzz.ratio(txt, target) >= 75:
                                local_partner = i
                                break
                        if local_partner is not None:
                            break
                # If partner col not found but column count matches, assume same positions as best
                if local_partner is None and len(header) >= max(best_partner_col, best_desc_col)+1:
                    local_partner = best_partner_col
                    local_desc = local_desc if local_desc is not None else best_desc_col
                if local_partner is None:
                    continue
                # Accept this table
                rows = extract_rows_from_table(table, local_partner, local_desc)
                if rows:
                    all_rows.extend(rows)
                    pages_used.append(page)
                    found = True
                    break

            else:
                # header does not repeat; check heuristic: this table has at least (best_partner_col & best_desc_col) columns
                # and contains a reasonable number of non-empty partner cells (heuristic threshold)
                partner_like_count = 0
                for r in table[1:]:
                    if not r:
                        continue
                    if best_partner_col < len(r) and norm(r[best_partner_col]):
                        partner_like_count += 1
                    elif len(r) > 0 and norm(r[0]):
                        # maybe partner is in first column
                        partner_like_count += 1
                # decide threshold: at least 1 non-empty partner-like cell and table has enough columns
                if partner_like_count >= 1 and (len(header) >= max(best_partner_col, best_desc_col)+1):
                    # assume same column mapping
                    rows = extract_rows_from_table(table, best_partner_col, best_desc_col)
                    if rows:
                        all_rows.extend(rows)
                        pages_used.append(page)
                        found = True
                        break
        if not found:
            # no plausible continuation found on this page -> stop
            break
        # else continue to next page (must be consecutive)
        page += 1

    return all_rows, pages_used

def extract_nevada_special(pdf_path: str) -> Tuple[pd.DataFrame, List[int]]:
    rows = []
    pages_used = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pno, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables() or []
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    header_row = table[0]
                    header_text = " ".join([norm(h) for h in header_row if h]).lower()
                    if all(w in header_text for w in NEV_HDR_WORDS):
                        pages_used.append(pno)
                        logger.info(f"Found Nevada provider table on page {pno}")
                        for row in table[1:]:
                            if not row:
                                continue
                            provider = norm(row[0]) if len(row) >= 1 else ""
                            desc_parts = [norm(c) for c in row[1:] if c]
                            desc = " | ".join(desc_parts)
                            if provider.strip():
                                rows.append((provider, desc))
    except Exception as e:
        logger.error(f"Nevada special extraction failed: {e}")
        return pd.DataFrame(), []
    df = pd.DataFrame(rows, columns=["Partner", "Description"])
    return df, sorted(set(pages_used))

def extract_best_partner_table(pdf_path: str) -> Tuple[pd.DataFrame, List[int]]:
    """
    Main logic: find best candidate table (must have a Description header),
    extract its rows, then attempt to collect consecutive-page continuations.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            candidates = find_candidate_tables(pdf)
            if not candidates:
                # no strict Description-header candidates in doc
                return pd.DataFrame(), []

            # pick best candidate by score (tie-break by earlier page)
            candidates = sorted(candidates, key=lambda c: (-c["score"], c["page"]))
            best = candidates[0]
            start_page = best["page"]
            partner_col = best["partner_col"]
            desc_col = best["desc_col"]
            ncols = best["ncols"]

            logger.info(f"Selected best table: page {start_page}, table {best['table_idx']+1}, "
                        f"partner_col={partner_col}, desc_col={desc_col}, score={best['score']:.2f}")

            rows = extract_rows_from_table(best["table"], partner_col, desc_col)
            pages_used = [start_page]

            # attempt to find consecutive continuations
            cont_rows, cont_pages = extract_consecutive_continuations(pdf, start_page, ncols, partner_col, desc_col)
            if cont_rows:
                rows.extend(cont_rows)
                pages_used.extend(cont_pages)

            if not rows:
                return pd.DataFrame(), []

            # dedupe & cleanup
            df = pd.DataFrame(rows, columns=["Partner", "Description"])
            for col in ["Partner", "Description"]:
                df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            df = df[(df["Partner"] != "") | (df["Description"] != "")]
            df = df.drop_duplicates().reset_index(drop=True)

            return df, sorted(set(pages_used))
    except Exception as e:
        logger.error(f"Error extracting from {pdf_path}: {e}")
        return pd.DataFrame(), []

def process_pdf_file(pdf_path: str):
    state = os.path.splitext(os.path.basename(pdf_path))[0].replace("_", " ")
    logger.info(f"\n=== Processing {state} ({pdf_path}) ===")

    # try the strict extraction first
    df, pages = extract_best_partner_table(pdf_path)

    # if empty and looks like Nevada, try special handler
    if df.empty and "nevada" in state.lower():
        logger.info("No strict partners table found — trying Nevada special handler.")
        df, pages = extract_nevada_special(pdf_path)

    if df.empty:
        logger.warning(f"⚠️ {state}: no Partner/Provider rows found")
        return

    # Save to Excel
    out_path = os.path.join(OUTPUT_DIR, f"{state}_partners.xlsx")
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Partners", index=False)
        start_page = min(pages) if pages else "?"
        end_page = max(pages) if pages else "?"
        logger.info(f"✅ {state}: {len(df)} rows from pages {start_page} - {end_page} → {out_path}")
        if len(df) > 0:
            logger.info("Sample entries:")
            for idx, row in df.head(5).iterrows():
                logger.info(f"  {row['Partner'][:80]} | {row['Description'][:100]}")
    except Exception as e:
        logger.error(f"❌ Failed to save {state}: {e}")

if __name__ == "__main__":
    pdfs = sorted(glob.glob(os.path.join(INPUT_DIR, "*.pdf")))
    if not pdfs:
        logger.warning("No PDF files found in the input directory.")
    for p in pdfs:
        process_pdf_file(p)
