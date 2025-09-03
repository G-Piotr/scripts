# xlsx_size_audit: a tiny analyzer for bloated .xlsx files
#
# What it does
#  - Treats your .xlsx as a .zip and lists every internal file with sizes
#  - Highlights common culprits: xl/media/*, xl/sharedStrings.xml, xl/worksheets/*
#  - Produces a CSV report + a concise TXT summary
#
# How to use (inside this notebook or your local Python):
#   1) Set XLSX_PATH to your file path.
#   2) Run the cell. If the path exists, you'll get a table preview and downloadable reports.
#
# If you prefer a standalone script, this cell also writes /mnt/data/xlsx_size_audit.py which you can download and run via:
#   python xlsx_size_audit.py /path/to/file.xlsx
#
import os, io, zipfile, csv, math, sys, textwrap
from datetime import datetime

# Try to import pandas for a nicer table; fall back if unavailable.
try:
    import pandas as pd
    HAVE_PANDAS = True
except Exception:
    HAVE_PANDAS = False

SCRIPT_PATH = "/mnt/data/xlsx_size_audit.py"

SCRIPT_SOURCE = r'''#!/usr/bin/env python3
import os, zipfile, math, sys, argparse, xml.etree.ElementTree as ET

def human_bytes(n: int) -> str:
    if n is None:
        return "—"
    units = ["B","KB","MB","GB","TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"

def count_shared_strings(zf: zipfile.ZipFile, path="xl/sharedStrings.xml"):
    try:
        with zf.open(path) as f:
            # Fast-ish streaming count of <si> elements
            # Don't load whole file into memory.
            count = 0
            buf = b""
            for chunk in iter(lambda: f.read(65536), b""):
                buf += chunk
                # Count occurrences of b"<si" in the chunk buffer (naive but robust enough)
                # Keep last few bytes in case "<s" "<i" across boundary
                count += buf.count(b"<si")
                buf = buf[-3:]
            return count
    except KeyError:
        return None
    except Exception:
        return None

def scan_xlsx(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with zipfile.ZipFile(path) as zf:
        rows = []
        total_uncompressed = 0
        total_compressed = 0
        media_total = 0
        media_count = 0
        worksheets_total = 0
        sharedstrings_size = None
        sharedstrings_count = None

        for info in zf.infolist():
            name = info.filename
            comp = info.compress_size
            uncomp = info.file_size
            total_uncompressed += uncomp
            total_compressed += comp

            if name.startswith("xl/media/"):
                media_total += uncomp
                media_count += 1

            if name.startswith("xl/worksheets/"):
                worksheets_total += uncomp

            if name == "xl/sharedStrings.xml":
                sharedstrings_size = uncomp

            ratio = (1 - (comp / uncomp)) * 100 if uncomp > 0 else 0.0
            rows.append({
                "path": name,
                "uncompressed_bytes": uncomp,
                "compressed_bytes": comp,
                "compression_savings_%": round(ratio, 2),
            })

        # Optional: estimate count of shared strings (can be costly for huge files; keep simple/streamy)
        if sharedstrings_size is not None:
            try:
                sharedstrings_count = count_shared_strings(zf)
            except Exception:
                sharedstrings_count = None

    rows.sort(key=lambda r: r["uncompressed_bytes"], reverse=True)
    summary = {
        "total_uncompressed": total_uncompressed,
        "total_compressed": total_compressed,
        "overall_compression_savings_%": round((1 - (total_compressed / total_uncompressed)) * 100, 2) if total_uncompressed else 0.0,
        "media_count": media_count,
        "media_uncompressed_total": media_total,
        "worksheets_uncompressed_total": worksheets_total,
        "sharedStrings_uncompressed": sharedstrings_size,
        "sharedStrings_est_count": sharedstrings_count,
    }

    return rows, summary

def suggest_actions(summary, top_paths):
    tips = []
    # Media
    if summary.get("media_count", 0) > 0 and summary.get("media_uncompressed_total", 0) > 10 * 1024 * 1024:
        tips.append("Folder xl/media/ jest duży → rozważ kompresję obrazów (JPG/WebP), usunięcie duplikatów lub zmniejszenie rozdzielczości.")

    # Shared strings
    ss = summary.get("sharedStrings_uncompressed")
    if ss and ss > 10 * 1024 * 1024:
        tips.append("Plik xl/sharedStrings.xml jest bardzo duży → rozważ usunięcie nadmiarowych tekstów, zamianę formuł tekstowych na wartości lub deduplikację powtarzalnych wartości.")

    # Worksheets
    if summary.get("worksheets_uncompressed_total", 0) > 20 * 1024 * 1024:
        tips.append("Duże arkusze w xl/worksheets/ → sprawdź 'używany zakres' (Used Range). Usuń puste wiersze/kolumny na końcu i zapisz plik na nowo.")

    # Top offenders
    if top_paths:
        worst = top_paths[0]["path"]
        tips.append(f"Największy element: {worst} → sprawdź, czy jest potrzebny i czy da się go zredukować.")

    if not tips:
        tips.append("Nie widać jednego, oczywistego winowajcy — rozważ oczyszczenie używanego zakresu, kompresję obrazów i usunięcie zbędnych stylów/formatowania.")

    return tips

def main():
    ap = argparse.ArgumentParser(description="Analiza rozmiaru pliku .xlsx (zachowuje się jak archiwum ZIP).")
    ap.add_argument("xlsx_path", help="Ścieżka do pliku .xlsx")
    ap.add_argument("--top", type=int, default=25, help="Ile największych elementów pokazać (domyślnie 25)")
    ap.add_argument("--csv", default=None, help="Zapisz szczegóły do CSV (domyślnie obok pliku, z sufiksem _xlsx_audit.csv)")
    ap.add_argument("--txt", default=None, help="Zapisz podsumowanie do TXT (domyślnie obok pliku, z sufiksem _xlsx_audit.txt)")
    args = ap.parse_args()

    rows, summary = scan_xlsx(args.xlsx_path)
    top_n = rows[:args.top]

    # Print console summary
    print("== PODSUMOWANIE ==")
    print(f"Plik: {args.xlsx_path}")
    print(f"Łączny rozmiar (uncompressed): {human_bytes(summary['total_uncompressed'])}")
    print(f"Łączny rozmiar (compressed):   {human_bytes(summary['total_compressed'])}")
    print(f"Kompresja ogółem:              {summary['overall_compression_savings_%']}%")
    print(f"Obrazy: {summary['media_count']} plików, razem {human_bytes(summary['media_uncompressed_total'])}")
    if summary.get('sharedStrings_uncompressed'):
        print(f"sharedStrings.xml:             {human_bytes(summary['sharedStrings_uncompressed'])}"
              + (f", ~liczba wpisów: {summary['sharedStrings_est_count']}" if summary.get('sharedStrings_est_count') is not None else ""))
    print(f"Worksheets total:              {human_bytes(summary['worksheets_uncompressed_total'])}")
    print()

    # Print offenders
    print("== NAJWIĘKSZE ELEMENTY ==")
    for r in top_n:
        print(f"{human_bytes(r['uncompressed_bytes']).rjust(10)}  {r['path']} (savings {r['compression_savings_%']}%)")

    # Suggestions
    print("\n== SUGESTIE ==")
    for tip in suggest_actions(summary, top_n):
        print(f"- {tip}")

    # Exports
    base, ext = os.path.splitext(args.xlsx_path)
    csv_path = args.csv or f"{base}_xlsx_audit.csv"
    txt_path = args.txt or f"{base}_xlsx_audit.txt"

    # Save CSV
    try:
        import csv
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["path","uncompressed_bytes","compressed_bytes","compression_savings_%"])
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"\nCSV zapisany: {csv_path}")
    except Exception as e:
        print(f"Nie udało się zapisać CSV: {e}")

    # Save TXT summary
    try:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("PODSUMOWANIE\n")
            f.write(f"Plik: {args.xlsx_path}\n")
            f.write(f"Łączny rozmiar (uncompressed): {human_bytes(summary['total_uncompressed'])}\n")
            f.write(f"Łączny rozmiar (compressed):   {human_bytes(summary['total_compressed'])}\n")
            f.write(f"Kompresja ogółem:              {summary['overall_compression_savings_%']}%\n")
            f.write(f"Obrazy: {summary['media_count']} plików, razem {human_bytes(summary['media_uncompressed_total'])}\n")
            if summary.get('sharedStrings_uncompressed'):
                f.write(f"sharedStrings.xml:             {human_bytes(summary['sharedStrings_uncompressed'])}")
                if summary.get('sharedStrings_est_count') is not None:
                    f.write(f", ~liczba wpisów: {summary['sharedStrings_est_count']}")
                f.write("\n")
            f.write(f"Worksheets total:              {human_bytes(summary['worksheets_uncompressed_total'])}\n\n")
            f.write("NAJWIĘKSZE ELEMENTY\n")
            for r in rows[:25]:
                f.write(f"{human_bytes(r['uncompressed_bytes']).rjust(10)}  {r['path']} (savings {r['compression_savings_%']}%)\n")
            f.write("\nSUGESTIE\n")
            from textwrap import fill
            from shutil import get_terminal_size
            width = 100
            for tip in suggest_actions(summary, rows[:25]):
                f.write(f"- {tip}\n")
        print(f"TXT zapisany: {txt_path}")
    except Exception as e:
        print(f"Nie udało się zapisać TXT: {e}")

if __name__ == "__main__":
    main()
'''
# Write the standalone script for download
with open(SCRIPT_PATH, "w", encoding="utf-8") as f:
    f.write(SCRIPT_SOURCE)

# Make it executable (best-effort; might not matter in this environment)
try:
    os.chmod(SCRIPT_PATH, 0o755)
except Exception:
    pass

print("Utworzono skrypt:", SCRIPT_PATH)

# Optional: inline helper to run analysis inside this notebook session
def analyze_xlsx_inline(xlsx_path: str, top=25):
    import zipfile, os
    if not os.path.exists(xlsx_path):
        print("Plik nie istnieje:", xlsx_path)
        return None, None
    from collections import defaultdict
    with zipfile.ZipFile(xlsx_path) as zf:
        rows = []
        total_uncompressed = 0
        total_compressed = 0
        for info in zf.infolist():
            comp = info.compress_size
            uncomp = info.file_size
            total_uncompressed += uncomp
            total_compressed += comp
            ratio = (1 - (comp / uncomp)) * 100 if uncomp > 0 else 0.0
            rows.append({
                "path": info.filename,
                "uncompressed_bytes": uncomp,
                "compressed_bytes": comp,
                "compression_savings_%": round(ratio, 2),
            })
    rows.sort(key=lambda r: r["uncompressed_bytes"], reverse=True)
    summary = {
        "total_uncompressed": total_uncompressed,
        "total_compressed": total_compressed,
        "overall_compression_savings_%": round((1 - (total_compressed / total_uncompressed)) * 100, 2) if total_uncompressed else 0.0,
    }
    if HAVE_PANDAS:
        df = pd.DataFrame(rows)
        from caas_jupyter_tools import display_dataframe_to_user
        display_dataframe_to_user("Szczegóły plików wewnątrz XLSX", df)
    return rows, summary

# Example placeholder: set your path here if you want to run inline
XLSX_PATH = ""  # <- wstaw pełną ścieżkę do Twojego pliku .xlsx i uruchom ponownie tę komórkę

if XLSX_PATH:
    rows, summary = analyze_xlsx_inline(XLSX_PATH, top=25)
    if rows:
        print("Top 10 największych elementów:")
        for r in rows[:10]:
            print(f"{r['uncompressed_bytes']:>12}  {r['path']}")
        print("Podsumowanie:", summary)