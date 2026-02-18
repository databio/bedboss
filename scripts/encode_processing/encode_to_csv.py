"""
ENCODE JSON to CSV extraction script.

!! BEFORE RUNNING THIS SCRIPT, DOWNLOAD ENCODE METADATA:
FOR only hg38 files:
```
curl -L -H "Accept: application/json" "https://www.encodeproject.org/search/?type=File&file_format=bed&assembly=GRCh38&limit=100000&status=released&format=json" -o encode_bed_page1.json
```

Streams an ENCODE JSON metadata file and extracts selected fields
into a CSV. Uses ijson for memory-efficient parsing of large files (1GB+).

Usage:
    python encode_to_csv.py --input encode_bed_page_smal.json --output output.csv
"""

import argparse
import csv
import re

import ijson

from bedboss.bbuploader.metadata_extractor import cell_lines, find_cell_line

CSV_COLUMNS = [
    "sample_name",
    "assay",
    "genome",
    "description",
    "link",
    "sample_id",
    "experiment_id",
    "file_size",
    "cell_line",
    "cell_type",
    "tissue",
    "target",
    "treatment",
    "date_created",
]

# Regex to extract treatment from simple_biosample_summary
# Matches "treated with ..." up to "genetically" or end of string
_TREATMENT_RE = re.compile(r"treated with (.+?)(?=\s*(?:genetically|$))", re.IGNORECASE)


def extract_id_from_path(path: str, prefix: str) -> str:
    """Extract ID from patterns like /files/ENCFF023BDU/ or /experiments/ENCSR733DAS/."""
    match = re.search(rf"/{prefix}/([^/]+)/", path)
    return match.group(1) if match else path


def process_record(record: dict) -> dict:
    """Extract and transform fields from a single @graph record."""
    raw_id = record.get("@id", "")
    sample_name = extract_id_from_path(raw_id, "files") if raw_id else ""

    dataset = record.get("dataset", "")
    # dataset can be /experiments/ID/, /annotations/ID/, or just ID
    experiment_id = dataset.strip("/").split("/")[-1] if dataset else ""

    biosample = record.get("biosample_ontology") or {}
    if isinstance(biosample, list):
        biosample = biosample[0] if biosample else {}
    term_name = biosample.get("term_name", "")

    # Try exact match first, fall back to substring match (find_cell_line)
    term_lower = term_name.lower()
    if term_lower in cell_lines:
        cell_line = cell_lines[term_lower]
    else:
        cell_line = find_cell_line(term_name)

    if cell_line:
        cell_type = ""
    elif term_name and term_name.replace("-", "").replace(" ", "").isupper():
        # All-uppercase names (e.g. "GM12878", "HCT116") are likely cell lines
        cell_line = term_name
        cell_type = ""
    else:
        cell_type = term_name
        cell_line = ""

    organ_slims = biosample.get("organ_slims") or []
    tissue = (
        ", ".join(organ_slims) if isinstance(organ_slims, list) else str(organ_slims)
    )

    # Antibody/protein target (present for ChIP-seq)
    target = record.get("target") or {}
    target = target.get("label", "") if isinstance(target, dict) else ""

    # Extract treatment from description (no dedicated field exists)
    description = record.get("simple_biosample_summary", "")
    treatment_match = _TREATMENT_RE.search(description)
    treatment = treatment_match.group(1).strip().rstrip(",") if treatment_match else ""

    # Assay may be a string or list
    assay = record.get("assay_term_name", "")
    if isinstance(assay, list):
        assay = ", ".join(assay)

    return {
        "sample_name": sample_name,
        "assay": assay,
        "genome": record.get("assembly", ""),
        "description": description,
        "link": record.get("href", ""),
        "sample_id": record.get("accession", ""),
        "experiment_id": experiment_id,
        "file_size": record.get("file_size", ""),
        "cell_line": cell_line,
        "cell_type": cell_type,
        "tissue": tissue,
        "target": target,
        "treatment": treatment,
        "date_created": record.get("date_created", ""),
    }


def main(input_path: str, output_path: str) -> None:
    """Stream-parse the ENCODE JSON and write extracted records to CSV."""
    with open(input_path, "rb") as infile, open(
        output_path, "w", newline=""
    ) as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        count = 0
        for record in ijson.items(infile, "@graph.item"):
            row = process_record(record)
            writer.writerow(row)
            count += 1

    print(f"Done. Wrote {count} records to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract ENCODE JSON metadata to CSV")
    parser.add_argument("--input", required=True, help="Path to ENCODE JSON file")
    parser.add_argument("--output", required=True, help="Path to output CSV file")
    args = parser.parse_args()
    main(args.input, args.output)
