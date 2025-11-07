import os
import tarfile
import re
import glob
import arxiv
from collections import defaultdict


def extract_base_arxiv_id(filename):
    base = filename.replace('.tar.gz', '')
    match = re.match(r'^(\d{4}\.\d{5})', base)
    if match:
        return match.group(1)
    return None


def get_paper_metadata(arxiv_id):
    try:
        client = arxiv.Client()
        search = arxiv.Search(id_list=[arxiv_id])
        paper = next(client.results(search))

        bibtex_key = f"arxiv:{arxiv_id.replace('.', '')}"
        authors = " and ".join([author.name for author in paper.authors])
        
        escaped_title = paper.title.replace('{', r'\{').replace('}', r'\}')
        escaped_abstract = paper.summary.replace('{', r'\{').replace('}', r'\}')
        
        bibtex = f"""@article{{{bibtex_key},
            title={{{escaped_title}}},
            author={{{authors}}},
            journal={{arXiv preprint arXiv:{arxiv_id}}},
            year={{{paper.published.year}}},
            month={{{paper.published.month}}},
            eprint={{{arxiv_id}}},
            primaryClass={{{paper.primary_category}}},
            url={{https://arxiv.org/abs/{arxiv_id}}},
            archivePrefix={{arXiv}},
            eprinttype={{arXiv}},
            abstract={{{escaped_abstract}}}
        }}"""
        return bibtex, paper

    except Exception as e:
        print(f"  Warning: Could not fetch metadata for {arxiv_id}: {e}")
        bibtex_key = f"arxiv:{arxiv_id.replace('.', '')}"
        bibtex = f"""@article{{{bibtex_key},
                    title={{arXiv:{arxiv_id}}},
                    eprint={{{arxiv_id}}},
                    url={{https://arxiv.org/abs/{arxiv_id}}},
                    archivePrefix={{arXiv}},
                    eprinttype={{arXiv}}
                }}"""
        return bibtex, None


def organize_papers(sources_dir="./sources", output_dir="."):
    sources_path = os.path.abspath(sources_dir)
    output_path = os.path.abspath(output_dir)

    if not os.path.exists(sources_path):
        print(f"Error: Sources directory '{sources_dir}' does not exist.")
        return

    paper_files = defaultdict(list)

    print("Scanning source files...")
    tar_files = glob.glob(os.path.join(sources_path, "*.tar.gz"))
    print(f"Found {len(tar_files)} tar.gz files")

    for tar_file in tar_files:
        base_id = extract_base_arxiv_id(os.path.basename(tar_file))
        if base_id:
            paper_files[base_id].append(tar_file)
        else:
            print(f"Warning: Could not extract arXiv ID from {tar_file}")

    print(f"\nFound {len(paper_files)} unique papers")
    print("=" * 60)

    for idx, (arxiv_id, files) in enumerate(sorted(paper_files.items()), 1):
        folder_name = arxiv_id.replace('.', '-')
        paper_dir = os.path.join(output_path, folder_name)
        tex_dir = os.path.join(paper_dir, "tex")

        print(f"\n[{idx}/{len(paper_files)}] Processing {arxiv_id} -> {folder_name}/")

        os.makedirs(tex_dir, exist_ok=True)

        print(f"  Extracting {len(files)} version(s)...")
        for tar_file in sorted(files):
            version_match = re.search(r'v(\d+)', os.path.basename(tar_file))
            version = version_match.group(1) if version_match else "1"

            version_dir = os.path.join(tex_dir, f"v{version}")
            os.makedirs(version_dir, exist_ok=True)

            try:
                with tarfile.open(tar_file, 'r:gz') as tar:
                    tar.extractall(path=version_dir)
                print(f"    ✓ Extracted {os.path.basename(tar_file)} -> tex/v{version}/")
            except Exception as e:
                print(f"    ✗ Error extracting {os.path.basename(tar_file)}: {e}")

        print(f"  Fetching metadata and generating BibTeX...")
        bibtex_content, paper = get_paper_metadata(arxiv_id)

        bib_file = os.path.join(paper_dir, "references.bib")
        with open(bib_file, 'w', encoding='utf-8') as f:
            f.write(bibtex_content)
        print(f"    ✓ Created references.bib")

        if paper:
            print(f"    Title: {paper.title[:60]}...")

    print("\n" + "=" * 60)
    print(f"Organization complete!")
    print(f"Processed {len(paper_files)} papers")
    print(f"Output directory: {output_path}")


if __name__ == "__main__":
    organize_papers(sources_dir="./sources", output_dir=".")
