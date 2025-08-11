#!/usr/bin/env python3
"""
Statewide North Carolina Public Construction Bid Scraper
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from datetime import datetime
import logging
from urllib.parse import urljoin
from pathlib import Path
import argparse
import tabula

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NCBidScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.output_dir = Path("nc_data")
        self.raw_dir = self.output_dir / "raw_pdfs"
        self.processed_dir = self.output_dir / "processed_data"
        self.reports_dir = self.output_dir / "reports"

        # Comprehensive list of NC public bid portals
        self.base_urls = [
            # TIER 1: STATEWIDE AGENCIES (Highest Yield)
            "https://ncadmin.nc.gov/businesses/construction/projects-design-advertised-bidding", # State/University Buildings
            "https://connect.ncdot.gov/letting/Pages/default.aspx",                         # NCDOT Projects

            # TIER 2: MAJOR COUNTIES & CITIES
            "https://www.mecknc.gov/Finance/Procurement/Pages/Solicitations.aspx",           # Mecklenburg County
            "https://www.wake.gov/departments-government/finance/business-inclusion-procurement/solicitation-opportunities", # Wake County
            "https://www.charlottenc.gov/Businesses/Business-Inclusion/Vendor-Management", # City of Charlotte
            "https://raleighnc.gov/doing-business/bids-and-proposals",                    # City of Raleigh
            "https://www.cmsk12.org/en-US/doing-business/solicitations"                      # Charlotte-Mecklenburg Schools
        ]
        self.all_bids = []

    def get_page(self, url, retries=3, delay=2):
        for attempt in range(retries):
            try:
                time.sleep(delay)
                response = self.session.get(url, timeout=45)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1: return None
        return None

    def find_project_links(self, page_url):
        logging.info(f"Searching for project links on: {page_url}")
        response = self.get_page(page_url)
        if not response: return []
        
        soup = BeautifulSoup(response.content, 'lxml')
        links = soup.find_all('a', href=True)
        project_links = []
        
        keywords = ['bid', 'tabulation', 'letting', 'award', 'solicitation', 'project']
        for link in links:
            link_text = link.get_text(strip=True)
            if any(keyword in link_text.lower() for keyword in keywords) and link['href'].lower().endswith('.pdf'):
                project_links.append({
                    'name': link_text,
                    'url': urljoin(page_url, link['href']),
                    'source': page_url
                })
        logging.info(f"Found {len(project_links)} potential PDF links.")
        return project_links

    def process_pdf(self, project):
        logging.info(f"Processing PDF: {project['name']}")
        response = self.get_page(project['url'])
        if not response: return

        pdf_path = self.raw_dir / (re.sub(r'[^\w\s-]', '', project['name'])[:100].replace(' ', '_') + '.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(response.content)

        try:
            tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, stream=True)
            if not tables:
                logging.warning(f"Tabula found no tables in {pdf_path.name}")
                return

            for i, df in enumerate(tables):
                if df.empty or len(df.columns) < 2: continue
                
                df['project_name'] = project['name']
                df['source_url'] = project['url']
                df['pdf_source_file'] = pdf_path.name
                df['pdf_table_index'] = i
                df['scrape_timestamp'] = datetime.now().isoformat()
                
                # Clean column names
                df.columns = [str(col).lower().replace('\r', ' ').replace('\n', ' ').strip().replace(' ', '_') for col in df.columns]
                
                self.all_bids.append(df)
            logging.info(f"Successfully extracted {len(tables)} table(s) from {pdf_path.name}")

        except Exception as e:
            logging.error(f"Failed to process PDF {pdf_path.name} with Tabula: {e}")

    def run(self, mode='test'):
        all_project_links = []
        urls_to_scan = self.base_urls if mode == 'full' else [self.base_urls[0]] # Only scan first URL in test mode
        
        for url in urls_to_scan:
            all_project_links.extend(self.find_project_links(url))
        
        unique_projects = {p['url']: p for p in all_project_links}.values()
        logging.info(f"Found {len(unique_projects)} unique project PDFs to process.")
        
        projects_to_process = list(unique_projects) if mode == 'full' else list(unique_projects)[:5] # Limit to 5 PDFs in test mode

        for project in projects_to_process:
            self.process_pdf(project)

        if self.all_bids:
            final_df = pd.concat(self.all_bids, ignore_index=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = self.processed_dir / f"nc_bid_data_{timestamp}.csv"
            final_df.to_csv(output_path, index=False)
            logging.info(f"✅ Success! Saved {len(final_df)} total bid rows to {output_path}")
        else:
            logging.warning("No data was extracted. Output file not created.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape NC public construction bids.")
    parser.add_argument('--mode', type=str, default='test', choices=['test', 'full'], help='Run mode: test or full.')
    args = parser.parse_args()
    
    scraper = NCBidScraper()
    scraper.run(mode=args.mode)
