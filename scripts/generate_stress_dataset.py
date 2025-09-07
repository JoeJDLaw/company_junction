#!/usr/bin/env python3
"""Generate a 10K+ row stress test dataset for the Company Junction pipeline.

This script creates a comprehensive dataset that covers:
- 80% realistic company names
- 15% edge cases (unicode, special chars, etc.)
- 5% adversarial cases (should NOT group)

Date Contract:
- Normal rows (90%) are within the last 10 years from today/fixed_today
- Labeled date edge rows may be outside the window (including future) by design
- Out-of-window dates are tagged: date|edge|ancient (older than 10y) or date|edge|future (after today)

Schema includes all input fields the pipeline consumes directly.
"""

import csv
import random
import string
import hashlib
import base64
import yaml
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Dict, Any

# Set random seed for reproducibility
random.seed(42)

# Test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "tests" / "data"
OUTPUT_FILE = TEST_DATA_DIR / "companies_stress_10k.csv"
ADVERSARIAL_MANIFEST = TEST_DATA_DIR / "seeds" / "adversarial.yaml"
GROUPS_MANIFEST = TEST_DATA_DIR / "seeds" / "groups.yaml"

# Ensure test data directory exists
TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
ADVERSARIAL_MANIFEST.parent.mkdir(parents=True, exist_ok=True)

# Global fixed date for deterministic date generation
FIXED_TODAY = None

# Realistic company name patterns
REALISTIC_COMPANIES = [
    # Tech companies
    "Microsoft Corporation", "Apple Inc", "Google LLC", "Amazon.com Inc", "Meta Platforms Inc",
    "Tesla Inc", "Netflix Inc", "Uber Technologies Inc", "Airbnb Inc", "Spotify Technology",
    "Salesforce Inc", "Oracle Corporation", "IBM Corporation", "Intel Corporation", "Cisco Systems",
    
    # Financial services
    "JPMorgan Chase & Co", "Bank of America Corporation", "Wells Fargo & Company", "Goldman Sachs Group",
    "Morgan Stanley", "Citigroup Inc", "American Express Company", "Visa Inc", "Mastercard Inc",
    "PayPal Holdings Inc", "Square Inc", "Stripe Inc", "Robinhood Markets Inc",
    
    # Retail & Consumer
    "Walmart Inc", "Target Corporation", "Home Depot Inc", "Lowe's Companies Inc", "Costco Wholesale",
    "Best Buy Co Inc", "Nike Inc", "Adidas AG", "Coca-Cola Company", "PepsiCo Inc",
    "Procter & Gamble Co", "Johnson & Johnson", "Unilever PLC", "Nestle SA",
    
    # Healthcare & Pharma
    "Pfizer Inc", "Johnson & Johnson", "Merck & Co Inc", "AbbVie Inc", "Bristol Myers Squibb",
    "Gilead Sciences Inc", "Moderna Inc", "Regeneron Pharmaceuticals Inc",
    
    # Energy & Utilities
    "Exxon Mobil Corporation", "Chevron Corporation", "ConocoPhillips", "EOG Resources Inc",
    "NextEra Energy Inc", "Duke Energy Corporation", "Southern Company", "Dominion Energy Inc",
    
    # Manufacturing & Industrial
    "General Electric Company", "3M Company", "Caterpillar Inc", "Boeing Company", "Lockheed Martin",
    "Raytheon Technologies Corporation", "Honeywell International Inc", "United Technologies",
    
    # Media & Entertainment
    "Walt Disney Company", "Comcast Corporation", "ViacomCBS Inc", "Warner Bros Discovery",
    "Netflix Inc", "Spotify Technology SA", "Roku Inc", "Twilio Inc",
    
    # Transportation & Logistics
    "FedEx Corporation", "United Parcel Service Inc", "Delta Air Lines Inc", "American Airlines Group",
    "Southwest Airlines Co", "Union Pacific Corporation", "Norfolk Southern Corporation",
    
    # Real Estate & Construction
    "American Tower Corporation", "Crown Castle International Corp", "SBA Communications Corporation",
    "PulteGroup Inc", "D.R. Horton Inc", "Lennar Corporation", "KB Home",
    
    # Food & Beverage
    "McDonald's Corporation", "Starbucks Corporation", "Yum! Brands Inc", "Chipotle Mexican Grill",
    "Domino's Pizza Inc", "Papa John's International Inc", "Dunkin' Brands Group Inc",
    
    # Automotive
    "Ford Motor Company", "General Motors Company", "Tesla Inc", "Toyota Motor Corporation",
    "Honda Motor Co Ltd", "Nissan Motor Co Ltd", "BMW AG", "Mercedes-Benz Group AG",
    
    # Telecommunications
    "Verizon Communications Inc", "AT&T Inc", "T-Mobile US Inc", "Comcast Corporation",
    "Charter Communications Inc", "Dish Network Corporation", "Sprint Corporation",
    
    # Consulting & Professional Services
    "Accenture PLC", "Deloitte Touche Tohmatsu Limited", "PricewaterhouseCoopers LLP",
    "Ernst & Young Global Limited", "KPMG International Limited", "McKinsey & Company",
    "Boston Consulting Group Inc", "Bain & Company Inc",
    
    # Software & Cloud
    "Microsoft Corporation", "Amazon Web Services Inc", "Google Cloud Platform", "Oracle Corporation",
    "Salesforce Inc", "ServiceNow Inc", "Workday Inc", "Snowflake Inc", "Palantir Technologies Inc",
    "CrowdStrike Holdings Inc", "Okta Inc", "Zscaler Inc", "Datadog Inc", "Splunk Inc",
    
    # E-commerce & Marketplaces
    "Amazon.com Inc", "eBay Inc", "Etsy Inc", "Shopify Inc", "Square Inc", "PayPal Holdings Inc",
    "MercadoLibre Inc", "Alibaba Group Holding Limited", "JD.com Inc", "Pinduoduo Inc",
    
    # Gaming & Interactive Media
    "Activision Blizzard Inc", "Electronic Arts Inc", "Take-Two Interactive Software Inc",
    "Roblox Corporation", "Unity Software Inc", "Epic Games Inc", "Nintendo Co Ltd",
    "Sony Group Corporation", "Microsoft Corporation", "Google LLC",
    
    # Biotech & Life Sciences
    "Gilead Sciences Inc", "Biogen Inc", "Amgen Inc", "Vertex Pharmaceuticals Inc",
    "Illumina Inc", "Thermo Fisher Scientific Inc", "Danaher Corporation", "Abbott Laboratories",
    
    # Aerospace & Defense
    "Boeing Company", "Lockheed Martin Corporation", "Raytheon Technologies Corporation",
    "Northrop Grumman Corporation", "General Dynamics Corporation", "L3Harris Technologies Inc",
    
    # Chemicals & Materials
    "Dow Inc", "DuPont de Nemours Inc", "3M Company", "PPG Industries Inc", "Sherwin-Williams Company",
    "International Flavors & Fragrances Inc", "Ecolab Inc", "Air Products and Chemicals Inc",
    
    # Insurance & Reinsurance
    "Berkshire Hathaway Inc", "UnitedHealth Group Inc", "Anthem Inc", "Cigna Corporation",
    "Aetna Inc", "Humana Inc", "MetLife Inc", "Prudential Financial Inc", "Allstate Corporation",
    
    # Hospitality & Travel
    "Marriott International Inc", "Hilton Worldwide Holdings Inc", "Hyatt Hotels Corporation",
    "Expedia Group Inc", "Booking Holdings Inc", "TripAdvisor Inc", "Airbnb Inc",
    
    # Education & Training
    "Chegg Inc", "Coursera Inc", "Udemy Inc", "2U Inc", "Grand Canyon Education Inc",
    "Strayer Education Inc", "Capella Education Company", "Laureate Education Inc",
    
    # Agriculture & Food Production
    "Tyson Foods Inc", "JBS SA", "Cargill Inc", "Archer Daniels Midland Company",
    "Bunge Limited", "Conagra Brands Inc", "General Mills Inc", "Kellogg Company",
    
    # Mining & Metals
    "BHP Group Limited", "Rio Tinto Group", "Vale SA", "Freeport-McMoRan Inc",
    "Newmont Corporation", "Barrick Gold Corporation", "Southern Copper Corporation",
    
    # Waste Management & Environmental
    "Waste Management Inc", "Republic Services Inc", "Clean Harbors Inc", "Stericycle Inc",
    "Covanta Holding Corporation", "Casella Waste Systems Inc", "Advanced Disposal Services Inc",
]

# Edge case patterns
EDGE_CASES = [
    # Unicode variants
    "99¢ Store", "99 Cents Store", "AT&T Inc", "AT and T Inc", "M&M's Inc", "M and M's Inc",
    "Johnson & Johnson", "Johnson and Johnson", "H&M", "H and M", "Ben & Jerry's", "Ben and Jerry's",
    
    # Special characters
    "O'Reilly Auto Parts", "OReilly Auto Parts", "D'Angelo's", "DAngelos", "L'Occitane", "LOccitane",
    "McDonald's", "McDonalds", "O'Brien's", "OBriens", "D'Arcy's", "DArcys",
    
    # Acronyms and abbreviations
    "IBM", "International Business Machines", "GE", "General Electric", "HP", "Hewlett Packard",
    "GM", "General Motors", "AT&T", "American Telephone and Telegraph", "UPS", "United Parcel Service",
    "FBI", "Federal Bureau of Investigation", "CIA", "Central Intelligence Agency",
    "NASA", "National Aeronautics and Space Administration", "FDA", "Food and Drug Administration",
    
    # Numeric prefixes
    "3M", "3M Company", "7-Eleven", "7 Eleven", "24 Hour Fitness", "24-Hour Fitness",
    "1-800-Flowers", "1800 Flowers", "99 Cents Only", "99¢ Only", "360 Behavioral Health",
    
    # Punctuation variations
    "Co.", "Company", "Corp.", "Corporation", "Inc.", "Incorporated", "Ltd.", "Limited",
    "LLC", "Limited Liability Company", "LP", "Limited Partnership", "LLP", "Limited Liability Partnership",
    
    # Hyphenated names
    "Xerox Corporation", "Xerox Corp", "FedEx", "Federal Express", "Kinko's", "FedEx Kinko's",
    "Time-Warner", "Time Warner", "AOL-Time Warner", "AOL Time Warner",
    
    # Parenthetical information
    "Apple Inc (California)", "Apple Inc", "Microsoft Corporation (Redmond)", "Microsoft Corporation",
    "Google LLC (Mountain View)", "Google LLC", "Amazon.com Inc (Seattle)", "Amazon.com Inc",
    
    # Multiple names separated by semicolons
    "Acme Corporation; Acme Corp", "Beta Industries; Beta Inc", "Gamma LLC; Gamma Limited",
    "Delta Corp; Delta Corporation", "Epsilon Inc; Epsilon Incorporated",
    
    # Very long names
    "The Very Long Company Name That Tests String Processing and Similarity Algorithms Inc",
    "International Business Machines Corporation Global Technology Services Division",
    "United Parcel Service Inc Worldwide Express and Logistics Services",
    "General Electric Company Global Research and Development Center",
    "Johnson & Johnson Consumer Products Company Worldwide",
    
    # Names with excessive whitespace
    "  Apple Inc  ", "  Microsoft Corporation  ", "  Google LLC  ", "  Amazon.com Inc  ",
    "Apple   Inc", "Microsoft   Corporation", "Google   LLC", "Amazon   Inc",
    
    # Names with control characters (simulated)
    "Apple\tInc", "Microsoft\rCorporation", "Google\nLLC", "Amazon\fInc",
    
    # Names with line breaks (simulated)
    "Apple\nInc", "Microsoft\nCorporation", "Google\nLLC", "Amazon\nInc",
    
    # Empty and malformed names
    "", "   ", "N/A", "TBD", "Unknown", "Test", "Sample", "Example", "Dummy", "Temp",
    "Temporary", "Delete", "Remove", "Do not use", "Not sure", "Unsure", "No idea",
    
    # Names with only punctuation
    "!!!", "???", "---", "+++", "***", "###", "$$$", "%%%", "^^^", "&&&",
    
    # Names with only numbers
    "123", "456", "789", "000", "999", "111", "222", "333", "444", "555",
    
    # Names with mixed scripts (simulated)
    "Microsoft", "Microѕoft", "Apple", "Аррle", "Google", "Gооgle", "Amazon", "Аmazon",
    
    # NEW: More company name variations for deduplication testing
    "Amazon.com Inc", "Amazon Inc", "Amazon   Inc", "Amazon.com Corp", "Amazon Bank Inc", "Amazon Web Services Inc",
    "Google LLC", "Google   LLC", "Google\nLLC", "Gооgle", "Google Drive", "Google Cloud Platform",
    "Johnson & Johnson", "Johnson and Johnson", "Johnson & Johnson Ltd", "Johnson & Johnson Consumer Products Company Worldwide",
    "Microsoft Corporation", "Microsoft Corp", "Microsoft   Corporation", "Microѕoft Corporation", "Microsoft Office",
    "Apple Inc", "Apple Corporation", "Apple   Inc", "Apple Music", "Apple Bank Inc",
    "General Electric", "General Electric Company", "General Electric Company Global Research and Development Center",
    "GE", "GE Healthcare", "GE Aviation", "GE Power", "GE Renewable Energy",
    
    # NEW: Suffix edge cases - middle of name, abbreviated forms
    "Intl. Business Machines", "Intl Business Machines", "International Corp Ltd", "International Corp Limited",
    "Global Corp Inc", "Global Corporation Inc", "Worldwide Corp LLC", "Worldwide Corporation LLC",
    "Tech Corp Systems", "Technology Corporation Systems", "Data Corp Solutions", "Data Corporation Solutions",
    "Software Corp Services", "Software Corporation Services", "Cloud Corp Platforms", "Cloud Corporation Platforms",
    "AI Corp Technologies", "AI Corporation Technologies", "ML Corp Solutions", "ML Corporation Solutions",
    
    # NEW: Encoding edge cases
    "🚀 Rocket Corp", "🚀 Rocket Corporation", "💻 Tech Solutions", "💻 Technology Solutions",
    " Data Corp", " Data Corporation", " Cloud Services", " Cloud Services",
    "🎯 Target Corp", "🎯 Target Corporation", " Amazon Inc", " Amazon Inc",
    " Apple Inc", " Apple Inc", " Google LLC", " Google LLC",
    
    # NEW: Right-to-left script simulation (using mixed direction)
    "Microsoft Corporation", "Microsoft Corporation", "Apple Inc", "Apple Inc",
    "Google LLC", "Google LLC", "Amazon.com Inc", "Amazon.com Inc",
    
    # NEW: More Unicode variants
    "Café Corporation", "Cafe Corporation", "Naïve Inc", "Naive Inc", "Résumé Corp", "Resume Corp",
    "Piñata LLC", "Pinata LLC", "Señor Corp", "Senor Corp", "Niño Inc", "Nino Inc",
    
    # NEW: More special character variations
    "AT&T Inc", "AT and T Inc", "AT&T Corporation", "AT and T Corporation",
    "M&M's Inc", "M and M's Inc", "M&M's Corporation", "M and M's Corporation",
    "H&M", "H and M", "H&M Corporation", "H and M Corporation",
    "Ben & Jerry's", "Ben and Jerry's", "Ben & Jerry's Corporation", "Ben and Jerry's Corporation",
    
    # NEW: More punctuation edge cases
    "Co., Inc.", "Company, Incorporated", "Corp., LLC", "Corporation, Limited Liability Company",
    "Ltd., Corp.", "Limited, Corporation", "Inc., Co.", "Incorporated, Company",
    "LLC, LP", "Limited Liability Company, Limited Partnership", "LLP, Corp.", "Limited Liability Partnership, Corporation",
    
    # NEW: More control character variations
    "Apple\tInc\tCorp", "Microsoft\rCorporation\rLLC", "Google\nLLC\nInc", "Amazon\fInc\fCorp",
    "Tesla\tInc\tLLC", "Netflix\rInc\rCorp", "Spotify\nInc\nLLC", "Uber\fInc\fCorp",
    
    # NEW: More line break variations
    "Apple\nInc\nCorp", "Microsoft\nCorporation\nLLC", "Google\nLLC\nInc", "Amazon\nInc\nCorp",
    "Tesla\nInc\nLLC", "Netflix\nInc\nCorp", "Spotify\nInc\nLLC", "Uber\nInc\nCorp",
    
    # NEW: More empty and malformed variations
    "   ", "    ", "     ", "      ", "       ", "        ", "         ", "          ",
    "N/A", "n/a", "N/a", "n/A", "NA", "na", "Na", "nA",
    "TBD", "tbd", "Tbd", "TBD", "T.B.D.", "t.b.d.", "T.b.d.", "T.B.d.",
    "Unknown", "unknown", "UNKNOWN", "UnKnOwN", "UNKNOWN", "Unknown Company", "Unknown Corp", "Unknown Inc",
    "Test", "test", "TEST", "TeSt", "TEST", "Test Company", "Test Corp", "Test Inc",
    "Sample", "sample", "SAMPLE", "SaMpLe", "SAMPLE", "Sample Company", "Sample Corp", "Sample Inc",
    "Example", "example", "EXAMPLE", "ExAmPlE", "EXAMPLE", "Example Company", "Example Corp", "Example Inc",
    "Dummy", "dummy", "DUMMY", "DuMmY", "DUMMY", "Dummy Company", "Dummy Corp", "Dummy Inc",
    "Temp", "temp", "TEMP", "TeMp", "TEMP", "Temp Company", "Temp Corp", "Temp Inc",
    "Temporary", "temporary", "TEMPORARY", "TeMpOrArY", "TEMPORARY", "Temporary Company", "Temporary Corp", "Temporary Inc",
    "Delete", "delete", "DELETE", "DeLeTe", "DELETE", "Delete Company", "Delete Corp", "Delete Inc",
    "Remove", "remove", "REMOVE", "ReMoVe", "REMOVE", "Remove Company", "Remove Corp", "Remove Inc",
    "Do not use", "do not use", "DO NOT USE", "Do NoT UsE", "DO NOT USE", "Do not use Company", "Do not use Corp", "Do not use Inc",
    "Not sure", "not sure", "NOT SURE", "NoT sUrE", "NOT SURE", "Not sure Company", "Not sure Corp", "Not sure Inc",
    "Unsure", "unsure", "UNSURE", "UnSuRe", "UNSURE", "Unsure Company", "Unsure Corp", "Unsure Inc",
    "No idea", "no idea", "NO IDEA", "No IdEa", "NO IDEA", "No idea Company", "No idea Corp", "No idea Inc",
    
    # NEW: More punctuation-only variations
    "!!!", "???", "---", "+++", "***", "###", "$$$", "%%%", "^^^", "&&&",
    "!!!", "???", "---", "+++", "***", "###", "$$$", "%%%", "^^^", "&&&",
    "!!!", "???", "---", "+++", "***", "###", "$$$", "%%%", "^^^", "&&&",
    
    # NEW: More number-only variations
    "123", "456", "789", "000", "999", "111", "222", "333", "444", "555",
    "1234", "5678", "9012", "3456", "7890", "2468", "1357", "9753", "8642", "7410",
    "12345", "67890", "11111", "22222", "33333", "44444", "55555", "66666", "77777", "88888",
]

# Adversarial cases (should NOT group)
ADVERSARIAL_CASES = [
    # Bank distractors
    "Apple Inc", "Apple Bank Inc", "Microsoft Corporation", "Microsoft Bank Corporation",
    "Google LLC", "Google Bank LLC", "Amazon.com Inc", "Amazon Bank Inc",
    "Tesla Inc", "Tesla Bank Inc", "Netflix Inc", "Netflix Bank Inc",
    "Oracle Corporation", "Oracle Bank Corporation", "IBM Corporation", "IBM Bank Corporation",
    "Salesforce Inc", "Salesforce Bank Inc", "Adobe Inc", "Adobe Bank Inc",
    
    # Venue distractors
    "Oracle Corporation", "Oracle Park", "Target Corporation", "Target Field",
    "Citi Field", "Citigroup Inc", "Yankee Stadium", "Yankee Candle Company",
    "Fenway Park", "Fenway Health", "Wrigley Field", "Wrigley Company",
    "Madison Square Garden", "Madison Square Garden Company", "Radio City Music Hall", "Radio City Music Hall Company",
    "Carnegie Hall", "Carnegie Hall Corporation", "Lincoln Center", "Lincoln Center for the Performing Arts",
    
    # Brand extensions
    "Uber Technologies Inc", "Uber Eats", "Amazon.com Inc", "Amazon Prime",
    "Google LLC", "Google Drive", "Microsoft Corporation", "Microsoft Office",
    "Apple Inc", "Apple Music", "Netflix Inc", "Netflix Games",
    "Spotify Technology", "Spotify Premium", "Twitter Inc", "Twitter Blue",
    "Meta Platforms Inc", "Meta Quest", "Tesla Inc", "Tesla Energy",
    
    # Geographic distractors
    "Boston Consulting Group", "Boston University", "Harvard University", "Harvard Business School",
    "Stanford University", "Stanford Research Institute", "MIT", "MIT Technology Review",
    "Berkeley University", "Berkeley Systems", "Yale University", "Yale New Haven Hospital",
    "Columbia University", "Columbia Pictures", "Princeton University", "Princeton Review",
    "Cornell University", "Cornell Lab of Ornithology", "Dartmouth College", "Dartmouth Hitchcock Medical Center",
    
    # Industry distractors
    "General Electric", "General Motors", "General Dynamics", "General Mills",
    "United Airlines", "United Technologies", "United Parcel Service", "United Health Group",
    "American Airlines", "American Express", "American Tower", "American International Group",
    "Delta Air Lines", "Delta Dental", "Southwest Airlines", "Southwest Gas",
    "JetBlue Airways", "JetBlue Technology Ventures", "Alaska Airlines", "Alaska Air Group",
    
    # Size distractors
    "Big Lots", "Big Lots Inc", "Small Business Administration", "Small Business Development Center",
    "Large Corporation", "Large Scale Systems", "Medium Company", "Medium Scale Operations",
    "Mega Corp", "Mega Corporation", "Micro Systems", "Micro Systems Inc",
    "Mini Cooper", "Mini Corporation", "Max Corporation", "Max Systems Inc",
    
    # Time-based distractors
    "New York Times", "New York Life", "New York Stock Exchange", "New York University",
    "Old Navy", "Old Dominion", "Old Spice", "Old Republic",
    "Modern Times", "Modern Corporation", "Ancient Corporation", "Ancient Systems",
    "Future Corp", "Future Systems", "Past Corporation", "Past Systems",
    
    # Color distractors
    "Blue Cross Blue Shield", "Blue Apron", "Blue Origin", "Blue Shield of California",
    "Red Bull", "Red Hat", "Red Cross", "Red Lobster",
    "Green Mountain", "Green Corporation", "Yellow Corporation", "Yellow Systems",
    "Purple Corporation", "Purple Systems", "Orange Corporation", "Orange Systems",
    
    # Number distractors
    "First National Bank", "First Data", "First Solar", "First Energy",
    "Second City", "Second Life", "Second Harvest", "Second Wind",
    "Third Rock", "Third Corporation", "Fourth Corporation", "Fourth Systems",
    "Fifth Third", "Fifth Corporation", "Sixth Corporation", "Sixth Systems",
    
    # Direction distractors
    "Northrop Grumman", "Northwestern University", "Northwestern Mutual", "Northwestern Medicine",
    "Southwest Airlines", "Southwest Gas", "Southwest Airlines", "Southwest Research Institute",
    "Eastern Airlines", "Eastern Corporation", "Western Corporation", "Western Systems",
    "Central Corporation", "Central Systems", "Middle Corporation", "Middle Systems",
    
    # NEW: More complex adversarial cases
    "Apple Inc", "Apple Computer Inc", "Apple Bank Inc", "Apple Music Inc", "Apple TV Inc",
    "Microsoft Corporation", "Microsoft Office Corporation", "Microsoft Bank Corporation", "Microsoft Gaming Corporation",
    "Google LLC", "Google Search LLC", "Google Bank LLC", "Google Cloud LLC", "Google AI LLC",
    "Amazon.com Inc", "Amazon Prime Inc", "Amazon Bank Inc", "Amazon Web Services Inc", "Amazon Music Inc",
    "Tesla Inc", "Tesla Motors Inc", "Tesla Bank Inc", "Tesla Energy Inc", "Tesla Solar Inc",
    "Netflix Inc", "Netflix Streaming Inc", "Netflix Bank Inc", "Netflix Games Inc", "Netflix Studios Inc",
    
    # NEW: More venue and location distractors
    "Oracle Corporation", "Oracle Park", "Oracle Arena", "Oracle Stadium", "Oracle Center",
    "Target Corporation", "Target Field", "Target Center", "Target Stadium", "Target Arena",
    "Citi Field", "Citigroup Inc", "Citi Center", "Citi Stadium", "Citi Arena",
    "Yankee Stadium", "Yankee Candle Company", "Yankee Center", "Yankee Arena", "Yankee Stadium Corporation",
    "Fenway Park", "Fenway Health", "Fenway Center", "Fenway Arena", "Fenway Stadium",
    "Wrigley Field", "Wrigley Company", "Wrigley Center", "Wrigley Arena", "Wrigley Stadium",
    
    # NEW: More brand extension distractors
    "Uber Technologies Inc", "Uber Eats", "Uber Freight", "Uber Health", "Uber Works",
    "Amazon.com Inc", "Amazon Prime", "Amazon Fresh", "Amazon Go", "Amazon Studios",
    "Google LLC", "Google Drive", "Google Maps", "Google Photos", "Google Play",
    "Microsoft Corporation", "Microsoft Office", "Microsoft Teams", "Microsoft Azure", "Microsoft Dynamics",
    "Apple Inc", "Apple Music", "Apple TV", "Apple Pay", "Apple Watch",
    "Netflix Inc", "Netflix Games", "Netflix Studios", "Netflix Animation", "Netflix Interactive",
    
    # NEW: More geographic distractors
    "Boston Consulting Group", "Boston University", "Boston College", "Boston Medical Center", "Boston Children's Hospital",
    "Harvard University", "Harvard Business School", "Harvard Medical School", "Harvard Law School", "Harvard Extension School",
    "Stanford University", "Stanford Research Institute", "Stanford Medical Center", "Stanford Children's Hospital", "Stanford Health Care",
    "MIT", "MIT Technology Review", "MIT Media Lab", "MIT Sloan School", "MIT Computer Science",
    "Berkeley University", "Berkeley Systems", "Berkeley Lab", "Berkeley Art Museum", "Berkeley Repertory Theatre",
    "Yale University", "Yale New Haven Hospital", "Yale School of Medicine", "Yale Law School", "Yale School of Management",
    
    # NEW: More industry distractors
    "General Electric", "General Motors", "General Dynamics", "General Mills", "General Motors Financial",
    "United Airlines", "United Technologies", "United Parcel Service", "United Health Group", "United Rentals",
    "American Airlines", "American Express", "American Tower", "American International Group", "American Electric Power",
    "Delta Air Lines", "Delta Dental", "Delta Airlines", "Delta Systems", "Delta Corporation",
    "Southwest Airlines", "Southwest Gas", "Southwest Airlines", "Southwest Research Institute", "Southwest Airlines Group",
    "JetBlue Airways", "JetBlue Technology Ventures", "JetBlue Systems", "JetBlue Corporation", "JetBlue Holdings",
    "Alaska Airlines", "Alaska Air Group", "Alaska Systems", "Alaska Corporation", "Alaska Holdings",
]

# Suffix variations
SUFFIXES = ["Inc", "LLC", "Ltd", "Corp", "Corporation", "Company", "Co", "LP", "LLP", "Limited"]

# Relationship types
RELATIONSHIPS = ["Parent", "Child", "Sibling", "Subsidiary", "Division", "Branch", "Franchise", "Partner"]

# Disposition types
DISPOSITIONS = ["Update", "Merge", "Drop", "Keep"]

def generate_salesforce_id() -> str:
    """Generate a valid 15-character Salesforce ID."""
    # Salesforce IDs start with '001' for Account objects
    prefix = "001"
    # Generate 12 random alphanumeric characters
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
    return prefix + suffix

def generate_leap_year_edge_cases():
    """Generate comprehensive leap year edge cases programmatically."""
    valid_leaps = []
    invalid_leaps = []
    
    # Generate leap years from 1900-2100
    for year in range(1900, 2101):
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            # Valid leap year
            valid_leaps.append(f"{year}-02-29")
        elif year % 4 == 0 and year % 100 == 0 and year % 400 != 0:
            # Invalid leap year (divisible by 100 but not 400)
            invalid_leaps.append(f"{year}-02-29")
    
    return valid_leaps, invalid_leaps


def generate_created_date() -> str:
    """Generate a random ISO date within the last 10 years, including edge cases."""
    # Use fixed date if provided, otherwise use current date
    today = FIXED_TODAY or datetime.now()
    
    # 90% normal dates, 10% edge cases
    if random.random() < 0.9:
        # Normal date range
        start_date = today - timedelta(days=3650)  # 10 years ago
        end_date = today
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        return random_date.strftime("%Y-%m-%d")
    else:
        # Edge cases - use programmatic leap year generator
        valid_leaps, invalid_leaps = generate_leap_year_edge_cases()
        # Mix valid and invalid leap days for comprehensive testing
        all_edge_dates = valid_leaps + invalid_leaps
        return random.choice(all_edge_dates)

def generate_parent_account_id() -> str:
    """Generate a parent account ID (50% chance of having one)."""
    if random.random() < 0.5:
        return generate_salesforce_id()
    return ""


def deterministic_sfdc_id(name: str, salt: str = "") -> str:
    """
    Stable 15-char SFDC-like ID based on name + salt.
    Prefix '001' for Account. Base62-encode a hash to fill the remainder.
    """
    b = hashlib.sha256((name + "|" + salt).encode("utf-8")).digest()
    # base64 urlsafe, then filter to [A-Za-z0-9], then take 12 chars
    enc = base64.urlsafe_b64encode(b).decode("ascii")
    base62 = "".join([c for c in enc if c.isalnum()])[:12]
    return "001" + base62  # total length 15


def load_seed_pairs() -> Dict[str, Any]:
    """Load seed pairs from adversarial YAML manifest."""
    if ADVERSARIAL_MANIFEST.exists():
        with open(ADVERSARIAL_MANIFEST, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {"pairs": []}


def load_seed_clusters() -> Dict[str, Any]:
    """Load seed clusters from groups YAML manifest."""
    if GROUPS_MANIFEST.exists():
        with open(GROUPS_MANIFEST, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {"clusters": []}


def _append_tags(existing: str, *new_tokens: str) -> str:
    """Append new tags to existing tags, deduplicating while preserving order."""
    parts = [] if not existing else [t for t in existing.split("|") if t]
    for t in new_tokens:
        if t:
            parts.append(t)
    # de-dupe while preserving order
    seen = set()
    dedup = [t for t in parts if not (t in seen or seen.add(t))]
    return "|".join(dedup)


def _tag_out_of_window(created_date: str, scenario_tags: str) -> str:
    """Tag dates that fall outside the 10-year window or after today/fixed_today."""
    try:
        anchor = (FIXED_TODAY or datetime.now()).date()
        cd = datetime.strptime(created_date, "%Y-%m-%d").date()
        # ancient: strictly older than 10 years
        if (anchor - cd).days > 3650:
            scenario_tags = _append_tags(scenario_tags, "date", "edge", "ancient")
        # future: strictly after anchor
        if cd > anchor:
            scenario_tags = _append_tags(scenario_tags, "date", "edge", "future")
    except ValueError:
        # Invalid date - tag as invalid edge case
        scenario_tags = _append_tags(scenario_tags, "date", "edge", "invalid")
    return scenario_tags


def mark_duplicate_tags(scenario_tags: str, notes: str) -> tuple[str, str]:
    """Unified duplicate tagging logic for consistent duplicate ID handling."""
    if scenario_tags:
        scenario_tags = (scenario_tags + "|id|duplicate").strip("|")
    else:
        scenario_tags = "id|duplicate"
    
    if notes:
        notes = notes + "; intentional duplicate id for dedup"
    else:
        notes = "intentional duplicate id for dedup"
    
    return scenario_tags, notes


def add_seed_row(rows: List[Tuple[str, ...]], account_id: str, account_name: str, 
                 created_date: str, suffix_class: str, disposition: str, 
                 parent_account_id: str, relationship: str, scenario_tags: str, 
                 ground_truth_entity_id: str, notes: str, cluster_id: str = "") -> None:
    """Add a seed row with consistent formatting and duplicate ID tagging."""
    # Check for duplicate IDs and tag them
    is_duplicate = False
    for existing_row in rows:
        if existing_row[0] == account_id:
            is_duplicate = True
            break
    
    if is_duplicate:
        scenario_tags, notes = mark_duplicate_tags(scenario_tags, notes)
    
    rows.append((
        account_id,
        account_name,
        created_date,
        suffix_class,
        disposition,
        parent_account_id,
        relationship,
        scenario_tags,
        ground_truth_entity_id,
        notes,
        cluster_id
    ))

def generate_company_name(category: str) -> str:
    """Generate a company name based on category."""
    if category == "realistic":
        base_name = random.choice(REALISTIC_COMPANIES)
        # 30% chance of adding suffix variation
        if random.random() < 0.3:
            suffix = random.choice(SUFFIXES)
            if not base_name.endswith(suffix):
                base_name = base_name.replace(" Inc", "").replace(" LLC", "").replace(" Corp", "").replace(" Corporation", "").replace(" Company", "").replace(" Ltd", "").replace(" Limited", "")
                base_name += f" {suffix}"
        return base_name
    
    elif category == "edge":
        return random.choice(EDGE_CASES)
    
    elif category == "adversarial":
        return random.choice(ADVERSARIAL_CASES)
    
    return "Unknown Company"

def generate_stress_dataset(num_rows: int = 10000) -> List[Tuple[str, ...]]:
    """Generate the stress test dataset."""
    rows = []
    
    # Distribution: 80% realistic, 15% edge, 5% adversarial
    realistic_count = int(num_rows * 0.8)
    edge_count = int(num_rows * 0.15)
    adversarial_count = num_rows - realistic_count - edge_count
    
    print(f"Generating {num_rows} rows:")
    print(f"  - {realistic_count} realistic company names (80%)")
    print(f"  - {edge_count} edge cases (15%)")
    print(f"  - {adversarial_count} adversarial cases (5%)")
    
    # Track IDs for data integrity testing
    used_ids = set()
    duplicate_ids = set()
    
    # Generate realistic companies
    for i in range(realistic_count):
        account_id = generate_salesforce_id()
        
        # 1% chance of duplicate ID for data integrity testing
        is_duplicate = False
        if random.random() < 0.01 and len(used_ids) > 0:
            account_id = random.choice(list(used_ids))
            duplicate_ids.add(account_id)
            is_duplicate = True
        
        used_ids.add(account_id)
        
        account_name = generate_company_name("realistic")
        created_date = generate_created_date()
        suffix_class = random.choice(SUFFIXES) if random.random() < 0.7 else ""
        disposition = random.choice(DISPOSITIONS) if random.random() < 0.3 else ""
        parent_account_id = generate_parent_account_id()
        relationship = random.choice(RELATIONSHIPS) if parent_account_id else ""
        
        # Tag duplicates using unified helper
        if is_duplicate:
            scenario_tags, notes = mark_duplicate_tags("", "")
        else:
            scenario_tags, notes = "", ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, scenario_tags)
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,
            "",  # ground_truth_entity_id
            notes,
            ""   # cluster_id
        ))
    
    # Generate edge cases
    for i in range(edge_count):
        account_id = generate_salesforce_id()
        
        # 2% chance of duplicate ID for edge cases
        is_duplicate = False
        if random.random() < 0.02 and len(used_ids) > 0:
            account_id = random.choice(list(used_ids))
            duplicate_ids.add(account_id)
            is_duplicate = True
        
        used_ids.add(account_id)
        
        account_name = generate_company_name("edge")
        created_date = generate_created_date()
        suffix_class = random.choice(SUFFIXES) if random.random() < 0.5 else ""
        disposition = random.choice(DISPOSITIONS) if random.random() < 0.2 else ""
        parent_account_id = generate_parent_account_id()
        relationship = random.choice(RELATIONSHIPS) if parent_account_id else ""
        
        # Tag duplicates using unified helper
        if is_duplicate:
            scenario_tags, notes = mark_duplicate_tags("", "")
        else:
            scenario_tags, notes = "", ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, scenario_tags)
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,
            "",  # ground_truth_entity_id
            notes,
            ""   # cluster_id
        ))
    
    # Generate adversarial cases
    for i in range(adversarial_count):
        account_id = generate_salesforce_id()
        
        # 3% chance of duplicate ID for adversarial cases
        is_duplicate = False
        if random.random() < 0.03 and len(used_ids) > 0:
            account_id = random.choice(list(used_ids))
            duplicate_ids.add(account_id)
            is_duplicate = True
        
        used_ids.add(account_id)
        
        account_name = generate_company_name("adversarial")
        created_date = generate_created_date()
        suffix_class = random.choice(SUFFIXES) if random.random() < 0.6 else ""
        disposition = random.choice(DISPOSITIONS) if random.random() < 0.4 else ""
        parent_account_id = generate_parent_account_id()
        relationship = random.choice(RELATIONSHIPS) if parent_account_id else ""
        
        # Tag duplicates using unified helper
        if is_duplicate:
            scenario_tags, notes = mark_duplicate_tags("", "")
        else:
            scenario_tags, notes = "", ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, scenario_tags)
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,
            "",  # ground_truth_entity_id
            notes,
            ""   # cluster_id
        ))
    
    # Add some data integrity edge cases
    print(f"Adding data integrity edge cases...")
    
    # Add some rows with null/empty account_id
    for i in range(10):
        account_name = generate_company_name("edge")
        created_date = generate_created_date()
        suffix_class = random.choice(SUFFIXES) if random.random() < 0.5 else ""
        disposition = random.choice(DISPOSITIONS) if random.random() < 0.2 else ""
        parent_account_id = generate_parent_account_id()
        relationship = random.choice(RELATIONSHIPS) if parent_account_id else ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, "")
        
        rows.append((
            "",  # Empty account_id
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,  # scenario_tags
            "",  # ground_truth_entity_id
            "",  # notes
            ""   # cluster_id
        ))
    
    # Add some rows with same company name but different dates (testing dedup across time)
    base_companies = ["Apple Inc", "Microsoft Corporation", "Google LLC", "Amazon.com Inc", "Tesla Inc"]
    for company in base_companies:
        for i in range(3):  # 3 variations per company
            account_id = generate_salesforce_id()
            used_ids.add(account_id)
            
            # Same company name, different dates
            account_name = company
            created_date = generate_created_date()
            suffix_class = random.choice(SUFFIXES) if random.random() < 0.7 else ""
            disposition = random.choice(DISPOSITIONS) if random.random() < 0.3 else ""
            parent_account_id = generate_parent_account_id()
            relationship = random.choice(RELATIONSHIPS) if parent_account_id else ""
            
            # Tag out-of-window dates
            scenario_tags = _tag_out_of_window(created_date, "")
            
            rows.append((
                account_id,
                account_name,
                created_date,
                suffix_class,
                disposition,
                parent_account_id,
                relationship,
                scenario_tags,  # scenario_tags
                "",  # ground_truth_entity_id
                "",  # notes
                ""   # cluster_id
            ))
    
    # Add some relationship cycles (A -> B -> C -> A)
    cycle_companies = ["Cycle Corp A", "Cycle Corp B", "Cycle Corp C"]
    cycle_ids = [generate_salesforce_id() for _ in range(3)]
    
    for i, (company, account_id) in enumerate(zip(cycle_companies, cycle_ids)):
        used_ids.add(account_id)
        
        account_name = company
        created_date = generate_created_date()
        suffix_class = random.choice(SUFFIXES) if random.random() < 0.7 else ""
        disposition = random.choice(DISPOSITIONS) if random.random() < 0.3 else ""
        
        # Create cycle: A -> B -> C -> A
        if i == 0:  # A
            parent_account_id = cycle_ids[2]  # A's parent is C
            relationship = "Child"
        elif i == 1:  # B
            parent_account_id = cycle_ids[0]  # B's parent is A
            relationship = "Child"
        else:  # C
            parent_account_id = cycle_ids[1]  # C's parent is B
            relationship = "Child"
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, "")
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,  # scenario_tags
            "",  # ground_truth_entity_id
            "",  # notes
            ""   # cluster_id
        ))
    
    print(f"Data integrity edge cases added:")
    print(f"  - {len(duplicate_ids)} duplicate account_ids")
    print(f"  - 10 empty account_ids")
    print(f"  - 15 same company names with different dates")
    print(f"  - 3 relationship cycles")
    
    # Add date edge cases (extreme dates for parser testing)
    print(f"Adding date edge cases...")
    valid_leaps, invalid_leaps = generate_leap_year_edge_cases()
    
    # Add valid leap days (extreme but valid)
    for i, valid_date in enumerate(valid_leaps[:10]):  # Limit to 10 for dataset size
        account_id = generate_salesforce_id()
        used_ids.add(account_id)
        
        account_name = f"Valid Leap Company {i+1}"
        created_date = valid_date
        suffix_class = "Inc"
        disposition = ""
        parent_account_id = ""
        relationship = ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, "date|edge|extreme")
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,  # scenario_tags
            "",  # ground_truth_entity_id
            "valid but extreme leap day for parser checks",  # notes
            ""   # cluster_id
        ))
    
    # Add invalid leap days (truly invalid dates)
    for i, invalid_date in enumerate(invalid_leaps[:5]):  # Limit to 5 for dataset size
        account_id = generate_salesforce_id()
        used_ids.add(account_id)
        
        account_name = f"Invalid Leap Company {i+1}"
        created_date = invalid_date
        suffix_class = "Inc"
        disposition = ""
        parent_account_id = ""
        relationship = ""
        
        # Tag out-of-window dates
        scenario_tags = _tag_out_of_window(created_date, "date|edge|invalid")
        
        rows.append((
            account_id,
            account_name,
            created_date,
            suffix_class,
            disposition,
            parent_account_id,
            relationship,
            scenario_tags,  # scenario_tags
            "",  # ground_truth_entity_id
            "invalid leap day for parser error handling",  # notes
            ""   # cluster_id
        ))
    
    # Add seed block from YAML manifests
    print(f"Adding seed block from manifests...")
    seed_data = load_seed_pairs()
    cluster_data = load_seed_clusters()
    seed_count = 0
    seed_ids = set()
    
    # Process adversarial pairs
    for pair in seed_data.get("pairs", []):
        pair_id = pair["id"]
        expectation = pair["expectation"]
        tags = pair["tags"]
        notes = pair["notes"]
        
        # Process left side
        left_gt = pair["left"]["gt_entity"]
        for variant in pair["left"]["variants"]:
            account_id = deterministic_sfdc_id(variant, salt=pair_id)
            
            # Check for seed ID collision
            if account_id in seed_ids:
                raise RuntimeError(f"Seed ID collision: {variant} in {pair_id}")
            seed_ids.add(account_id)
            
            account_name = variant
            created_date = "2023-01-15"  # Recent valid date
            suffix_class = ""
            disposition = ""
            parent_account_id = ""
            relationship = ""
            
            add_seed_row(rows, account_id, account_name, created_date, suffix_class, 
                        disposition, parent_account_id, relationship, "|".join(tags), 
                        left_gt, notes)
            seed_count += 1
        
        # Process right side
        right_gt = pair["right"]["gt_entity"]
        for variant in pair["right"]["variants"]:
            account_id = deterministic_sfdc_id(variant, salt=pair_id)
            
            # Check for seed ID collision
            if account_id in seed_ids:
                raise RuntimeError(f"Seed ID collision: {variant} in {pair_id}")
            seed_ids.add(account_id)
            
            account_name = variant
            created_date = "2023-01-15"  # Recent valid date
            suffix_class = ""
            disposition = ""
            parent_account_id = ""
            relationship = ""
            
            add_seed_row(rows, account_id, account_name, created_date, suffix_class, 
                        disposition, parent_account_id, relationship, "|".join(tags), 
                        right_gt, notes)
            seed_count += 1
    
    # Process cluster groups
    cluster_count = 0
    for cluster in cluster_data.get("clusters", []):
        cluster_id = cluster["id"]
        cluster_notes = cluster["notes"]
        
        for item in cluster["items"]:
            name = item["name"]
            gt_entity = item["gt_entity"]
            tags = item["tags"]
            
            account_id = deterministic_sfdc_id(name, salt=cluster_id)
            
            # Check for seed ID collision
            if account_id in seed_ids:
                raise RuntimeError(f"Seed ID collision: {name} in {cluster_id}")
            seed_ids.add(account_id)
            
            account_name = name
            created_date = "2023-01-15"  # Recent valid date
            suffix_class = ""
            disposition = ""
            parent_account_id = ""
            relationship = ""
            
            # Add cluster_id to notes for tracing
            item_notes = f"{cluster_notes} [cluster: {cluster_id}]"
            
            add_seed_row(rows, account_id, account_name, created_date, suffix_class, 
                        disposition, parent_account_id, relationship, "|".join(tags), 
                        gt_entity, item_notes, cluster_id)
            cluster_count += 1
    
    print(f"Seed block added: {seed_count} pair rows from {len(seed_data.get('pairs', []))} pairs")
    print(f"Cluster block added: {cluster_count} cluster rows from {len(cluster_data.get('clusters', []))} clusters")
    print(f"Total seed rows: {seed_count + cluster_count}")
    print(f"Seed ID uniqueness: ✓ (no collisions detected)")
    
    # Shuffle the rows to mix categories
    random.shuffle(rows)
    
    return rows

def main():
    """Generate the stress test dataset."""
    global FIXED_TODAY
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate Company Junction stress test dataset")
    parser.add_argument("--fixed-today", default=None, 
                       help="YYYY-MM-DD to anchor relative dates (e.g., 2024-01-01)")
    args = parser.parse_args()
    
    # Set fixed date if provided
    if args.fixed_today:
        try:
            FIXED_TODAY = datetime.strptime(args.fixed_today, "%Y-%m-%d")
            print(f"🚀 Generating Company Junction Stress Test Dataset (fixed date: {args.fixed_today})")
        except ValueError:
            print(f"❌ Invalid date format: {args.fixed_today}. Use YYYY-MM-DD format.")
            return 1
    else:
        print("🚀 Generating Company Junction Stress Test Dataset")
    
    print("=" * 60)
    
    # Generate dataset
    rows = generate_stress_dataset(10000)
    
    # Write to CSV
    print(f"\n📝 Writing dataset to {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'account_id',
            'account_name', 
            'created_date',
            'suffix_class',
            'disposition',
            'parent_account_id',
            'relationship',
            'scenario_tags',
            'ground_truth_entity_id',
            'notes',
            'cluster_id'
        ])
        
        # Write data rows
        writer.writerows(rows)
    
    print(f"✅ Dataset generated successfully!")
    print(f"   - File: {OUTPUT_FILE}")
    print(f"   - Rows: {len(rows):,}")
    print(f"   - Columns: 11 (7 original + 4 new)")
    print(f"   - Size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Show sample of each category
    print(f"\n📊 Sample data:")
    print("Realistic companies:")
    for i in range(5):
        print(f"  - {rows[i][1]}")
    
    print("\nEdge cases:")
    for i in range(5, 10):
        print(f"  - {rows[i][1]}")
    
    print("\nAdversarial cases:")
    for i in range(10, 15):
        print(f"  - {rows[i][1]}")
    
    return 0

if __name__ == "__main__":
    exit(main())
