"""
Data cleaning module for Salesforce export processing.

This module handles:
- CSV/Excel file loading
- Duplicate detection based on name matching
- Field merging logic
- Data validation and cleaning
- CLI orchestration for end-to-end pipeline
"""

import pandas as pd
import argparse
import sys
import logging
from pathlib import Path
from typing import List, Dict, Tuple
import os

# Import local modules
from normalize import normalize_dataframe, excel_serial_to_datetime
from similarity import pair_scores, save_candidate_pairs
from grouping import build_groups, compute_score_to_primary, save_groups
from survivorship import select_primary_records, generate_merge_preview, save_survivorship_results
from disposition import apply_dispositions, save_dispositions
from alias_matching import compute_alias_matches, create_alias_cross_refs, save_alias_matches
from utils import load_settings, load_relationship_ranks, setup_logging, ensure_directory_exists

logger = logging.getLogger(__name__)


def load_salesforce_data(file_path: str) -> pd.DataFrame:
    """
    Load Salesforce export data from CSV or Excel file.
    
    Args:
        file_path: Path to the Salesforce export file
        
    Returns:
        DataFrame containing the Salesforce data
    """
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")


def validate_required_columns(df: pd.DataFrame) -> bool:
    """
    Validate that required columns are present.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if validation passes
        
    Raises:
        ValueError: If required columns are missing
    """
    required_columns = ['Account ID', 'Account Name', 'Relationship', 'Created Date']
    
    # Check for Account Name or fallback to Employer Name
    name_columns = ['Account Name', 'Employer Name']
    has_name_column = any(col in df.columns for col in name_columns)
    
    if not has_name_column:
        raise ValueError(f"Missing required name column. Need one of: {name_columns}")
    
    missing_columns = []
    for col in required_columns:
        if col not in df.columns:
            missing_columns.append(col)
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return True


def run_pipeline(input_path: str, output_dir: str, config_path: str) -> None:
    """
    Run the complete deduplication pipeline.
    
    Args:
        input_path: Path to input CSV file
        output_dir: Directory for output files
        config_path: Path to configuration file
    """
    logger.info("Starting Company Junction deduplication pipeline")
    
    # Load configuration
    settings = load_settings(config_path)
    relationship_ranks = load_relationship_ranks('config/relationship_ranks.csv')
    
    # Setup logging
    setup_logging(settings.get('logging', {}).get('level', 'INFO'))
    
    # Ensure output directories exist
    ensure_directory_exists(output_dir)
    ensure_directory_exists('data/interim')
    
    try:
        # Step 1: Load and validate data
        logger.info(f"Loading data from {input_path}")
        df = load_salesforce_data(input_path)
        
        # Validate required columns
        validate_required_columns(df)
        
        # Handle Excel serial dates
        if 'Created Date' in df.columns:
            df['Created Date'] = df['Created Date'].apply(excel_serial_to_datetime)
        
        logger.info(f"Loaded {len(df)} records")
        
        # Step 2: Normalize data
        logger.info("Normalizing company names")
        name_column = settings.get('data', {}).get('name_column', 'Account Name')
        df_norm = normalize_dataframe(df, name_column)
        
        # Save normalized data
        interim_format = settings.get('io', {}).get('interim_format', 'parquet')
        normalized_path = f"data/interim/accounts_normalized.{interim_format}"
        if interim_format == 'parquet':
            df_norm.to_parquet(normalized_path, index=False)
        else:
            df_norm.to_csv(normalized_path, index=False)
        logger.info(f"Saved normalized data to {normalized_path}")
        
        # Step 3: Generate candidate pairs
        logger.info("Generating candidate pairs")
        pairs_df = pair_scores(df_norm, settings)
        
        # Save candidate pairs
        pairs_path = f"data/interim/candidate_pairs.{interim_format}"
        save_candidate_pairs(pairs_df, pairs_path)
        
        # Step 4: Build groups
        logger.info("Building duplicate groups")
        df_groups = build_groups(df_norm, pairs_df, settings)
        
        # Compute scores to primary
        df_groups = compute_score_to_primary(df_groups, pairs_df)
        
        # Save groups
        groups_path = f"data/interim/groups.{interim_format}"
        save_groups(df_groups, groups_path)
        
        # Step 5: Select primary records
        logger.info("Selecting primary records")
        df_primary = select_primary_records(df_groups, relationship_ranks, settings)
        
        # Generate merge preview
        df_primary = generate_merge_preview(df_primary)
        
        # Save survivorship results
        survivorship_path = f"data/interim/survivorship.{interim_format}"
        save_survivorship_results(df_primary, survivorship_path)
        
        # Step 6: Apply dispositions
        logger.info("Applying disposition classification")
        df_dispositions = apply_dispositions(df_primary, settings)
        
        # Save dispositions
        dispositions_path = f"data/interim/dispositions.{interim_format}"
        save_dispositions(df_dispositions, dispositions_path)
        
        # Step 7: Compute alias matches and cross-references
        logger.info("Computing alias matches and cross-references")
        alias_matches_path = f"data/interim/alias_matches.{interim_format}"
        result = compute_alias_matches(df_norm, df_groups, settings)
        
        if isinstance(result, tuple) and len(result) == 2:
            df_alias_matches, alias_stats = result
        else:
            df_alias_matches, alias_stats = result, {}
        
        save_alias_matches(df_alias_matches, alias_matches_path)
        
        # Add alias cross-references to dispositions
        df_dispositions = create_alias_cross_refs(df_dispositions, df_alias_matches)
        
        # Step 8: Create final review-ready output
        logger.info("Creating review-ready output")
        review_path = os.path.join(output_dir, 'review_ready.csv')
        df_dispositions.to_csv(review_path, index=False)
        
        # Also write Parquet version for UI (native types for alias fields)
        try:
            parquet_path = os.path.join(output_dir, 'review_ready.parquet')
            df_dispositions.to_parquet(parquet_path, index=False)
            logger.info(f"Also wrote Parquet review file: {parquet_path}")
        except Exception as e:
            logger.warning(f"Parquet write failed: {e}")
        
        logger.info(f"Pipeline completed successfully. Review file: {review_path}")
        
        # Log alias performance stats
        if alias_stats:
            logger.info(f"Alias pairs generated: {alias_stats.get('pairs_generated', 0)} (capped blocks: {alias_stats.get('capped_blocks', 0)})")
            logger.info(f"Alias matches accepted (score ≥ high & suffix match): {alias_stats.get('accepted_matches', 0)}")
            logger.info(f"Alias matching completed in {alias_stats.get('elapsed_time', 0):.2f}s")
        
        # Print summary
        disposition_counts = df_dispositions['Disposition'].value_counts()
        logger.info(f"Disposition summary: {disposition_counts.to_dict()}")
        
        group_count = len(df_dispositions['group_id'].unique())
        logger.info(f"Total groups: {group_count}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Company Junction Deduplication Pipeline')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--outdir', required=True, help='Output directory path')
    parser.add_argument('--config', default='config/settings.yaml', help='Configuration file path')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)
    
    # Run pipeline
    run_pipeline(args.input, args.outdir, args.config)


if __name__ == "__main__":
    main()
