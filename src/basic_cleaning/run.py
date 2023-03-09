#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def remove_outliers(df: pd.DataFrame, min: float, max: float, column: str):
    """
    Remove outliers from a dataframe
    
    Args:
        df: The dataframe to remove outliers from
        min: The minimum value to keep
        max: The maximum value to keep
        column: The column to remove outliers from

    Returns:
        A new dataframe with the outliers removed
    """
    idx = df[column].between(min, max)
    return df[idx].copy()

def go(args):
    """
    Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact

    Args:
        args: The command line arguments

    Returns:
        None
    """

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load the artifact as a pandas dataframe
    logger.info("Loading input artifact")
    df = pd.read_csv(artifact_local_path)

    # Apply some basic data cleaning
    logger.info("Applying basic cleaning")
    df = remove_outliers(df, args.min_price, args.max_price, "price")

    # Convert last_review column to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Remove rows with logitude and latitude out of a certain range
    df = remove_outliers(df, -74.25, -73.50, "longitude")
    df = remove_outliers(df, 40.5, 41.2, "latitude")

    # save to a csv file
    logger.info("Saving cleaned artifact")
    df.to_csv("clean_sample.csv", index=False)

    # Save the cleaned dataframe to a new artifact
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    logger.info("Done!")

    os.remove("clean_sample.csv")

    run.finish()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The name of the artifact to download",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The name of the artifact to save",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type of the artifact to save",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="The description of the artifact to save",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price to keep",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The maximum price to keep",
        required=True
    )


    args = parser.parse_args()

    go(args)
