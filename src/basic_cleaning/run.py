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


def go(args):

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
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last_review column to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Remove rows with logitude and latitude out of a certain range
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

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
