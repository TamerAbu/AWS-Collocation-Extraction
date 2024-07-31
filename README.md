# AWS Collocation Extraction
## Overview
This project involves implementing an automated collocation extraction system using Amazon Elastic MapReduce (EMR) and Apache Hadoop on the Google 2-grams dataset. Collocations are identified based on the Normalized Pointwise Mutual Information (NPMI) value calculated for pairs of ordered words.

## System Architecture
The system comprises three main components:

-Local Application: Initiates the map-reduce job on Amazon EMR and manages interactions with AWS services.

-Amazon Elastic MapReduce (EMR): Distributes the collocation extraction task across multiple nodes and processes the Google 2-grams dataset.

-Amazon S3: Stores input datasets, intermediate data, and output results.

## Program Description:
The program is divided into 5 steps, each performing a specific task in the process of analyzing word pair trends over decades.
*over steps 2-5 we used partitioner to divide our keys by dacade.

### Step 1: Data Preparation
Filters input data to include only letters (a-z).
Organizes word pairs by decades.
Counts occurrences of each word pair per decade.
Counts total pairs of words (N) per decade.
Output Keys-Values:
decade * N TAB count
decade word1 word2 TAB count
The reducer combines all keys and their counts into a single entry per key.


### Step 2: Calculation of Part 1 of the Formula
Calculates log(N) - log(c(w1)) for each word pair.
Output Keys-Values:
decade * N TAB count
decade word1 * TAB count
decade word1 word2 TAB count
After reduction: decade word1 word2 TAB count N log(N) - log(c(w1))


### Step 3: Final NPMI Calculation
Calculates log(c(w1,w2)) - log(c(w2)), combining it to finalize the NPMI (Normalized Pointwise Mutual Information) score.
Output Keys-Values:
decade word2 * TAB count
decade word2 word1 TAB count
After reduction: decade word1 word2 TAB count N log(c(w1,w2)) + log(N) - log(c(w1)) - log(c(w2))


### Step 4: Summation of NPMI Scores per Decade
Sums all NPMI scores for each decade.
Output Keys-Values:
decade word2 * NPMI count
decade word2 word1 TAB NPMI
After reduction: decade * NPMI sum
decade word1 word2 TAB count N NPMI


### Step 5: Filtering by Minimum NPMI Scores
Applies filtering based on relMinPmi and minPmi to exclude low NPMI scores.
Output Keys-Values:
decade word2 * NPMI count
decade word2 word1 TAB NPMI
After reduction: decade word1 word2 TAB count N NPMI

## Setup Instructions:
-Ensure the existence of the bucket named s3://<your-bucket-name/.
-Inside the bucket, ensure there is a logs/ folder: s3://<your-bucket-name>/logs/.
-Ensure to updaye on the credentials.
-Upload steps-1.0.jar to the bucket.

Run HadoopRunner-1.jar using the command:
-java -jar HadoopRunner-1.jar <minPmi> <relMinPmi>

## Details
#### Instance Type:
M4XLarge

#### Processing Time:
For the full English corpus with a minPmi of 0.5 and a relMinPmi of 0.2, using 63 mappers, the processing time was 2 hours and 5 minutes.

### Additional Information:
The system leverages Amazon EMR to enable scalable distributed processing.
Datasets are stored in Amazon S3 and directly accessed by the EMR nodes during processing.
Stop words are removed during the collocation extraction process.
The results are saved in Amazon S3, where they can be accessed for further analysis.

### References:
AWS documentation for AWS SDK and services.
Google 2-grams dataset.
