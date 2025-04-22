# Prisma Cloud Policies as a Spreadsheet - Automated Compliance Standard Policy Management Process

## Overview

This Python script is designed to manage compliance standard and policies within a cloud environment by interacting with the Prisma Cloud API.

It automates processes such as policy updates for compliance standard assignment, custom policy creation, RQL (Resource Query Language) validation, custom search creation, and updating CSV files with new policy information. 

It uses both synchronous and asynchronous methods to fetch, validate, and update policy details.  

Alert counts are included for each policy to asist with policy inclusion in the compliance standard or custom policy creation decision. 

The script leverages local configuration files to securely handle API credentials and performs extensive operations, including compliance metadata management and label processing.

### Key Features

- **Token Management**: Manages API access tokens and handles token renewal using an `asyncio.Lock` for asynchronous workflows.
- **Policy Fetching and Updating**: Retrieves compliance standards, policies by subtype/type, validates them, and updates their details in a CSV file.
- **Alerts Data Management**: Fetches alert counts for policies (resolved, open, dismissed) and processes them for analysis.
- **Excel Generation**: Reads and writes Excel files for further reporting and analysis.

## Requirements

To run this code, you will need:

- Python 3.10 or later
- The following Python libraries:
  - `requests`
  - `aiohttp`
  - `asyncio`
  - `configparser`
  - `boto3` (commented out but might be required if using AWS Secrets Manager)

## Installation

1. Setup **uv** on your local machine - https://github.com/astral-sh/uv:

   ```bash
   uv python install python3.12
   uv init compliance_policies
   uv add configparser boto3 requests aiohttp asyncio openpyxl colorama
3. Clone this repository to your local machine:
  ```
   git clone https://github.com/okostine-panw/compliance_policies.git
  ```
4. create data directory/folder to save your data
   
## Configuration

### API Configuration
The script uses a local `.ini` configuration file (`API_config.ini`) to manage credentials securely. Make sure to update the file with your Prisma Cloud API details.
Optional AWS Secret code is included in the comments

**Example `API_config.ini`**:
```ini
[URL]
BaseURL = https://your.prismacloud.api

[AUTHENTICATION]
ACCESS_KEY_ID = your_access_key_id
SECRET_KEY = your_secret_key
```
## Usage
The script is designed to be run from the command line:
  -  **uv run Compliance-All-Policies-excel.py** - Get all existing policies, **The script will prompt to chose compliance standard to work with from the list of the custom compliance standards on you Prisma CLoud instance. You can optionally define a specific custom Compliance standard id instead of having to select one - All of the scripts will not work without compliance standard id**
  -  **uv run Compliance-Policies-Update.py** - Create a compliance standard Reference with all Requirement/Section details and the number of included policies for each. This is useful for  the policies assigning task to help with understanding what specific Section ID means.  Update polcies details in the spreadsheet, update current policies details from API, validate custom policies RQL, create custom policy, discover new policies added to the platform. Generate totals spreadsheet. Update policies from the spreadsheet with Included == YES/REMOVE, set these polciies with new compliance standard assignment and labels for the standard and section, update the spreadsheet. 
Along with possible YES/REMOVE options for Included, the policy compliance section must be included to be specified in the SectionOption column. Requirement is automatically retrieved from the compliance standard for a given section.
-  **uv run Compliance-Policies-Create-Standard.py** - Create a compliance standard from requirements and sections input file, needs Company name and compliance stndard id to work with. Modify the input file name to match your company and compliance standard and contents to match your specific needs.

Requires write permissions (SystemAdmin for platform native policies) to update default policies. Or policies created user permissions for custom policies.
Create new custom policies in Prisma Cloud from the csv with Included == YES, update csv spreadsheet.
Policy compliance section to be included must be specified in SectionOption column.
Requires write permissions to create custom policies.

## Main Workflow

Initialization: Reads the API configuration file and initializes a TokenManager to manage API tokens.

Get Compliance Standards: Fetches compliance standards using the standard_id specified in the event data.

Fetch and Process Policies: Retrieves policies based on specific subtypes or types and processes them by updating or adding new rows to the CSV.
For Custom Policies:
  RQL Validation and Search Creation: It validates the RQL for each policy in the csv file with Included set to YES and creates a search in Prisma Cloud if necessary.
  Policy Creation: The script creates new compliance policies with metadata and assigns appropriate labels.

CSV File Generation: The final total results are saved in CSV format, including a detailed breakdown of compliance policies.
Excel Handling: The script reads input policies from the excel file, create a backup, processes policies and update the original _latest file with the results.

## Outputs

The script generates multiple files as outputs:

Updated Policies Excel spreadsheet (_latest): Contains updated policy details, including any new policies discovered. Previous spreadsheet timestamped backup is automatically created before making changes.

Consolidated Report: A CSV summary report of all processed data, including totals of compliance standards.

Compliance Standard Summary with detailed Requirements and Sections as well as policy assignments numbers per section.

## Contributing

Contributions are welcome. Please submit a pull request with your proposed changes.

## License

This project is licensed under the MIT License. See the LICENSE file for more information.

