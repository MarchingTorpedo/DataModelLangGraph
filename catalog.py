from fastapi import  APIRouter
from pydantic import BaseModel, Field
from debug2 import debug2 as dbg
from datetime import datetime
from typing import List, Optional
from toolbox import methods
from dotenv import load_dotenv
import os

load_dotenv()

class GetDataProfiles(BaseModel):
    """Represents the functionality for getting the list of all available files along with their url."""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))

class ReadBizContext(BaseModel):
    """Represents the functionality for reading Business Context from the provided Business Context file (txt file)."""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))
    biz_context: str = Field("LOB_source_biz_context.txt", description=("The unique name of the business context file, e.g 'LOB_source_biz_context.txt' (default: 'LOB_source_biz_context.txt')."))

class GetTableList(BaseModel):
    """Represents the functionality for getting list of distinct table names which will be used for fetching individual table data for data cataloging from data profiling report."""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))
    data_profile_report: List[str] = Field(... ,  description=("A list of completed data profiling report files to be used for generating the data catalog, e.g., ['20230801_data_profiling_report.csv', '20230802_data_profiling_report.csv']."))

class GetMetadata(BaseModel):
    """Represents the functionality for getting metadata for each table from data profiling report which will be used to create data catalog by the agent along with business context """
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))
    data_profile_report: List[str] = Field(... ,  description=("A list of completed data profiling report files to be used for generating the data catalog, e.g., ['20230801_data_profiling_report.csv', '20230802_data_profiling_report.csv']."))
    table_name: str = Field(... ,  description=("A single table name from list of table names obtained by using 'get_tables_list' tool. This table will be used to fetch the metadata from data profile report to generate data catalog. e.g., 'allergies'."))

class StoreDataCatalog(BaseModel):  
    """Represents the functionality for storing the generated data catalog. Accepts the json string of the generated data catalog."""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))
    catalog_data: List[str] = Field(... ,  description=("A list of string of generated data catalog. This data will be stored in a csv file"))
    catalog_filename: str = Field(default=None, description="File name for storing generated data catalog. The file is created by 'generate_catalog_file' tool and it is only called once during the entire data cataloging process.")

class GenerateCatalogFile(BaseModel):
    """Represents the functionality creates a empty csv file for storing data catalog. The tool is called only once during the entire data cataloging process."""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))

class UploadDataCatalog(BaseModel):
    """Represents the functionality to upload the generated data catalog to storage account"""
    user_id: str = Field("user2", description=("The unique user id of a specific user (default: 'user2')."))
    catalog_filename: str = Field(default=None, description="File name for storing generated data catalog. The file is created by 'generate_catalog_file' tool and it is only called once during the entire data cataloging process.")

script_name = os.path.basename(__file__)

router = APIRouter()

@router.post("/get_profile_report", operation_id="get_profile_report",
            summary="List completed data profiling reports available for data catalog generation",
            description=(
                "Returns a list of completed data profiling reports that are ready to be used for generating a data catalog. "
                "These reports contain metadata and profiling insights derived from previously processed input files. "
                "Use this endpoint when initiating catalog creation—not for starting new data profiling tasks."
                "Accepts a user ID as input and returns filenames along with their accessible URLs."
            ))
async def get_profile_report(p_body: GetDataProfiles):
    """
    Endpoint to retrieve all completed data profiling reports for a specific user, including filenames and direct URLs, which can be used to generate a data catalog.
   
    It returns a list of profiling reports
    that have already been processed and are ready for use. Each report includes:
        - `filename`: the name of the profiling report file
        - `url`: a direct link to access the report in Azure Blob Storage
       
    NOTE: Steps to follow:-
        - Call this `get_profile_report` tool when the user wants to generate a data catalog from existing profiling results.
        - This is **not** for listing raw input files. Use `get_files` for that purpose.
        - Display the list of available profiling reports in the chat.
        - Wait for user input to proceed with catalog generation.
        - Once the user selects data profile report, call the "data_catalog" tool for generating data catalog.

    Args:
        p_body (GetDataProfiles): Request body containing:
            user_id (str): The unique user id of a specific user (Defaults to "user2").

    Returns:
        dict: A dictionary containing:
            - available_input_files (list): List of dictionaries with 'filename' and 'url' for each report.
            - error (str, optional): Error message in case of failure.
    """
    try:
        ls_files = methods.get_available_files(p_body.user_id)
        print(f"Files available - {ls_files}")
        return {f"available_input_files": ls_files}
   
    except Exception as e:
        return {"error": f"Error in getting the data profile report list: {e}"}


@router.post("/read_biz_context", operation_id="read_biz_context",
            summary="Read and return business context from a specified file",
            description=(
                "Retrieves the business context text from a specified file stored in Azure Blob Storage. "
                "This context is typically used as a reference or input when generating a data catalog. "
                "The endpoint accepts a user ID and the name of the business context file, then returns the file's content as plain text."
            ))
async def read_biz_context(p_body: ReadBizContext):
    """
    Reads and returns the business context content from a specified file for a given user.

    This endpoint is used to extract descriptive business context information that supports data catalog generation.
    It does not return profiling reports or metadata files. Instead, it provides the raw textual content from a business context file.

    Args:
        p_body (ReadBizContext): Request body containing:
            - user_id (str): Unique identifier for the user (default: "user2").
            - biz_context (str): Name of the business context file (default: "LOB_source_biz_context.txt").

    Returns:
        dict: A dictionary containing:
            - Business_Context (str): The full text content of the business context file.
            - error (str, optional): Error message in case of failure.
    """
    try:
        biz_context = methods.read_business_context(p_body.user_id, p_body.biz_context)
        return {f"Business_Context": biz_context}
   
    except Exception as e:
        return {"error": f"Error in reading the business context file : {e}"}


@router.post("/get_tables_list", operation_id="get_tables_list",
            summary="Returns list of distinct table names from data profiling report for data cataloging",
            description=(
                "Extracts a list of distinct table names from completed data profiling report files stored in Azure Blob Storage. "
                "These table names are used to fetch individual table-level metadata during data catalog generation."
            ))
async def get_tables_list(p_body: GetTableList):
    """
    Retrieves a list of distinct table names from the specified data profiling report(s) for a given user.

    This endpoint is used during the data cataloging process to identify which tables are available for metadata extraction.
    It reads the profiling report(s), locates the 'object' column, and returns all unique table names found.

    NOTE: Steps to follow:
        1. This endpoint is part of the catalog generation workflow and should be used after 'read_biz_context' tool.
        2. Call this endpoint with the correct `user_id` and list of profiling report filenames.
        3. The endpoint reads the data profile report file and extracts unique values from the 'object' column.
        4. Use the returned table names to fetch detailed metadata for each table using the 'get_metadata' tool.
   
    Args:
        p_body (GetTableList): Request body containing:
            - user_id (str): Unique identifier for the user (default: "user2").
            - data_profile_report (List[str]): List of completed data profiling report filenames.

    Returns:
        dict: A dictionary containing:
            - Available_Table_Names_List (List[str]): List of distinct table names extracted from the profiling report(s).
            - error (str, optional): Error message in case of failure.
    """
    try:
        print(f"Get Table Function, User ID: {p_body.user_id}, Data Profile: {p_body.data_profile_report}")
        table_list = methods.get_list_of_tables(p_body.user_id, p_body.data_profile_report)
        return {f"Available_Table_Names_List": table_list}
   
    except Exception as e:
        return {"error": f"Error in getting the table list : {e}"}


@router.post("/get_tables_metadata", operation_id="get_tables_metadata",
            summary="Returns metadata for a specific table from data profiling report to support data catalog generation",
            description=(
                "Retrieves detailed metadata for a specified table from  data profiling report files. "
                "This metadata is used in combination with business context to generate a comprehensive data catalog."
            ))
async def get_tables_metadata(p_body: GetMetadata):
    """
    Retrieves metadata for a specific table from the provided data profiling report for a given user.

    This endpoint is used during the data cataloging process to extract column-level metadata, statistics, and profiling insights
    for a selected table. It filters the profiling report based on the table name and returns the relevant metadata in JSON format.

    NOTE: Steps to follow:
        1. Use the 'get_tables_list' tool to retrieve available table names from the profiling report.
        2. Call this endpoint with the correct `user_id`, list of profiling report filenames, and the selected `table_name`.
        3. The endpoint filters the profiling report(s) to extract metadata specific to the given table.
        4. Use the returned metadata along with business context to generate the final data catalog.
   
    Args:
        p_body (GetMetadata): Request body containing:
            - user_id (str): Unique identifier for the user (default: "user2").
            - data_profile_report (List[str]): List of completed data profiling report filenames.
            - table_name (str): Name of the table to extract metadata for (e.g., "allergies").

    Returns:
        dict: A dictionary containing:
            - Table_Metadata (List[dict]): Metadata records for the specified table.
            - error (str, optional): Error message in case of failure.

    """
    try:
        table_metadata = methods.get_data_profile(p_body.user_id, p_body.data_profile_report, p_body.table_name)
        return {f"Table_Metadata": table_metadata}
   
    except Exception as e:
        return {"error": f"Error in getting the table metadata : {e}"}

@router.post("/generate_catalog_file", operation_id="generate_catalog_file",
            summary="Returns data catalog file name which agent uses to store data catalog",
            description=(
                "Create a blank csv file which agent can use to store generated data catalog "
                "This tool is called only once"
            ))
async def generate_catalog_file(p_body: GenerateCatalogFile):
    """
    Generates a blank CSV file for storing the data catalog.

    This endpoint is designed to be invoked once during the entire data cataloging process. It creates an empty CSV file
    with a unique timestamp-based filename and stores it in a specific directory. The generated filename is returned
    to the agent, which will use it to populate the data catalog during subsequent operations.
   
    NOTE: This tool should only be called once per cataloging session.
   
    Args:
    p_body : GenerateCatalogFile
        A request body containing the user ID. If not provided, defaults to 'user2'.

    Returns:
    str:
        - catalog_filename (str): The name of the newly created CSV file, which will be used to store the data catalog.
    """
    execution_id = datetime.now().strftime('%Y%m%d%H%M%S')
    catalog_filename = f'{execution_id}_data_catalog.csv'
    catalog_directory = f"{os.getenv('OUTPUT_BASE_PATH')}Data_Catalog/{p_body.user_id}"
    os.makedirs(catalog_directory, exist_ok=True)
    file_path = os.path.join(catalog_directory, catalog_filename)
   
    with open(file_path, 'w') as f:
        pass
    return catalog_filename
   
   
@router.post("/store_catalog", operation_id="store_catalog",
            summary="Stores generated data catalog into an existing CSV file",
            description=(
                "Stores the generated data catalog into a CSV file created by the 'generate_catalog_file' tool. "
                "This tool supports iterative updates to the catalog file using profiling metadata and business context. "
                "The catalog data must be passed as a JSON string, which will be parsed and appended to the specified CSV file. "
                "The catalog file is created only once at the beginning of the cataloging process and reused for all subsequent updates."
            ))
async def store_catalog(p_body: StoreDataCatalog):
    """
    Stores the generated data catalog into a CSV file created by the 'generate_catalog_file' tool.

    This endpoint is used to persist structured metadata derived from profiling reports and business context.
    The input catalog data must be provided as a JSON-formatted string. The tool parses this string, converts it
    into tabular format, and appends it to the designated CSV file.

    Important:
    - The CSV file must be created beforehand using the 'generate_catalog_file' tool, which is called only once.
    - The filename returned by that tool should be passed as `catalog_filename` in all calls to this endpoint.
    - This tool supports multiple invocations to iteratively build the catalog.
    - Stores one table at a time.

    NOTE: Usage Steps:
    1. Call 'generate_catalog_file' once to create the CSV file and get the filename.
    2. Generate catalog data as a JSON string using profiling and business logic.
    3. Call this endpoint with the catalog string and the filename from step 1.
    4. Repeat step 3 to append additional catalog entries as needed.

    Args:
        user_id (str): Unique identifier for the user (default: 'user2').
        catalog_data (List[str]): JSON string representing the generated data catalog.
        catalog_filename (str): Name of the CSV file created by 'generate_catalog_file'. Required for storing data.

    Returns:
        dict: A dictionary containing the stored table metadata or an error message.
    """


    try:
        # print(f"user_id: {p_body.user_id}, catalog: {p_body.catalog_filename}")
        # print(f"data: {p_body.catalog_data}")
        table_metadata = methods.store_data_catalog(p_body.user_id, p_body.catalog_data, p_body.catalog_filename)
        return {f"Table_Metadata": table_metadata}
   
    except Exception as e:
        return {"error": f"Error in storing data catalog : {e}"}

@router.post("/upload_catalog", operation_id="upload_catalog",
            summary="Uploads the generated data catalog file to Storage Account",
            description=(
                "Uploads the generated data catalog file to ADLS storage account"
                "This tool is only called once at the end of entire data cataloging process "
            ))
async def store_catalog(p_body: StoreDataCatalog):
    """
    Uploads the finalized data catalog CSV file to the ADLS storage account.

    This endpoint is called only once at the end of the entire data cataloging process. It uploads the completed
    data catalog file—originally created by the 'generate_catalog_file' tool and populated using the 'store_catalog' tool—
    to a designated Azure Data Lake Storage (ADLS) location.

    Important:
    - The catalog file must already exist and be fully populated before calling this endpoint.
    - This tool is used only once, after all catalog data for all tables has been written to the file.

    Usage Steps:
    1. Call 'generate_catalog_file' once to create the CSV file and receive the filename.
    2. Use 'store_catalog' one or more times to populate the file with catalog data.
    3. Once the catalog is complete, call this endpoint with the filename to upload it to the storage account.

    Args:
        user_id (str): Unique identifier for the user (default: 'user2').
        catalog_filename (str): Name of the completed catalog CSV file to upload. Must match the filename created by 'generate_catalog_file'.

    Returns:
        dict: A dictionary containing upload confirmation details or an error message if the upload fails.
    """

    try:
       
        up_cat = methods.upload_blob_files(p_body.user_id,  p_body.catalog_filename)
        return {f"Uploaded_file_details:": up_cat}
   
    except Exception as e:
        return {"error": f"Error in uploading the file : {e}"}
