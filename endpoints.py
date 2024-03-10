import io
import os
from salesforce import Salesforce
from vector_store import Vector_Store
from fastapi import FastAPI, responses
from dotenv import load_dotenv, find_dotenv
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

_ = load_dotenv(find_dotenv())

# my_knowledge__kav_createdbyid = '005Vd000001282YIAQ'
# my_contentversion_createdbyid = '005Vd000001282YIAQ'

# Create a FastAPI instance
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Create a Salesforce instance
sf = Salesforce(
    os.getenv("USERNAME"),
    os.getenv("PASSWORD"),
    os.getenv("CONSUMER_KEY"),
    os.getenv("CONSUMER_SECRET"),
    os.getenv("DOMAIN"),
)


# Create and initialize VectorStore instance
db = Vector_Store(sf=sf, collection_name="ContentVersion")

metadata_fields = [
    "Id",
    "CreatedById",
    "ContentDocumentId",
    "CreatedDate",
    "Title",
    "FileType",
]

db.init_vector_semantic(
    metadata=metadata_fields,
    chunk_size=600,
    chunk_overlap=40,
    From="ContentVersion",
    # Where="CreatedById="'005Vd000000Zn4HIAS'",
    Where="CreatedById='005Vd000001282YIAQ'",
)

db.init_vector_exact(
    metadata=metadata_fields,
    From="ContentVersion",
    # Where="CreatedById="'005Vd000000Zn4HIAS'",
    Where="CreatedById='005Vd000001282YIAQ'",
)

metadata_assets_fields = [
    "Id",
    "KnowledgeArticleId",
    "CreatedById",
    "Language",
    "Title",
    "Summary",
    "Description__c",
]
db.init_vector_assets(
    metadata=metadata_assets_fields,
    From="Knowledge__kav",
    Where="CreatedById='005Vd000001282YIAQ'",
)

db.init_vector_assets_exact(
    metadata=metadata_assets_fields,
    From="Knowledge__kav",
    Where="CreatedById='005Vd000001282YIAQ'",
)


# Define home endpoints
@app.get("/")
def home():
    return {"message": "Welcome to the Salesforce API!"}


# Define semantic search endpoint
@app.get("/semantic_search")
def semantic_search(
    query: str,
    files_title: str = None,
    last_day: bool = False,
    last_month: bool = False,
    last_year: bool = False,
):
    files_title_filters = db.filters(Title=files_title)
    files_time_filters = db.time_filters(
        last_day=last_day, last_month=last_month, last_year=last_year
    )
    results_dictionary = {}
    results_dictionary["assets_results"] = db.assets_semantic_search(
        query=query,
        n_results=4,
    )
    results_dictionary["files_results"] = db.files_semantic_search(
        query=query,
        n_results=4,
        max_distance=0.5,
        # filters=files_title_filters,
        # time_filter=files_time_filters,
    )
    # combined_results = db.combine_files_with_assets(
    #     result_files=results_dictionary["files_results"]
    # )
    # return combined_results
    return results_dictionary


# @app.get("/semantic_search")
# def semantic_search(query: str):
#     results_dictionary = {}
#     results_dictionary["assets_results"] = db.assets_semantic_search(query, 2)
#     results_dictionary["files_results"] = db.files_semantic_search(query, 10, 0.5)
#     attachment_links = sf.get_attachments_in_assets(
#         results_dictionary["assets_results"]
#     )

#     assets_results_dict = {
#         asset["Id"]: asset for asset in results_dictionary["assets_results"]
#     }
#     print("ommmmar", assets_results_dict, "\n")
#     linked_attachments = []
#     for asset_id, attachment_ids in attachment_links.items():
#         if attachment_ids == ["0"]:
#             continue
#         print(f"Asset ID: {asset_id}")
#         print(f"Attachment IDs: {attachment_ids}")
#         for file_result in results_dictionary["files_results"]:
#             print(f"File Result ID: {file_result['Id']}")
#             print(f"file attachments so far: {attachment_ids}")
#             if file_result["Id"] in attachment_ids:
#                 print("hi there")
#                 linked_attachments.append(file_result)

#         assets_results_dict[asset_id]["linked_attachments"] = linked_attachments
#     return list(assets_results_dict.values())


# Define non semantic search endpoint
@app.get("/exact_search")
def exact_search(
    query: str,
    files_title: str = None,
    last_day: bool = False,
    last_month: bool = False,
    last_year: bool = False,
):
    files_title_filters = db.filters(Title=files_title)
    files_time_filters = db.time_filters(
        last_day=last_day, last_month=last_month, last_year=last_year
    )
    results_dictionary = {}
    results_dictionary["assets_results"] = db.assets_exact_search(
        search_query=query,
        n_results=4,
    )
    results_dictionary["files_results"] = db.files_exact_search(
        search_query=query,
        n_results=4,
        # max_distance=0.5,
        filters=files_title_filters,
        time_filter=files_time_filters,
    )
    combined_results = db.combine_files_with_assets(
        result_files=results_dictionary["files_results"]
    )
    return combined_results


# Define chatbot endpoint
@app.get("/chatbot")
def chatbot(query: str):
    pass


# Define add_document endpoint
@app.post("/records/add_document")
def add_document():
    db.add_document(From="ContentVersion", Where="CreatedById='005Vd000000Zn4HIAS'")


# Define delete_document endpoint
@app.delete("/records/delete_document")
def delete_document():
    db.delete_document(From="ContentVersion", Where="CreatedById='005Vd000000Zn4HIAS'")


# Define download records endpoint
@app.get("/records/download")
def download_record(id: str):
    file_bytes = sf.get_record_by_id(id)
    metadata = sf.get_metadata_by_id(
        id, metadata_fields=["Title", "FileExtension"], From="ContentVersion"
    )
    file_name = f"{metadata['Title']}.{metadata['FileExtension']}"
    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={file_name}"},
    )
