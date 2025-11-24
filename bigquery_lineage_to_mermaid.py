# BQ to Mermaid diagram

# What This Script Does:
# Connects to BigQuery using the google-cloud-bigquery Python client.
# Iterates through all datasets in the specified GCP project.
# Queries INFORMATION_SCHEMA.REFERENCED_TABLES to find dependencies.
# Builds a lineage dictionary mapping source → target tables.
# Outputs a Mermaid diagram definition which can be pasted into Confluence.

# To Do:
# Create aditional loop based on Defined Project list
# Output directly to Confluence using api


from google.cloud import bigquery

def extract_bigquery_lineage(project_id):
    client = bigquery.Client(project=project_id)
    
    # Get list of datasets in the project
    datasets = list(client.list_datasets())
    lineage = {}

    for dataset in datasets:
        dataset_id = dataset.dataset_id
        query = f"""
            SELECT
                table_name,
                referenced_project_id,
                referenced_dataset_id,
                referenced_table_id
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.REFERENCED_TABLES`
        """
        try:
            query_job = client.query(query)
            results = query_job.result()
            for row in results:
                target = f"{project_id}.{dataset_id}.{row.table_name}"
                source = f"{row.referenced_project_id}.{row.referenced_dataset_id}.{row.referenced_table_id}"
                if target not in lineage:
                    lineage[target] = []
                lineage[target].append(source)
        except Exception as e:
            print(f"Skipping dataset {dataset_id} due to error: {e}")

    return lineage

def generate_mermaid_diagram(lineage):
    lines = ["graph TD"]
    for target, sources in lineage.items():
        for source in sources:
            lines.append(f"    {source.replace('.', '_')} --> {target.replace('.', '_')}")
    return "
".join(lines)

if __name__ == "__main__":
    project_id = "your-gcp-project-id"  # Replace with your GCP project ID
    lineage = extract_bigquery_lineage(project_id)
    diagram = generate_mermaid_diagram(lineage)
    print("
Mermaid Diagram Definition:
")
    print(diagram)