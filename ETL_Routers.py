from collections import defaultdict
import pandas as pd
from pathlib import Path
from pymongo import MongoClient

# Connect to MongoDB and retrieve documents
def connect_to_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["Router_Ingested"]
    return db

def get_documents(db):
    config_collection = db["Router_Configuration"]
    traffic_collection = db["Router_Traffic"]
    config_documents = list(config_collection.find())
    traffic_documents = list(traffic_collection.find())
    return config_documents, traffic_documents

# Process documents
def process_documents(config_documents, traffic_documents, processed_documents_file):
    matched_documents = []  # List to store matched documents
    processed_document_ids = set()  # Set to store processed document IDs

    processed_documents_path = Path(processed_documents_file)

    # Load processed document IDs from the file
    if processed_documents_path.exists():
        with open(processed_documents_path, 'r') as f:
            processed_document_ids = set(line.strip() for line in f)

    for config_doc in config_documents:
        config_doc_id = str(config_doc['_id'])  # Convert ObjectId to string
        # Check if the config document ID has already been processed
        if config_doc_id in processed_document_ids:
            print(f"Config document {config_doc_id} already processed. Skipping.")
            continue  # Skip processing this document
        
        config_routers = config_doc.get('routers', {})
        for ip_address, router_data in config_routers.items():
            config_time = list(router_data.keys())[0] if router_data else None
            if not config_time:
                continue
            for traffic_doc in traffic_documents[:]:  # Iterate over a copy of traffic_documents
                traffic_routers = traffic_doc.get('routers', {})
                traffic_router_data = traffic_routers.get(ip_address, {})
                traffic_time = list(traffic_router_data.keys())[0] if traffic_router_data else None
                if config_time == traffic_time:
                    matched_documents.append((config_doc, traffic_doc))
                    print("Match found between config document and traffic document:")
                    print("Config Document:", config_doc["_id"])
                    print("Traffic Document:", traffic_doc["_id"])
                    traffic_documents.remove(traffic_doc)  # Remove the matched traffic document

    if matched_documents:
        # Update processed documents file
        update_processed_documents(matched_documents, processed_documents_path)
    else:
        print("No matching documents found.")

    return matched_documents






def update_processed_documents(matched_documents, file_path):
    processed_document_ids = set()  # Set to store processed document IDs
    
    # Load processed document IDs from the file
    with open(file_path, 'r') as f:
        for line in f:
            processed_document_ids.add(line.strip())
    
    with open(file_path, 'a') as f:  # Open the file in append mode
        for config_doc, traffic_doc in matched_documents:
            config_doc_id = str(config_doc['_id'])
            traffic_doc_id = str(traffic_doc['_id'])
            if config_doc_id not in processed_document_ids:
                f.write(config_doc_id + '\n')
            if traffic_doc_id not in processed_document_ids:
                f.write(traffic_doc_id + '\n')
            processed_document_ids.add(config_doc_id)
            processed_document_ids.add(traffic_doc_id)
    
    print("Matched document IDs saved to 'processed_documents.txt'.")




def extract_interface_id(stat_description):
    """
    Extracts the interface ID from the stat description.

    Args:
        stat_description (str): The description of the statistic.

    Returns:
        int: The extracted interface ID.
    """
    stat_description = stat_description.strip()  # Remove leading/trailing spaces
    if '.' in stat_description:
        interface_id = stat_description.split('.')[-1]
        if interface_id.isdigit():  # Check if the extracted part is a digit
            return int(interface_id)
    return 0  # Return 0 if no valid interface ID is found

def remove_interface_id(stat_description):
    """
    Removes the interface ID from the stat description.

    Args:
        stat_description (str): The description of the statistic.

    Returns:
        str: The stat description without the interface ID.
    """
    parts = stat_description.split('.')
    return '.'.join(parts[:-1])

def extract_protocol_version(stat_description):
    """
    Extracts the protocol version from the stat description.

    Args:
        stat_description (str): The description of the statistic.

    Returns:
        str: The extracted protocol version.
    """
    parts = stat_description.split('.')
    protocol_version = parts[-1]
    return protocol_version

def remove_protocol_version(stat_description):
    """
    Removes the protocol version from the stat description.

    Args:
        stat_description (str): The description of the statistic.

    Returns:
        str: The stat description without the protocol version.
    """
    parts = stat_description.split('.')
    description_without_protocol = '.'.join(parts[:-1])
    return description_without_protocol


def create_stat_dataframe_from_document(stat_document):

    print("Creating DataFrame from Stat document")

    df_list = []
    for ip_address, ip_data in stat_document["routers"].items():
        for measure_time, stats in ip_data["Measure_Time"].items():
            for stat_description, stat_value in stats.items():
                if stat_description == "Time":  # Skip 'Time' entry
                    continue
                interface_id = extract_interface_id(stat_description)
                if interface_id != 0:
                    stat_description = remove_interface_id(stat_description)
                protocol_version = extract_protocol_version(stat_description)
                stat_description = remove_protocol_version(stat_description)
                measure_date = measure_time.split()[0]
                measure_Time = measure_time.split()[1]

                df_list.append({
                    'IP_Address': ip_address,
                    'Measure_Date': measure_date,
                    'Measure_Time': measure_Time,
                    'Protocol_Version': protocol_version,
                    'Stat_Description': stat_description,
                    'Interface_ID': interface_id,
                    'Stat_Value': stat_value
                })

    stat_df = pd.DataFrame(df_list)

    description_mapping = {
    "IP-MIB-ipIfStatsInBcastPkts" : "IP Interface Inbound Broadcast Packets",
    "IP-MIB-ipIfStatsInDiscards" :  "IP Interface Inbound Discarded Packets",
    "IP-MIB-ipIfStatsInMcastOctets" : "IP Interface Inbound Multicast Octets",
    "IP-MIB-ipIfStatsInMcastPkts" : "IP Interface Inbound Multicast Packets",  
    "IP-MIB-ipIfStatsHCOutMcastPkts" : "IP Interface Outbound Multicast Packets(High Capacity)",
    "IP-MIB-ipIfStatsHCOutMcastOctets" : "IP Interface Outbound Multicast Octets(High Capacity)",
    "IP-MIB-ipIfStatsInAddrErrors" :  "IP Interface Inbound Address Errors",
    "IP-MIB-ipIfStatsInUnknownProtos" :  "IP Interface Inbound Packets with Unknown Protocols",
    "IP-MIB-ipIfStatsOutBcastPkts" : "IP Interface Outbound Broadcast Packets",
    "IP-MIB-ipIfStatsOutDiscards" :  "IP Interface Outbound Discarded Packets",
    "IP-MIB-ipIfStatsHCInBcastPkts" : "IP Interface Inbound Broadcast Packets(High_Capacity)",
    "IP-MIB-ipIfStatsHCInMcastOctets" : "IP Interface Inbound Multicast Octets(High_Capacity)",
    "IP-MIB-ipIfStatsHCOutBcastPkts" : "IP Interface Outbound Broadcast Packets(High_Capacity)",
    "IP-MIB-ipIfStatsHCInMcastPkts" : "IP Interface Inbound Multicast Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsOutNoRoutes" : "IP System Outbound Packets with No Routes",    
    "IP-MIB-ipIfStatsOutFragFails": "IP Interface Out Fragmentation Failures",
    "IP-MIB-ipIfStatsOutFragOKs": "IP Interface Out Fragmentation Successes",
    "IP-MIB-ipIfStatsOutFragReqds": "IP Interface Out Fragmentation Requests",
    "IP-MIB-ipIfStatsOutMcastOctets": "IP Interface Out Multicast Octets",
    "IP-MIB-ipIfStatsOutMcastPkts": "IP Interface Out Multicast Packets",
    "IP-MIB-ipIfStatsOutOctets": "IP Interface Out Octets",
    "IP-MIB-ipIfStatsOutRequests": "IP Interface Out Requests",
    "IP-MIB-ipIfStatsOutTransmits": "IP Interface Out Transmits",
    "IP-MIB-ipIfStatsReasmFails": "IP Interface Reassembly Failures",
    "IP-MIB-ipIfStatsReasmOKs": "IP Interface Reassembly Successes",
    "IP-MIB-ipIfStatsReasmReqds": "IP Interface Reassembly Requests",
    "IP-MIB-ipSystemStatsHCInBcastPkts": "IP System Inbound Broadcast Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInDelivers": "IP System Inbound Delivered Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInForwDatagrams": "IP System Inbound Forwarded Datagrams(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInMcastOctets": "IP System Inbound Multicast Octets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInMcastPkts": "IP System Inbound Multicast Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInOctets": "IP System Inbound Octets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCInReceives": "IP System Inbound Receives(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutBcastPkts": "IP System Outbound Broadcast Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutForwDatagrams": "IP System Outbound Forwarded Datagrams(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutMcastOctets": "IP System Outbound Multicast Octets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutMcastPkts": "IP System Outbound Multicast Packets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutOctets": "IP System Outbound Octets(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutRequests": "IP System Outbound Requests(High_Capacity)",
    "IP-MIB-ipSystemStatsHCOutTransmits": "IP System Outbound Transmits(High_Capacity)",
    "IP-MIB-ipSystemStatsInAddrErrors": "IP System Inbound Address Errors",
    "IP-MIB-ipSystemStatsInBcastPkts": "IP System Inbound Broadcast Packets",
    "IP-MIB-ipSystemStatsInDelivers": "IP System Inbound Delivered Packets",
    "IP-MIB-ipSystemStatsInDiscards": "IP System Inbound Discarded Packets",
    "IP-MIB-ipSystemStatsInForwDatagrams": "IP System Inbound Forwarded Datagrams",
    "IP-MIB-ipSystemStatsInHdrErrors": "IP System Inbound Header Errors",
    "IP-MIB-ipSystemStatsInMcastOctets": "IP System Inbound Multicast Octets",
    "IP-MIB-ipSystemStatsInMcastPkts": "IP System Inbound Multicast Packets",
    "IP-MIB-ipSystemStatsInNoRoutes": "IP System Inbound Packets with No Routes",
    "IP-MIB-ipSystemStatsInOctets": "IP System Inbound Octets",
    "IP-MIB-ipSystemStatsInReceives": "IP System Inbound Receives",
    "IP-MIB-ipSystemStatsInTruncatedPkts": "IP System Inbound Truncated Packets",
    "IP-MIB-ipSystemStatsInUnknownProtos": "IP System Inbound Packets with Unknown Protocols",
    "IP-MIB-ipSystemStatsOutBcastPkts": "IP System Outbound Broadcast Packets",
    "IP-MIB-ipSystemStatsOutDiscards": "IP System Outbound Discarded Packets",
    "IP-MIB-ipSystemStatsOutForwDatagrams": "IP System Outbound Forwarded Datagrams",
    "IP-MIB-ipSystemStatsOutFragCreates": "IP System Outbound Fragmentation Creates",
    "IP-MIB-ipSystemStatsOutFragFails": "IP System Outbound Fragmentation Failures",
    "IP-MIB-ipSystemStatsOutFragOKs": "IP System Outbound Fragmentation Successes",
    "IP-MIB-ipSystemStatsOutFragReqds": "IP System Outbound Fragmentation Requests",
    "IP-MIB-ipSystemStatsOutMcastOctets": "IP System Outbound Multicast Octets",
    "IP-MIB-ipSystemStatsOutMcastPkts": "IP System Outbound Multicast Packets",
    "IP-MIB-ipSystemStatsOutOctets": "IP System Outbound Octets",
    "IP-MIB-ipSystemStatsOutRequests": "IP System Outbound Requests",
    "IP-MIB-ipSystemStatsOutTransmits": "IP System Outbound Transmits",
    "IP-MIB-ipSystemStatsReasmFails": "IP System Reassembly Failures",
    "IP-MIB-ipSystemStatsReasmOKs": "IP System Reassembly Successes",
    "IP-MIB-ipSystemStatsReasmReqds": "IP System Reassembly Requests",
    "IP-MIB-ipIfStatsHCInDelivers": "IP Interface Inbound Delivered Packets(High_Capacity)",
    "IP-MIB-ipIfStatsHCInForwDatagrams": "IP Interface Inbound Forwarded Datagrams(High_Capacity)",
    "IP-MIB-ipIfStatsHCInOctets": "IP Interface Inbound Octets(High_Capacity)",
    "IP-MIB-ipIfStatsHCInReceives": "IP Interface Inbound Receives(High_Capacity)",
    "IP-MIB-ipIfStatsHCOutForwDatagrams": "IP Interface Outbound Forwarded Datagrams(High_Capacity)",
    "IP-MIB-ipIfStatsHCOutOctets": "IP Interface Outbound Octets(High_Capacity)",
    "IP-MIB-ipIfStatsHCOutRequests": "IP Interface Outbound Requests(High_Capacity)",
    "IP-MIB-ipIfStatsHCOutTransmits": "IP Interface Outbound Transmits(High_Capacity)",
    "IP-MIB-ipIfStatsInDelivers": "IP Interface Inbound Delivered Packets",
    "IP-MIB-ipIfStatsInForwDatagrams": "IP Interface Inbound Forwarded Datagrams",
    "IP-MIB-ipIfStatsInHdrErrors": "IP Interface Inbound Header Errors",
    "IP-MIB-ipIfStatsInNoRoutes": "IP Interface Inbound Packets with No Routes",
    "IP-MIB-ipIfStatsInOctets": "IP Interface Inbound Octets",
    "IP-MIB-ipIfStatsInReceives": "IP Interface Inbound Receives",
    "IP-MIB-ipIfStatsInTruncatedPkts": "IP Interface Inbound Truncated Packets",
    "IP-MIB-ipIfStatsOutForwDatagrams": "IP Interface Outbound Forwarded Datagrams",
    "IP-MIB-ipIfStatsOutFragCreates": "IP Interface Outbound Fragmentation Creates"
}

    # Map the values in the Stat_Description column using the mapping dictionary
    stat_df['New_Stat_Description'] = stat_df['Stat_Description'].map(description_mapping)

    # For entries where mapping doesn't exist, keep the original description
    stat_df['New_Stat_Description'].fillna(stat_df['Stat_Description'], inplace=True)

    print("Stat Data Frame:")
    print(stat_df.head())
    print("Length of stat_df:", len(stat_df))
    return stat_df



def create_config_dataframe_from_document(config_document):

    print("Creating DataFrame from Config document")

    df_list = []
    for ip_address, ip_data in config_document["routers"].items():
        for measure_time, interfaces in ip_data["Measure_Time"].items():
            for interface, interface_description in interfaces.items():
                if interface == "Time":  # Skip 'Time' entry
                    continue
                interface_id = extract_interface_id(interface)
                interface_name = interface.split('.')[0]
                measure_date = measure_time.split()[0]

                df_list.append({
                    'IP_Address': ip_address,
                    'Interface_ID': interface_id,
                    'Interface_Name': interface_name,
                    'Interface_Description': interface_description
                })

    config_df = pd.DataFrame(df_list)

    print("Config Data Frame:")
    print(config_df.head())
    print("Length of config_df:", len(config_df))
    return config_df

def create_documents(merged_df):
    documents = []
    # Use defaultdict to store interfaces by (Protocol_Version, Measure_Date, Measure_Time, IP_Address)
    interfaces_dict = defaultdict(list)
    
    # Separate rows with Interface_ID = 0
    interface_0_rows = merged_df[merged_df['Interface_ID'] == 0]

    # Filter out rows where Stat_Value is 0
    merged_df = merged_df[merged_df['Stat_Value'] != 0]

    # Group rows by key (excluding Interface_ID = 0 rows)
    for _, row in merged_df.iterrows():
        key = (row["Protocol_Version"], row["Measure_Date"], row["Measure_Time"], row["IP_Address"])
        interfaces_dict[key].append(row)
    
    # Iterate over groups and create documents
    for key, rows in interfaces_dict.items():
        general_metrics = [row for row in rows if row["Interface_ID"] == 0]
        other_metrics = [row for row in rows if row["Interface_ID"] != 0]
        
        # Create documents for other metrics
        for other_metric in other_metrics:
            other_doc = {
                "IP_Address": other_metric["IP_Address"],
                "Measure_Date": other_metric["Measure_Date"],
                "Measure_Time": other_metric["Measure_Time"],
                "Interface_ID": other_metric["Interface_ID"],
                "Interface_Name": other_metric["Interface_Name"],
                "Interface_Description": other_metric["Interface_Description"],
                "Protocol_Version": other_metric["Protocol_Version"],
                "New_Stat_Description": other_metric["New_Stat_Description"],
                "Stat_Value": other_metric["Stat_Value"]
            }
            
            # Find matching Interface_ID = 0 rows
            matching_rows = interface_0_rows[
                (interface_0_rows['Protocol_Version'] == other_metric['Protocol_Version']) &
                (interface_0_rows['Measure_Date'] == other_metric['Measure_Date']) &
                (interface_0_rows['Measure_Time'] == other_metric['Measure_Time']) &
                (interface_0_rows['IP_Address'] == other_metric['IP_Address'])
            ]
            
            # Add New_Stat_Description and Stat_Value from matching Interface_ID = 0 rows
            for _, matching_row in matching_rows.iterrows():
                other_doc['New_Stat_Description'] = matching_row['New_Stat_Description']
                other_doc['Stat_Value'] += matching_row['Stat_Value']  # Add Stat_Value
                
            documents.append(other_doc)
    
    return documents


#insert the document into mongodb collection
def insert_documents_into_mongodb(documents):
    """
    Insert documents into a MongoDB collection.
    
    Args:
        documents (list): List of documents to be inserted into MongoDB.
    """
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["Processed_DataBase"]  
    collection = db["Processed_Routers_Metrics"] 

    # Add indexes to the collection
    collection.create_index("IP_Address")
    collection.create_index("Measure_Date")
    collection.create_index("Measure_Time")
    collection.create_index("Protocol_Version")
    collection.create_index("Interface_ID") 
    
    # Insert documents into the collection
    collection.insert_many(documents)
    print("Documents inserted into MongoDB successfully.")


# Main function
def main():
    # Connect to MongoDB
    db = connect_to_mongodb()
    
    # Define the path for the processed documents file
    processed_documents_file = r'C:\Users\hp\OneDrive\Bureau\End_Studies_Project\Text_Files\Processed_documents.txt'

    while True:
        # Retrieve documents
        config_documents, traffic_documents = get_documents(db)
        
        # Process documents
        matched_documents = process_documents(config_documents, traffic_documents, processed_documents_file)

        # Update processed documents file
        update_processed_documents(matched_documents, processed_documents_file)

        # Continue processing only if there are matched documents
        if not matched_documents:
            print("No more matched documents to process.")
            break
        
        # Iterate over matched documents and perform further processing
        for config_doc, traffic_doc in matched_documents:
            # Create data frames from config and traffic documents
            config_df = create_config_dataframe_from_document(config_doc)
            print("Columns of config_df:", config_df.columns)

            traffic_df = create_stat_dataframe_from_document(traffic_doc)
            print("Columns of traffic_df:", traffic_df.columns)

            # Merge data frames based on common columns
            merged_df = pd.merge(traffic_df, config_df, on=['IP_Address', 'Interface_ID'], how='left')
            print("Columns of merged_df:", merged_df.columns) 
            # Replace empty columns for Interface_ID = 0
            merged_df.loc[merged_df['Interface_ID'] == 0, 'Interface_Name'] = 'General'
            merged_df.loc[merged_df['Interface_ID'] == 0, 'Interface_Description'] = 'General'
 
            #Create documents for insertion into MongoDB
            documents = create_documents(merged_df)

            # Insert documents into MongoDB collection
            insert_documents_into_mongodb(documents)

if __name__ == "__main__":
    main()




