import requests
import zipfile
import io
import csv
import xml.etree.ElementTree as ET
import boto3

def lambda_handler():

    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    
    response = requests.get(url)
    
    xml_content = response.content
    
    root = ET.fromstring(xml_content)
    
    # loop through each <doc> tag and extract the download link
    for doc in root.findall('./result/doc'):
        download_link = doc.find("./str[@name='download_link']").text
        if(download_link):
            break
    
    # Download the zip file
    response = requests.get(download_link)
    
    # Extract the contents of the zip file
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    xml_filename = zip_file.namelist()[0]  # Assume there's only one file in the zip
    xml_content = zip_file.read(xml_filename).decode("utf-8")
    root = ET.fromstring(xml_content)
    
    data = []
    # Iterate through all elements in the XML file
    for child in root.iter('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}FinInstrmGnlAttrbts'):
        # Extract data from each element
        row = []
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}Id').text)
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}FullNm').text)
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}ClssfctnTp').text)
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}CmmdtyDerivInd').text)
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}NtnlCcy').text)
        row.append(child.find('{urn:iso:std:iso:20022:tech:xsd:head.003.001.01}Issr').text)
        data.append(row)
    
    # Create CSV file
    csv_data = [['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
                 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']]
    # Add data to CSV file
    csv_data.extend(data)
    
    # Write CSV file to in-memory buffer
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(csv_data)
    
    # Save CSV file in S3 bucket
    # Create S3 client
    s3 = boto3.client('s3')
    bucket_name = 'my_bucket_name'
    key = 'file.csv'
    # Upload the CSV file to S3
    # s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)