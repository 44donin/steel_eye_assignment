'''
  The code will get the zip file form the link provided from the assignement and convert the xml file into a csv and upload it.
  

'''
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import os
from io import StringIO
import boto3


'''
 The class stell_eye_data_push does the task of pushing data into the s3 bucket when the given sequence of downloading and converting the steps
 are successfully completed


'''


class steel_eye_data_push:
  def __init__(self):
    '''
    the function dont need any input parameters since i am using a given fixed url as of now
    
    '''
    self.base_url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    
  def get_zip_file_link(self):
    '''
    It uses the base url link to get the link of zip file from the given xml from the link

    format:
    from the list of str tags we are getting the download links
    we are choosing the first download link to download the zip file
    
    '''
    response = requests.get(self.base_url)
    soup = BeautifulSoup(response.content,"xml")
    return soup.findAll("str",{"name":"download_link"})[0].text

  def convert_into_csv(self):
    '''
     this function takes the downloaded xml file from the steel_eye_data_push folder and converts the xml to csv based on the columns namely

              ['FinInstrmGnlAttrbts.Id',
              'FinInstrmGnlAttrbts.FullNm',
              'FinInstrmGnlAttrbts.ClssfctnTp',
              'FinInstrmGnlAttrbts.CmmdtyDerivInd',
              'FinInstrmGnlAttrbts.NtnlCcy',
              ]
    
    '''

    path = os.path.join("steel_eye_data_push",os.listdir("steel_eye_data_push")[0])
    soup = BeautifulSoup(open(path),"xml")
    column_names = ['FinInstrmGnlAttrbts.Id',
              'FinInstrmGnlAttrbts.FullNm',
              'FinInstrmGnlAttrbts.ClssfctnTp',
              'FinInstrmGnlAttrbts.CmmdtyDerivInd',
              'FinInstrmGnlAttrbts.NtnlCcy',
              ]
    df = pd.DataFrame(columns=column_names)
    iter = 0
    for tag in soup.find_all("FinInstrmGnlAttrbts"):
      row = [tag.Id.text,tag.FullNm.text,tag.ClssfctnTp.text,tag.CmmdtyDerivInd.text,tag.NtnlCcy.text]
      df.loc[len(df.index)] = row
      iter+=1
      if(iter>=100):
        iter=0
        break
    column_names_2 = ["Issr"
              ]
    df2 = pd.DataFrame(columns=column_names_2)

    for tag in soup.find_all("Issr"):
      row = [tag.text]
      iter+=1
      if(iter>=100):
        iter=0
        break
    df2.loc[len(df2.index)] = row
    self.final_csv = pd.concat([df,df2],axis="columns")
    return self.final_csv.shape
  def get_xml_file(self):
    '''
      this function gets the xml and stores it in an  arbitary directory name  steel_eye_data_push
    
    '''
    self.download_and_unzip(self.get_zip_file_link(),"steel_eye_data_push")
    
    

  def download_and_unzip(self,url, extract_to='.'):
    '''
     this function downloads the csv file from the url and unzips the zipped url file
    
    '''
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=extract_to)

  def push_to_bucket(self):
    '''
    this function pushes the data frame into the s3 bucket 
    
    '''
    s3bucket = boto3.client("s3",aws_access_key_id="")
    csv_buffer = StringIO()
    self.final_csv.to_csv(csv_buffer,header=True,index=False)
    csv_buffer.seek(0)
    s3bucket.put_object(Bucket="",Body=csv_buffer.get_value(),key="filename.csv")




if __name__ == "__main__":
  inst = steel_eye_data_push()
  inst.get_xml_file()
  print(inst.convert_into_csv())
  #inst.push_to_bucket() #since i dont have an account in aws cloud i am omiting this step