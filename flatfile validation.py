# import os
import json
import time
from pathlib import Path
import filecmp
import difflib
from difflib import Differ
import itertools
with open("Downloads/input_parm.json", 'rb') as file:
      # Load its content and make a new dictionary
    data = json.load(file)
    length =len(data[config])
    print(length)
#f = open('Downloads/abc.json',)
#data = json.load(f)
for i in range(0,length):
      print("Sr.no==>",i)
      download= (data["config"][i]["download"])
      spath= (data["config"][i]["spath"])
      table_name= (data["config"][i]["table_name"])
      dbname= (data["config"][i]["dbname"])
      src_sys_inst_id= (data["config"][i]["src_sys_inst_id"])
      business_dates3= (data["config"][i]["business_dates3"])
      businessdateedi= (data["config"][i]["businessdateedi"])
      edipath = (data["config"][i]["edipath"])
      file_name= (data["config"][i]["file_name"])

      S3file = os.path.join(download,spath,dbname,table_name,src_sys_inst_id,business_dates3,file_name)
      chg_dir_str_s3=S3file.replace("\\","/")
      p = Path(chg_dir_str_s3)
      #p.open(encoding='ISO-8859-1").read()
      with p.open(encoding='ISO-8859-1") as f:
          line_counts3 = 0
          for line in f:
              if line != "\n":
                  line_counts3 += 1
      print("Table Name ==>","######",table_name,"#####")
      print("S3filerowcount==>",line_counts3)
      #time.sleep(5)
      edifile = os.path.join(download,edipath,businessdateedi,file_name)
      chg_dir_str_edi=edifile.replace("\\","/")
      #print(changethedirstructure)
      x = Path(chg_dir_str_edi)
      #p.open().read()
      with x.open(encoding='ISO-8859-1") as ff:
          line_countedi = 0
          for linee in ff:
              if linee != "\n":
                  line_countedi += 1
      print("EDIfilerowcount==>",line_countedi)
      if line_counts3 == line_countedi:
            print("S3 objectstore file row count is matching with EDI file rowcount",line_counts3,"=",line_countedi,"===>PASSED")
      else:
            print("S3 objectstore file row count is not matching with EDI file rowcount",line_counts3,"!=",line_countedi,"===>FAILED")

      s3checksum =os.stat(p).st_size
      edichecksum =os.stat(x).st_size
      print("S3 Objectstore file Checksum ==>",s3checksum)
      print("EDI file Checksum ==>",edichecksum)
      if s3checksum == edichecksum:
          print ("EDI Checksum is matching with S3 ==>",edichecksum,",",s3checksum,"******NO ACTION==>PASSED******")
      else:
          print ("EDI Checksum is Not matching with S3 ==>",edichecksum,",",s3checksum,"******ACTION REQUIRED==>FAILED******")
      datavalidation = filecmp.cmp(x,p,shallow = False)
      print("Datavalidation between EDI and S3 object store is",datavalidation)
      with x.open(encoding='ISO-8859-1") as f1, p.open(encoding='ISO-8859-1") as f2:
            for lineno, (line1, line2) in enumerate(zip(f1, f2), 1):
                  if line1 != line2:
                      print ('mismatch in line no:', lineno)
print ("====================================================================================")
