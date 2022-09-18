import argparse    # 1. argparseをインポート
import shutil
import glob
from pprint import pprint
import os
from threading import stack_size
import bs4
import json
import pandas
from pandas import json_normalize

class ArchiveMaticaOmeka:
  TMP_DIR_PATH = f"tmp"

  def __init__(self, dip_zip_file_path, mapping_json_file_path, task_id="test", debug=False):
    self.dip_zip_file_path = dip_zip_file_path
    mapping_json_file_path = mapping_json_file_path

    with open(mapping_json_file_path) as f:
      self.mapping = json.load(f)

    self.task_id = task_id

    tmp_dir_path = f"{self.TMP_DIR_PATH}/{task_id}"

    self.tmp_dir_path = tmp_dir_path

    if os.path.exists(tmp_dir_path):
      shutil.rmtree(tmp_dir_path)

    self.debug = debug

  def unpackArchive(self):
    """zipファイルの展開
    """

    shutil.unpack_archive(self.dip_zip_file_path, self.tmp_dir_path)

  ## 
  def getMetsFilePath(self):
    """metsファイルのパスの取得
    """

    files = glob.glob(f"{self.tmp_dir_path}/*/*.xml")
    # pprint(files)

    mets_path = None
    for file in files:
      if "METS" in file:
        mets_path = file

    # print(mets_path)
    self.mets_path = mets_path

    # スクレイピング対象のhtmlファイルからsoupを作成
    soup = bs4.BeautifulSoup(open(self.mets_path), 'xml')
    self.soup = soup

    if self.debug:
      print("-----\ngetMetsFilePath\n-----")
      print(soup.prettify())

  ## dmdSecの情報の取得
  def getDmdSec(self):
    """dmdSecの情報の取得
    """
    ## metsファイルのパスの取得

    mapping = self.mapping

    dmdSecs = self.soup.find_all("mets:dmdSec")

    items = {}
    item_ids = []
    mappings = {}

    for dmdSec in dmdSecs:
      dmdSec_id = dmdSec.get("ID")
      mdWrap = dmdSec.find("mets:mdWrap")
      mdType = mdWrap.get("MDTYPE")
      # print(mdType)

      if mdType == "DC":
        item = {}
        # items.append(item)
        dc = mdWrap.find("dcterms:dublincore")
        metadata = dc.findChildren()
        # pprint(metadata)

        for m in metadata:
          name = "dc:" + m.name
          value = m.text
          # print(name, value)

          if value != "" and name in mapping:
            p_id = mapping[name]
            

            if p_id == "dcterms:identifier":
              item_ids.append(value)
              id = value
            else:
              item[p_id] = value

        items[id] = item

        mappings[dmdSec_id] = id

      if mdType == "OTHER":
        # item = {}
        # items.append(item)
        dc = mdWrap.find("mets:xmlData")
        metadata = dc.findChildren()
        # pprint(metadata)

        for m in metadata:
          name = m.name
          value = m.text
          # print(name, value)

          if value != "" and name in mapping:
            p_id = mapping[name]
            
            item[p_id] = value

        items[id] = item

    # pprint(items)

    self.items = items
    self.item_ids = item_ids
    self.dmd_sec_mappings = mappings

    if self.debug:
      print("-----\nmappings\n-----")
      pprint(mappings)

    if self.debug:
      print("-----\ngetDmdSec\n-----")
      pprint(items)

  ## 
  def getStructMap(self):
    """structMapの情報の取得
    """

    item_ids = self.item_ids

    dmd_sec_mappings = self.dmd_sec_mappings

    # pprint(dmd_sec_mappings)

    # print("-----\nitem_ids\n-----")
    # pprint(item_ids)

    structMap = self.soup.find("mets:structMap")
    divs = structMap.find_all("mets:div")

    structs = []
    file_ids = []

    for div in divs:
      '''
      # print(div)
      id = div.get("LABEL")
      pid = div.parent.get("LABEL")
      # print(id, pid)
      
      struct = {
          "id": id,
          "parent": pid
      }
      '''

      pid = div.parent.get("LABEL")

      struct = {
        "id": div.get("LABEL"),
        "parent": pid
      }

      DMDID = div.get("DMDID")

      if DMDID is None:
        continue

      # print("DMDID", DMDID)

      DMDIDS = DMDID.split(" ")

      for DMDID in DMDIDS:
        if DMDID in dmd_sec_mappings:
          # print("*", DMDID)
          struct["item_id"] = dmd_sec_mappings[DMDID]

          fptrs = div.find_all("mets:fptr")

          # print("fptrs", fptrs)

          for fptr in fptrs:
            FILEID = fptr.get("FILEID")
            if FILEID is None:
              continue
            struct["file"] = FILEID
            file_ids.append(FILEID)

            structs.append(struct)


      '''
      print("id", id, "item_ids", item_ids, "pid", pid, "flg", id not in item_ids and pid not in item_ids)

      if id not in item_ids and pid not in item_ids:
        continue
      structs.append(struct)

      if div.has_attr("TYPE") and div.get("TYPE") == "Item":
        fptr = div.find("mets:fptr").get("FILEID")
        # print("fptr", fptr)
        if fptr:
          struct["file"] = fptr
          file_ids.append(fptr)

      # print("-----")

      '''

    # pprint(structs)

    self.structs = structs
    self.file_ids = file_ids

    if self.debug:
      print("-----\nstructs\n-----")
      pprint(structs)

      print("-----\nfile_ids\n-----")
      pprint(file_ids)
    
    # df = json_normalize(structs)

  def getStructMap2(self):
    """structMapの情報の取得
    """

    item_ids = self.item_ids

    dmd_sec_mappings = self.dmd_sec_mappings

    pprint(dmd_sec_mappings)

    print("-----\nitem_ids\n-----")
    pprint(item_ids)

    structMap = self.soup.find("mets:structMap")
    divs = structMap.find_all("mets:div")

    structs = []
    file_ids = []

    for div in divs:
      # print(div)
      id = div.get("LABEL")
      pid = div.parent.get("LABEL")
      # print(id, pid)
      
      struct = {
          "id": id,
          "parent": pid
      }

      DMDID = div.get("DMDID")

      print("DMDID", DMDID)

      DMDIDS = DMDID.split(" ")

      print("id", id, "item_ids", item_ids, "pid", pid, "flg", id not in item_ids and pid not in item_ids)

      if id not in item_ids and pid not in item_ids:
        continue
      structs.append(struct)

      if div.has_attr("TYPE") and div.get("TYPE") == "Item":
        fptr = div.find("mets:fptr").get("FILEID")
        # print("fptr", fptr)
        if fptr:
          struct["file"] = fptr
          file_ids.append(fptr)

      # print("-----")

    # pprint(structs)

    self.structs = structs
    self.file_ids = file_ids

    if self.debug:
      print("-----\nstructs\n-----")
      pprint(structs)

      print("-----\nfile_ids\n-----")
      pprint(file_ids)
    
    # df = json_normalize(structs)

  def getFileSec(self):
    """fileSecの情報の取得
    """

    ## mets:fileGrpの情報の取得

    fileSec = self.soup.find_all("mets:fileGrp")[0]

    files = fileSec.find_all("mets:file")

    file_map = {}

    for file in files:
      file_id = file.get("ID")
      # print(file_id)
      flocat = file.find("mets:FLocat").get("xlink:href")
      # print(flocat)
      file_map[file_id] = flocat

    if self.debug:
      print("-----\nfile_map\n-----")
      pprint(file_map)

    self.file_map = file_map

  def createOmeka(self):
    """Omekaへの登録用のフォーマットに変換
    """
    ## 

    file_map = self.file_map
    structs = self.structs
    items = self.items
    tmp_dir_path = self.tmp_dir_path

    ###

    params = []
    rows = []
    id_exists = []

    medias = []

    for struct in structs:

      row = {}

      item_payload = {
          "@type": "o:Item",
          
      }

      params.append({
          "data": item_payload,
          "files": []
      })

      # struct: 構造情報を持つ
      id = struct["parent"]

      item_id = struct["item_id"]

      if item_id in id_exists:
        continue

      id_exists.append(item_id)

      if "file" in struct:

        file_id = struct["file"]

        if file_id not in file_map:
          continue

        path = "objects/" +  file_id.replace("file-", "") + "-" + file_map[file_id].split("/")[-1]

        medias.append({
            "item": item_id, # id,
            "path": path
        })

        # continue

      rows.append(row)

      row["dcterms:identifier"] = item_id

      item = items[item_id]

      for pid in item:
        item_payload["p{}".format(pid)] = [
            {
                "property_id": pid, 
                "@value": item[pid], 
                "type" : "literal"
            }
        ]

        row[pid] = item[pid]

      if id != "objects":
        row["dcterms:isPartOf ^^resource"] = id

    from pandas import json_normalize
    df = json_normalize(rows)

    df.to_csv(f'{tmp_dir_path}/metadata.csv', index=False)

    from pandas import json_normalize
    df_m = json_normalize(medias)
    df_m

    # print(df_m)

    df_m.to_csv(f'{tmp_dir_path}/media.csv', index=False)

  def createOmeka2(self):
    """Omekaへの登録用のフォーマットに変換
    """
    ## 

    file_map = self.file_map
    structs = self.structs
    items = self.items
    tmp_dir_path = self.tmp_dir_path

    ###

    params = []
    rows = []
    id_exists = []

    medias = []

    for struct in structs:
      row = {}
      

      item_payload = {
          "@type": "o:Item",          
      }

      params.append({
          "data": item_payload,
          "files": []
      })

      id = struct["parent"]

      

      item_id = struct["id"]

      if item_id in id_exists:
        continue

      id_exists.append(item_id)

      if "file" in struct:

        file_id = struct["file"]

        if file_id not in file_map:
          continue
        path = "objects/" +  file_id.replace("file-", "") + "-" + file_map[file_id].split("/")[-1]

        medias.append({
            "item": id,
            "path": path
        })

        continue

      rows.append(row)

      row["dcterms:identifier"] = item_id

      item = items[item_id]

      for pid in item:
        item_payload["p{}".format(pid)] = [
            {
                "property_id": pid, 
                "@value": item[pid], 
                "type" : "literal"
            }
        ]

        row[pid] = item[pid]

      if id != "objects":
        row["dcterms:isPartOf ^^resource"] = id

    from pandas import json_normalize
    df = json_normalize(rows)

    df.to_csv(f'{tmp_dir_path}/metadata.csv', index=False)

    from pandas import json_normalize
    df_m = json_normalize(medias)
    df_m

    df_m.to_csv(f'{tmp_dir_path}/media.csv', index=False)

  def moveObjects(self):
    """Omekaへの登録用のフォーマットに変換
    """
    ## 
    path = glob.glob(f"{self.tmp_dir_path}/*/objects")[0]
    shutil.move(path, f"{self.tmp_dir_path}")

  @staticmethod
  def main(dip_zip_file_path, mapping_json_file_path, task_id="bcd"):
    archiveMaticaOmeka = ArchiveMaticaOmeka(dip_zip_file_path, mapping_json_file_path,task_id, debug=False)
    archiveMaticaOmeka.unpackArchive()
    archiveMaticaOmeka.getMetsFilePath()
    archiveMaticaOmeka.getDmdSec()
    archiveMaticaOmeka.getStructMap()
    archiveMaticaOmeka.getFileSec()
    archiveMaticaOmeka.createOmeka()
    archiveMaticaOmeka.moveObjects()

from convert import ArchiveMaticaOmeka

parser = argparse.ArgumentParser()    # 2. パーサを作る

# 3. parser.add_argumentで受け取る引数を追加していく
parser.add_argument('dip_zip_file_path', help='path to dip zip file')    # 必須の引数を追加
parser.add_argument('mapping_json_file_path', help='path to mapping json file')    # 必須の引数を追加
parser.add_argument('-tid', '--task_id', default="bcd")
# parser.add_argument('arg2', help='foooo')
# parser.add_argument('--arg3')    # オプション引数（指定しなくても良い引数）を追加
# parser.add_argument('-a', '--arg4')   # よく使う引数なら省略形があると使う時に便利

args = parser.parse_args()

dip_zip_file_path = args.dip_zip_file_path
mapping_json_file_path = args.mapping_json_file_path
task_id = args.task_id

ArchiveMaticaOmeka.main(dip_zip_file_path, mapping_json_file_path, task_id=task_id)





