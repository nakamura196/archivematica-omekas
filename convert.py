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
    # 順序を保持するためにlistを使用
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
            
            # print("*", p_id)

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
      print("-----\nmappings\ndmdSecとdcterms:identifierの関係\n-----")
      pprint(mappings)

    if self.debug:
      print("-----\ngetDmdSec\ndcterms:identifier毎のメタデータ\n-----")
      pprint(items)

  ## 
  def getStructMap(self):
    """structMapの情報の取得
    """

    dmd_sec_mappings = self.dmd_sec_mappings

    fptrs = self.soup.find_all("mets:fptr")

    structs = []

    for fptr in fptrs:
      file_id = fptr.get("FILEID")

      div_item = fptr.parent

      # フラット
      if "DMDID" in div_item.attrs:
        dmd_ids = div_item.get("DMDID").split(" ")
        for dmd_id in dmd_ids:
          if dmd_id not in dmd_sec_mappings:
            continue

          item_id = dmd_sec_mappings[dmd_id]
      else:


        label_item = div_item.get("LABEL")

        div_item_parent = div_item.parent

        if label_item in ["metadata.csv", "METS.xml", "directory_tree.txt"]:
          continue

        item_id = div_item_parent.get("LABEL")

        if item_id in ["OCRfiles"]:
          continue

      structs.append({
        "file": file_id,
        "item_id": item_id  # label_item_parent # label_item
      })

    hie = {}

    oDiv = self.soup.find("mets:div", {"LABEL": "objects"})

    for div in oDiv.find_all("mets:div"):
      label = div.get("LABEL")
      parent = div.parent
      pid = parent.get("LABEL")
      hie[label] = pid

    self.structs = structs

    self.hie = hie

    if self.debug:
      print("-----\nstructs\n-----")
      pprint(structs)

    if self.debug:
      print("-----\nhie\n-----")
      pprint(hie)

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

    hie = self.hie

    ###

    # params = []
    # rows = []
    # id_exists = []

    # メディア

    medias = []

    for struct in structs:

      # struct: 構造情報を持つ

      item_id = struct["item_id"]

      if "file" in struct:

        file_id = struct["file"]

        # print(file_id, file_id in file_map)

        if file_id not in file_map:
          continue

        path = "objects/" +  file_id.replace("file-", "") + "-" + file_map[file_id].split("/")[-1]

        medias.append({
            "item": item_id, # id,
            "path": path
        })

    item_ids = self.item_ids

    rows = []

    # from copy import deepcopy

    for item_id in item_ids:
      row = items[item_id]
      row["dcterms:identifier"] = item_id
      if item_id in hie:
        row["dcterms:isPartOf ^^resource"] = hie[item_id]
      rows.append(row)

    

    from pandas import json_normalize
    df = json_normalize(rows)

    df.to_csv(f'{tmp_dir_path}/metadata.csv', index=False)

    from pandas import json_normalize
    df_m = json_normalize(medias)
    df_m

    # print(df_m)

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
    
    # print("a", "b")
    
    archiveMaticaOmeka.getDmdSec()
    archiveMaticaOmeka.getStructMap()
    archiveMaticaOmeka.getFileSec()
    archiveMaticaOmeka.createOmeka()
    archiveMaticaOmeka.moveObjects()

if __name__ == "__main__":

  from convert import ArchiveMaticaOmeka

  parser = argparse.ArgumentParser()    # 2. パーサを作る

  # 3. parser.add_argumentで受け取る引数を追加していく
  parser.add_argument('dip_zip_file_path', help='path to dip zip file')    # 必須の引数を追加
  parser.add_argument('mapping_json_file_path', help='path to mapping json file')    # 必須の引数を追加
  parser.add_argument('-tid', '--task_id', default="bcd")

  args = parser.parse_args()

  dip_zip_file_path = args.dip_zip_file_path
  mapping_json_file_path = args.mapping_json_file_path
  task_id = args.task_id

  ArchiveMaticaOmeka.main(dip_zip_file_path, mapping_json_file_path, task_id=task_id)





