import linecache
from xml.etree import ElementTree
import wget
import os
import json
import xml
import xmlschema
import jsonschema
from jsonschema import validate,Draft7Validator,Draft202012Validator
from dateutil.parser import parse
from lxml import etree
from datetime import datetime
import pandas as pd
import xmltodict
import dict2xml
import re
import logging
import shutil
pd.options.mode.chained_assignment = None


class SourceProcess:
    
    columns_marche_2022 = [
        'techniques',
        'dateNotification',
        'modalitesExecution',
        'considerationsSociales',
        'considerationsEnvironnementales',
        'marcheInnovant',
        'origineUE',
        'origineFrance',
        'ccag',
        'offresRecues',
        'typesPrix',
        'attributionAvance']
    
    columns_concession_2022 = [
        'dateDebutExecution',
        'considerationsSociales',
        'considerationsEnvironnementales']
    
    """La classe SourceProcess est une classe abstraite qui sert de parent à chaque classe enfant de
    sources. Elle sert à définir le cas général des étapes de traitement d'une source : création des
    variables de classe (__init__), nettoyage des dossiers de la source (_clean_metadata_folder),
    récupération des URLs (_url_init), get, convert et fix."""
    def __init__(self, key,data_format):
        """L'étape __init__ crée les variables associées à la classe SourceProcess : key, source,
        format, df, file_name, url, cle_api et metadata."""
        logging.info("  ÉTAPE INIT")
        self.key = key
        self.data_format = data_format
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.url_source = self.metadata[self.key]["url_source"]
        self.liste_2022 = []
        self.liste_2019 = []
        self.df = pd.DataFrame()
        # Lavage des dossiers de la source
        self._clean_metadata_folder()

        # Récupération des urls
        self._url_init()

        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]

    def _clean_metadata_folder(self):
        """La fonction _clean_metadata_folder permet le nettoyage de /metadata/{self.source}"""
        # Lavage des dossiers dans metadata
        logging.info(f"Début du nettoyage de metadata/{self.source}")
        if os.path.exists(f"metadata/{self.source}"):
            shutil.rmtree(f"metadata/{self.source}")
        logging.info(f"Nettoyage metadata/{self.source} OK")

    def _url_init(self):
        """_url_init permet la récupération de l'ensemble des url des fichiers qui doivent être
        téléchargés pour une source. Ces url sont conservés dans self.metadata, le dictionnaire
        correspondant à la source."""
        logging.info("Début de la récupération de la liste des url")
        os.makedirs(f"metadata/{self.source}", exist_ok=True)
        os.makedirs(f"old_metadata/{self.source}", exist_ok=True)
        self.cle_api = self.metadata[self.key]["cle_api"]
        url = []
        if self.cle_api==[]:
            url = url + [self.url_source]
    
        for i in range(len(self.cle_api)):
            #Téléchargement du fichier de metadata de self.source et création de la 1ere variable json pour la comparaison 
            wget.download(f"https://www.data.gouv.fr/api/1/datasets/{self.cle_api[i]}/",
                          f"metadata/{self.source}/metadata_{self.key}_{i}.json")
            with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r+') as f:
                ref_json = json.load(f)
            ressources = ref_json["resources"]

            #Creation de la deuxième variable json pour la comparaison si le fichier old_metadata existe
            if os.path.exists(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json"):
                with open(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json", 'r+') as fl:
                    print("ouverture fichier")
                    refjson = json.load(fl)
                old_ressources = refjson["resources"]
                print("affectation faite")
            else:
                old_ressources = []
            
            #Comparaison et modificiation de la variable url
            if old_ressources==[]:
                url = url + [d["url"] for d in ressources if
                         (d["url"].endswith("xml") or d["url"].endswith("json"))]
            else: 
                url = self.check_date_file(url,ressources, old_ressources)
                print("Les urls dont le contenu a été modifié sont: ", url)

            #Cas où les fichiers old_metadata existent, on écrit dedans à nouveau
            if os.path.exists(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json"):
                with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r') as source_file:
                    contenu = source_file.read()
                    print("lecture")
                with open(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json", 'w') as destination_file:
                    destination_file.write(contenu)
                    print("ecriture")
            #Cas où les fichiers old_metadata n'existent pas, on fait une copie
            else:
                shutil.copy(f"metadata/{self.source}/metadata_{self.key}_{i}.json",f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json")
                print(os.listdir(f"old_metadata/{self.source}"))

        self.metadata[self.key]["url"] = url
        self.url = self.metadata[self.key]["url"]
        logging.info("Récupération des url OK")

    def check_date_file(self,url, new_ressources,old_ressources):
        """Fonction vérifiant si la date de dernière modification des fichiers ressources DANS les metadatas est strictement
           antérieure à la date de dernière modification """
        for i in range(len(new_ressources)):
            if new_ressources[i]["last_modified"]>old_ressources[i]["last_modified"] and (new_ressources[i]["url"].endswith("xml") or new_ressources[i]["url"].endswith("json")) :
                print(new_ressources[i])
                url = url + [new_ressources[i]["url"]] 
        return url

    def get(self):
        """Étape get qui permet le lavage du dossier sources/{self.source} et la récupération de
        l'ensemble des fichiers présents sur chaque url."""
        logging.info("  ÉTAPE GET")
        # Lavage des dossiers dans "sources"   
        logging.info(f"Début du nettoyage de sources/{self.source}")
        if os.path.exists(f"sources/{self.source}"):
            shutil.rmtree(f"sources/{self.source}")
        logging.info(f"Nettoyage sources/{self.source} OK")
        # Étape get des url
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s)")
        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        print("SELF.URL:" ,self.url)
        for i in range(len(self.url)):
            try:
                wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")
            except:
                logging.error("Problème de téléchargement du fichier ", self.url[i])
        logging.info(f"Téléchargement : {len(self.url)} fichier(s) OK")

    def _add_column_type(self, df: pd.DataFrame, default_type_name:str = None):
        if self.data_format=='2022' and not "_type" in df.columns and (default_type_name or "nature" in df.columns):
            if default_type_name:
                df['_type'] = default_type_name
            else:
                df['_type'] = df["nature"].apply(lambda x: "Marché" if x=="Marché" else "Concession")

    def _has_all_field_and_date_2024(self, record, record_type:str = None):
        if record_type:
            if record_type == 'marche':
                for column in self.columns_marche_2022:
                    if not column in record:
                        return False
                if not self.date_after_2024(record,'dateNotification','datePublicationDonnees'):
                    return False 
            else:
                for column in self.columns_concession_2022:
                    if not column in record:
                        return False
                if not self.date_after_2024(record,'dateDebutExecution','datePublicationDonnees'):
                    return False 
        else:
            if 'nature' in record:
                if 'concession' in record['nature'].lower():
                    for column in self.columns_concession_2022:
                        if not column in record:
                            return False
                    if not self.date_after_2024(record,'dateDebutExecution','datePublicationDonnees'):
                        return False 
                else:
                    if record['id']=="24992505":
                        print ('debug')
                    for column in self.columns_marche_2022:
                        if not column in record:
                            return False
                    if not self.date_after_2024(record,'dateNotification','datePublicationDonnees'):
                        return False 
            else:
                return False
        return True
    
    def date_norm(self,datestr:str):
        return datestr.replace('+','-') if datestr else datestr

    def date_before_2024(self, record):
        if 'nature' in record:
            if not record['nature'] is None  and 'concession' in record['nature'].lower():
                if 'dateDebutExecution' in record:
                    if record['dateDebutExecution'] and self.date_norm(record['dateDebutExecution'])<'2024-01-01':
                        return True
                if 'datedebutexecution' in record:
                    if record['datedebutexecution'] and self.date_norm(record['datedebutexecution'])<'2024-01-01':
                        return True
            else:
                if 'dateNotification' in record:
                    if record['dateNotification'] and self.date_norm(record['dateNotification'])<'2024-01-01':
                        return True
                if 'datenotification' in record:
                    if record['datenotification'] and self.date_norm(record['datenotification'])<'2024-01-01':
                        return True
        else:
            if 'dateNotification' in record:
                if record['dateNotification'] and self.date_norm(record['dateNotification'])<'2024-01-01':
                    return True
            if 'datenotification' in record:
                if record['datenotification'] and self.date_norm(record['datenotification'])<'2024-01-01':
                    return True
        return False
    
    def date_after_2024(self, record, col_date, col_date_publication):
        first = datetime.strptime("2024-01-01", "%Y-%m-%d")
        pattern = r'20[0-9]{2}-[0-1]{1}[0-9]{1}-[0-9]{2}'
        col_list = [col_date,col_date_publication]
        for col in col_list:
            if col in record and record[col] and re.match(pattern,record[col]):
                try:
                    date = datetime.strptime(record[col], "%Y-%m-%d")
                    if date>=first:
                        if col == col_date_publication:
                            record[col_date] = record[col_date_publication]
                        return True
                except:
                    None
        pattern = r'20[0-9]{2}/[0-1]{1}[0-9]{1}/[0-9]{2}'
        for col in col_list:
            if col in record and record[col] and re.match(pattern,record[col]):
                try:
                    tmp = record[col]
                    date =  datetime.strptime(tmp, '%Y/%m/%d').date()
                    if date>=first:
                        record[col] = tmp
                        if col == col_date_publication:
                            record[col_date] = record[col_date_publication] 
                        return True
                except:
                    None
        return False

    def _retain_with_format(self, dico:dict, file_name:str):
        #logging.info(f"Content check for {file_name}")
        dico_ignored = []
        if self.data_format=='2022':
            #self.df = self.df[~(((~self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateNotification']<'2024-01-01') |
            #                 ((self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateDebutExecution']<'2024-01-01'))))]
            if self.format=='xml':
                if 'marches' in dico:
                    if 'marche' in dico['marches']:
                        dico_ignored_marche = []
                        for m in dico['marches']['marche']:
                            if not self._has_all_field_and_date_2024(m, 'marche'):
                                dico_ignored_marche.append(m)
                        if len(dico_ignored_marche)>0:
                            for i in dico_ignored_marche:
                                dico['marches']['marche'].remove(i)
                                #del ['marches']['marche'][i]
                                dico_ignored.append(i)
                    if  'contrat-concession' in dico['marches']:
                        dico_ignored_concession = []
                        for m in dico['marches']['contrat-concession']:
                            if not self._has_all_field_and_date_2024(m, 'contrat-concession'):
                                dico_ignored_concession.append(m)
                        if len(dico_ignored_concession)>0:
                            for i in dico_ignored_concession:
                                del dico['marches']['contrat-concession'][i]
                                #dico['marches']['contrat-concession'].remove(i)
                                dico_ignored.append(i)
                else:
                    logging.error("Balise 'marches' inexistante")
                    dico.clear()
            else:    
                if 'marches' in dico:
                    if 'marche' in dico['marches']:
                        for m in dico['marches']['marche']:
                            #if 'dateNotification' in m and m['dateNotification']=='2024-11-24+01:00':
                            #    print('debug')
                            if not self._has_all_field_and_date_2024(m):
                                dico_ignored.append(m)
                        if len(dico_ignored)>0:
                            for i in dico_ignored:
                                #print(dico['marches']['marche'])
                                dico['marches']['marche'].remove(i)
                                #del dico['marches']['marche'][i]
                    if 'contrat-concession' in dico['marches']:
                        for m in dico['marches']['contrat-concession']:
                            #if 'dateNotification' in m and m['dateNotification']=='2024-11-24+01:00':
                            #    print('debug')
                            if not self._has_all_field_and_date_2024(m):
                                dico_ignored.append(m)
                        if len(dico_ignored)>0:
                            for i in dico_ignored:
                                #del dico['marches']['marche'][i]
                                dico['marches']['marche'].remove(i)
                else:
                    logging.error("Balise 'marches' inexistante")
                    dico.clear()
        else:
            #df_ignored = self.df[(((~self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateNotification']>='2024-01-01') |
            #                 ((self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateDebutExecution']>='2024-01-01'))))]
            #self.df = self.df[~(((~self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateNotification']>='2024-01-01') |
            #                 ((self.df['nature'].str.contains('concession', case=False, na=False)) & (self.df['dateDebutExecution']>='2024-01-01'))))]
            if self.format=='xml':
                if 'marches' in dico:
                    for record in dico['marches']['marche']:
                        if not self.date_before_2024(record):
                            dico_ignored.append(record)
                    if len(dico_ignored)>0:
                        for i in dico_ignored:
                            if (type(dico['marches']['marche'])=='dict'):
                                dico['marches']['marche'].pop(i)
                            else:
                                dico['marches']['marche'].remove(i)
                else:
                    for record in dico:
                        if not self.date_before_2024(record):
                            dico_ignored.append(record)
                    if len(dico_ignored)>0:
                        for i in dico_ignored:
                            dico.remove(i)
            else:
                if 'marches' in dico:
                    for record in dico['marches']:
                        if not self.date_before_2024(record):
                            dico_ignored.append(record)
                    if len(dico_ignored)>0:
                        for i in dico_ignored:
                            #print( dico['marches'])
                            del dico['marches'][i]
    
                else:
                    for record in dico:
                        if '2018t6Mbc-_qMg00'==record['id']:
                            print(record['id'])
                        if not self.date_before_2024(record):
                            dico_ignored.append(record)
                    if len(dico_ignored)>0:
                        for i in dico_ignored:
                            del dico[i]
                            #dico.remove(i)

        if len(dico_ignored)>0:
            logging.info(f"Ignored {len(dico_ignored)} record(s) in {file_name}")
            #if self.format=='xml':
            #    with open(file_name, 'w') as f:
            #        f.write(dict2xml.dict2xml(dico_ignored))
            #else:
            #    with open(file_name, 'w') as f:
            #        f.write(dict2xml.dict2xml(dico_ignored))
            ##del dico_ignored
        return dico

    def convert(self):
        """Étape de conversion des fichiers qui supprime les ' et concatène les fichiers présents
        dans {self.source} dans un seul DataFrame"""
        logging.info("  ÉTAPE CONVERT")
        # suppression des '
        count = 0
        repertoire_source = f"sources/{self.source}"
        for path in os.listdir(repertoire_source):
            if os.path.isfile(os.path.join(repertoire_source, path)):
                count += 1
        for i in range(count):
           
            file_path = f"sources/{self.source}/{self.file_name[i]}.{self.format}"
            #if file_path=='sources/ternum-bfc/ternum-bfc_20.xml':
            #    print('ERROR')
            #if file_path=='sources/data.gouv.fr_aife/data.gouv.fr_aife_575.xml':
            #    print('ERROR')
            file_exist = os.path.exists(file_path)
            if not file_exist:
                logging.warning(f"Le fichier {file_path} n existe pas.")

        if count != len(self.url):
            logging.warning("Nombre de fichiers en local inégal au nombre d'url trouvé")
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(count):
                #if i==575:
                #    print('ERROR')
                if self.data_format=='2022':
                    if not self.check(None,f"sources/{self.source}/{self.file_name[i]}.{self.format}"):
                        print (self.file_name[i])
                        logging.warning(f"sources/{self.source}/{self.file_name[i]}.{self.format} not a valide xml")
                        continue
                        #raise Exception(f"sources/{self.source}/{self.file_name[i]}.{self.format} not a valide xml")
                try:
                    with open(f"sources/{self.source}/{self.file_name[i]}.{self.format}", encoding='utf-8') as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
 
                    if dico['marches'] is not None:
                        self._retain_with_format(dico,f"sources/{self.source}/{self.file_name[i]}_{self.data_format}_ignored.{self.format}")
                        
                        if 'marches' in dico:
                            # Add marchés
                            if 'marche' in dico['marches']:   #à chaque noueau marché, on ajoute une colonne, qui sera utilisé par un dataframe. Ce dataframe sera ajouté dans la liste li
                                df = pd.DataFrame.from_dict(dico['marches']['marche'])
                                self._add_column_type(df,"Marché")
                                li.append(df)
                                ##del df

                            # Add Concession
                            if self.data_format=='2022' and 'contrat-concession' in dico['marches']:
                                df = pd.DataFrame.from_dict(dico['marches']['contrat-concession'])
                                self._add_column_type(df,"Concession")
                                li.append(df)
                                ##del df
                        ##del dico
                    else:  # cas presque null
                        logging.warning(f"Le fichier {self.file_name[i]} est vide, il est ignoré")
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier xml {self.file_name[i]} - {err}")

            if len(li) != 0:
                df = pd.concat(li)
                ##del li
                df = df.reset_index(drop=True)
            else:
                # create empty dataframe
                df = pd.DataFrame()
            self.df = df
        elif self.format == 'json':
            li = []
            for i in range(count):
                try:
                    with open(
                            f"sources/{self.source}/{self.file_name[i]}.{self.format}", encoding="utf-8") as json_file:
                        
                        #check for format compliance (only for data_format 2022)
                        if self.data_format=='2022':
                            if not self.check(json_file,None):
                                logging.warning(f"sources/{self.source}/{self.file_name[i]}.{self.format} not a valid json")
                                raise Exception("Json format not valid")
                        
                            dico = json.load(json_file)
                            self._retain_with_format(dico,f"sources/{self.source}/{self.file_name[i]}_{self.data_format}_ignored.{self.format}")

                            
                            if 'marches' in dico:
                                # Add marchés
                                if 'marche' in dico['marches']:
                                    df = pd.DataFrame.from_dict(dico['marches']['marche'])
                                    self._add_column_type(df,"Marché")
                                    li.append(df)
                                    ##del df

                                # Add Concession
                                if self.data_format=='2022' and 'contrat-concession' in dico['marches']:
                                    df = pd.DataFrame.from_dict(dico['marches']['contrat-concession'])
                                    self._add_column_type(df,"Concession")
                                    li.append(df)
                                    ##del df
                        else:
                            dico = json.load(json_file)
                            self._retain_with_format(dico,f"sources/{self.source}/{self.file_name[i]}_{self.data_format}_ignored.{self.format}")
                            df = pd.DataFrame.from_dict(dico['marches'])
                            self._add_column_type(df)
                            li.append(df)
                            ##del df
                        ##del dico
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier json {self.file_name[i]} - {err}")
            df = pd.concat(li)
            ##del li
            df = df.reset_index(drop=True)
            self.df = df
        logging.info("Conversion OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")

    def validateJson(self,jsonData,jsonScheme):
        try:
            #Draft7Validator.check_schema(jsonScheme)
            #Draft202012Validator.check_schema(jsonScheme)
            validate(instance=jsonData, schema=jsonScheme)
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Erreur de validation json - {err}")
            return False
        return True

    def validateXml(self, xml_path: str, xsd_path: str) -> bool:

        xml_schema_doc = etree.parse(xsd_path)
        xml_schema = etree.XMLSchema(xml_schema_doc)

        xml_doc = etree.parse(xml_path)

        try:
            result = xml_schema.validate(xml_doc)
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Erreur de validation xml - {err}")
            return False
        
        return result
    
    def check(self,jsonData,xml_path):
        if self.data_format=='2019':
            return True
        else:
            if self.format=='json':
                scheme_path = 'schemes/decp_2022.json'
                jsonScheme = json.load(open(scheme_path, "r",encoding='utf-8'))
                return self.validateJson(jsonData,jsonScheme)
            else:    # xml
                scheme_path = 'schemes/decp_2022.xsd'
                try:
                    with open(xml_path, 'r', encoding='utf-8') as xml_file:
                        xml_content = xml_file.read()
                    return xmlschema.validate(xml_content, scheme_path)==None
                except xmlschema.exceptions.XMLSchemaException as err:
                    logging.error(f"Erreur de validation xml - {err}")
                    return False
                #return self.validateXml(xml_path,scheme_path)
        #conform = False

        #all_columns_marche = ['procedure', 'modifications', 'nature', 'codecpv', 'considerationssociales',
        #'considerationsenvironnementales','dureemo is',
        #'datepublicationdonnees', 'titulaires', 'id', 'type', 'formeprix',
        #'datenotification', 'objet', 'montant', 'lieuexecution_code',
        #'lieuexecution_typecode', 'lieuexecution_nom', 'acheteur_id',
        #'acheteur_nom', 'datetransmissiondonneesetalab']

        #all_columns_concession = ['procedure', 'modifications', 'nature', 'codecpv', 'considerationssociales',
        #'considerationsenvironnementales','dureemois',
        #'datepublicationdonnees', 'titulaires', 'id', 'type', 'formeprix',
        #'datenotification', 'objet', 'montant', 'lieuexecution_code',
        #'lieuexecution_typecode', 'lieuexecution_nom', 'acheteur_id',
        #'acheteur_nom', 'datetransmissiondonneesetalab']

        #columns = [column.lower() for column in self.df.columns]

        #for col in all_columns_marche:
        #    if col not in columns:
        #        conform = False
        #        raise Exception("Format is not valid")
        #for col in all_columns_concession:
        #    if col not in columns:
        #        conform = False
        #        raise Exception("Format is not valid")
        #return conform

    def convert_boolean(self,col_name):
        #Conversion du type de la colonne en type str
        print("avant conversion", self.df.loc[:,col_name].dtype)
        self.df.loc[:,col_name].astype(str)
        print("apres conversion: ", self.df.loc[:,col_name].dtype)
        #self.df.loc[:,col_name].replace({True: 'True', False: 'False'})

        true_possibilites = [ 
        self.df[col_name].str.match(r'^(1)$', case=False, na=False),
        self.df[col_name].str.match(r'^(true)$', case=False, na=False)]

        false_possiblites = [
        self.df[col_name].str.match(r'^(0)$', case=False, na=False),
        self.df[col_name].str.match(r'^(false)$', case=False, na=False)
        ]

        for i in (true_possibilites):
            true_marcheInnovant = self.df.loc[:,col_name]==True    #récuperer les lignes valant "True"
            self.df.loc[true_marcheInnovant, col_name] = "oui"     #remplacement des "True" par "oui"
            
        for j in (false_possiblites):
            false_marcheInnovant = self.df.loc[:,col_name]==True  #récuperer les lignes valant "False"
            self.df.loc[false_marcheInnovant, col_name] = "non"



    # def convert_boolean(self,col_name):
    # #Vérification du type de la colonne "col_name". Le type doit être un string
    #     if self.df.loc[:,col_name].dtype=='O':
    #         true_marcheInnovant = self.df.loc[:,col_name].astype(str).str.match(r'^(1)$',case=False,na=False)
    #         self.df.loc[true_marcheInnovant, col_name] = "oui"
    #         true_marcheInnovant = self.df.loc[:,col_name].astype(str).str.match(r'^(true)$',case=False,na=False)
    #         self.df.loc[true_marcheInnovant, col_name] = "oui"

    #     true_marcheInnovant = self.df.loc[:,col_name].astype(str)==True
    #     self.df.loc[true_marcheInnovant, col_name] = "oui"

    #     if self.df.loc[:,col_name].dtype=='O':
    #         false_marcheInnovant = self.df.loc[:, col_name].astype(str).str.match(r'^(0)$',case=False,na=False) 
    #         self.df.loc[false_marcheInnovant, col_name] = "non"
    #         false_marcheInnovant = self.df.loc[:, col_name].astype(str).str.match(r'^(false)$',case=False,na=False) 
    #         self.df.loc[false_marcheInnovant, col_name] = "non"
    #     false_marcheInnovant = self.df.loc[:,col_name].astype(str)==False
    #     self.df.loc[false_marcheInnovant, col_name] = "non"

    def fix(self):
        """Étape fix qui crée la colonne source dans le DataFrame et qui supprime les doublons
        purs."""
        
        def get_id(x):
            if 'id' in x:
                return str(x["id"])
            else:
                return pd.NA
        
        logging.info("  ÉTAPE FIX")
        logging.info(f"Début de fix: Ajout source et suppression des doublons de {self.source}")
        # Ajout de source
        self.df = self.df.assign(source=self.source)
        # Transformation des acheteurs
        if self.data_format=='2022':
            if "acheteur" in self.df.columns:
                bool_nan_acheteur = ~self.df.loc[:, "acheteur"].isna()
                self.df.loc[bool_nan_acheteur, "acheteur.id"] = self.df.loc[bool_nan_acheteur, "acheteur"].apply(get_id)
                #with_acheteur =  ~pd.isna(self.df["acheteur"])
                #self.df[with_acheteur,"acheteur.id"]=self.df[with_acheteur,"acheteur"].apply(get_id)
                # Force type integer on column offresRecues
            if "offresRecues" in self.df.columns:
                self.df['offresRecues'] = self.df['offresRecues'].fillna(0).astype(int)
            if "marcheInnovant" in self.df.columns:
                print("TYPE COLONNE MARCHE INNOVANT:", self.df['marcheInnovant'].dtype)
                self.convert_boolean('marcheInnovant')
            if "attributionAvance" in self.df.columns:
                print("TYPE COLONNE ATTRIBUTION AVANCEE:", self.df['attributionAvance'].dtype)
                self.convert_boolean('attributionAvance')
            if "sousTraitanceDeclaree" in self.df.columns:
                print("TYPE COLONNE sous traitance:", self.df['sousTraitanceDeclaree'].dtype)
                self.convert_boolean('sousTraitanceDeclaree')
        else:
            if "acheteur" in self.df.columns:
                bool_nan_acheteur = ~self.df.loc[:, "acheteur"].isna()
                self.df.loc[bool_nan_acheteur, "acheteur.id"] = self.df.loc[bool_nan_acheteur, "acheteur"].apply(get_id)

        # Suppression des doublons
        df_str = self.df.astype(str)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        logging.info(f"Fix de {self.source} OK")
        logging.info(f"Nombre de marchés dans {self.source} après fix : {len(self.df)}")
    

    def mark_mandatory_field(self,df: pd.DataFrame,field_name:str) -> pd.DataFrame:
        if field_name in df.columns:
            empty_mandatory = ~pd.notna(df[field_name]) | pd.isnull(df[field_name])
            if not empty_mandatory.empty:
                df.loc[empty_mandatory,field_name] = 'MQ'
        return df

    def mark_optional_field(self,df: pd.DataFrame,field_name:str) -> pd.DataFrame:
        if field_name in df.columns:
            empty_optional  = ~pd.notna(df[field_name]) | pd.isnull(df[field_name])
            if not empty_optional.empty:
                df.loc[empty_optional,field_name] = 'CDL'
        return df

    #@compute_execution_time
    def marche_mark_fields(self,df: pd.DataFrame) -> pd.DataFrame:

        df = self.mark_mandatory_field(df,"id")
        df = self.mark_mandatory_field(df,"nature")
        df = self.mark_mandatory_field(df,"objet")
        df = self.mark_mandatory_field(df,"technique")
        df = self.mark_mandatory_field(df,"modaliteExecution")
        df = self.mark_mandatory_field(df,"codeCPV")
        df = self.mark_mandatory_field(df,"procedure")
        df = self.mark_mandatory_field(df,"dureeMois")
        df = self.mark_mandatory_field(df,"dateNotification")
        df = self.mark_mandatory_field(df,"considerationsSociales")
        df = self.mark_mandatory_field(df,"considerationsEnvironnementales")
        df = self.mark_mandatory_field(df,"marcheInnovant")
        df = self.mark_mandatory_field(df,"origineUE")
        df = self.mark_mandatory_field(df,"origineFrance")
        df = self.mark_mandatory_field(df,"ccag")
        df = self.mark_mandatory_field(df,"offresRecues")
        df = self.mark_mandatory_field(df,"montant")
        df = self.mark_mandatory_field(df,"formePrix")
        df = self.mark_mandatory_field(df,"typePrix")
        df = self.mark_mandatory_field(df,"attributionAvance")
        df = self.mark_mandatory_field(df,"datePublicationDonnees")
        df = self.mark_mandatory_field(df,"acheteur.id")
        df = self.mark_mandatory_field(df,"lieuExecution.code")
        df = self.mark_mandatory_field(df,"lieuExecution.typeCode")
        df = self.mark_mandatory_field(df,"titulaire_id_1")
        df = self.mark_mandatory_field(df,"titulaire_typeIdentifiant_1")
        df = self.mark_mandatory_field(df,"idActeSousTraitanceModification")
        df = self.mark_mandatory_field(df,"typeIdentifiantActeSousTraitanceModification")
        df = self.mark_mandatory_field(df,"dureeMoisActeSousTraitanceModification")
        df = self.mark_mandatory_field(df,"dateNotificationActeSousTraitanceModification")
        df = self.mark_mandatory_field(df,"montantActeSousTraitanceModification")
        df = self.mark_mandatory_field(df,"datePublicationDonneesActeSousTraitanceModification")

        df = self.mark_optional_field(df,"idAccordCadre")
        df = self.mark_optional_field(df,"tauxAvance")
        df = self.mark_optional_field(df,"typeGroupementOperateurs")
        df = self.mark_optional_field(df,"sousTraitanceDeclaree")
        df = self.mark_optional_field(df,"idSousTraitance")
        df = self.mark_optional_field(df,"dureeMoisSousTraitance")
        df = self.mark_optional_field(df,"dateNotificationSousTraitance")
        df = self.mark_optional_field(df,"montantSousTraitance")
        df = self.mark_optional_field(df,"variationPrixSousTraitance")
        df = self.mark_optional_field(df,"datePublicationDonneesSousTraitance")
        df = self.mark_optional_field(df,"idActeSousTraitance")
        df = self.mark_optional_field(df,"typeIdentifiantActeSousTraitance")
        df = self.mark_optional_field(df,"idModification")
        df = self.mark_optional_field(df,"dureeMoisModification")
        df = self.mark_optional_field(df,"montantModification")
        df = self.mark_optional_field(df,"titulairesModification")
        df = self.mark_optional_field(df,"typeIdentifiantModification")
        df = self.mark_optional_field(df,"dateNotificationModification")
        df = self.mark_optional_field(df,"datePublicationDonneesModification")
        
        return df

    #@compute_execution_time
    def concession_mark_fields(self,df: pd.DataFrame) -> pd.DataFrame:

        df = self.mark_mandatory_field(df,"id")
        df = self.mark_mandatory_field(df,"nature")
        df = self.mark_mandatory_field(df,"objet")
        df = self.mark_mandatory_field(df,"procedure")
        df = self.mark_mandatory_field(df,"dureeMois")
        df = self.mark_mandatory_field(df,"dateDebutExecution")
        df = self.mark_mandatory_field(df,"dateSignature")
        df = self.mark_mandatory_field(df,"considerationsSociales")
        df = self.mark_mandatory_field(df,"considerationsEnvironnementales")
        df = self.mark_mandatory_field(df,"valeurGlobale")
        df = self.mark_mandatory_field(df,"montantSubventionPublique")
        df = self.mark_mandatory_field(df,"datePublicationDonnees")
        df = self.mark_mandatory_field(df,"autoriteConcedante.id")
        df = self.mark_mandatory_field(df,"concessionnaire_id_1")
        df = self.mark_mandatory_field(df,"concessionnaire_typeIdentifiant_1")
        df = self.mark_mandatory_field(df,"depensesInvestissement")
        df = self.mark_mandatory_field(df,"datePublicationDonneesExecution")
        df = self.mark_mandatory_field(df,"intituleTarif")
        df = self.mark_mandatory_field(df,"tarif")

        df = self.mark_optional_field(df,"idModification")
        df = self.mark_optional_field(df,"dureeMoisModification")
        df = self.mark_optional_field(df,"valeurGlobaleModification")
        df = self.mark_optional_field(df,"dateSignatureModification")
        df = self.mark_optional_field(df,"datePublicationDonneesModification")

        return df
    
    def comment(self):
        # séparation des marchés et des concessions, car traitement différent
        df_marche = self.df.loc[~self.df['nature'].str.contains('concession', case=False, na=False)]

        df_concession1 = self.df.loc[self.df['nature'].str.contains('concession', case=False, na=False)]
        # df_concession prend aussi en compte les lignes restantes ou la colonne "_type" contient "concession" dans le df_marche et concatène les deux dataframes
        df_concession = pd.concat([self.df_concession1, df_marche.loc[df_marche['_type'].str.contains('concession', case=False, na=False)]])
        # remove old df for memory
        del df_concession1
        df_marche = df_marche.loc[~df_marche['_type'].str.contains('concession', case=False, na=False)]

        self.marche_mark_fields(df_marche)
        self.concession_mark_fields(df_concession)