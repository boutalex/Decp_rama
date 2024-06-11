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
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class SourceProcess:
    
    columns_marche_2022 = set([
        'id',
        'acheteur',
        'nature',
        'objet',
        'codeCPV',
        'techniques',
        'procedure',
        'dateNotification',
        'lieuExecution',
        'dureeMois',
        'modalitesExecution',
        'considerationsSociales',
        'considerationsEnvironnementales',
        'marcheInnovant',
        'origineUE',
        'origineFrance',
        'ccag',
        'offresRecues',
        'montant',
        'formePrix',
        'offresRecues',
        'typesPrix',
        'attributionAvance',
        'titulaires',
        'typeGroupementOperateurs',
        'sousTraitanceDeclaree',
        'datePublicationDonnees'
    ])
    
    colums_marche_opt_2022 = set([
        'idAccordCadre',
        'tauxAvance',
        'actesSousTraitance',
        'modifications',
        'modificationsActesSousTraitance'
    ])
    
    columns_concession_2022 = set([
        'id',
        'autoriteConcedante',
        'nature',
        'objet',
        'procedure',
        'dureeMois',
        'dateDebutExecution',
        'dateSignature',
        'considerationsSociales',
        'considerationsEnvironnementales',
        'concessionnaires',
        'valeurGlobale',
        'montantSubventionPublique',
        'datePublicationDonnees',
        'donneesExecution'
    ])
    
    columns_concession_opt_2022 = set([
        'modifications'
    ])

    columns_marche_2019 = set([
        'id',
        'nature',
        'objet',
        'codeCPV',
        'procedure',
        'lieuExecution',
        'dureeMois',
        'dateNotification',
        'datePublicationDonnees',
        'montant',
        'formePrix',
        'titulaires',
        'source',
        'acheteur'
    ])

    colums_marche_opt_2019 = set([
        'modifications'
    ])

    columns_concession_2019 = set([
        'id',
        'nature',
        'typeContrat',
        'objet',
        'codeCPV',
        'procedure',
        'lieuExecution',
        'dureeMois',
        'dateNotification',
        'datePublicationDonnees',
        'montant',
        'formePrix',
        'titulaires',
        'uuid',
        'source',
        'acheteur',
        'modifications'
    ])

    colums_concession_opt_2019 = set([
        'modifications'
    ])
   
    """La classe SourceProcess est une classe abstraite qui sert de parent à chaque classe enfant de
    sources. Elle sert à définir le cas général des étapes de traitement d'une source : création des
    variables de classe (__init__), nettoyage des dossiers de la source (_clean_metadata_folder),
    récupération des URLs (_url_init), get, convert et fix."""
    def __init__(self, key,data_format):
        """L'étape __init__ crée les variables associées à la classe SourceProcess : key, source,
        format, df, title, url, cle_api et metadata."""
        logging.info("  ÉTAPE INIT")
        self.key = key
        self.data_format = data_format
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.url_source = self.metadata[self.key]["url_source"]
        self.df = pd.DataFrame()
        self.title = []
        # Lavage des dossiers de la source
        self._clean_metadata_folder()

        # Récupération des urls
        self._url_init() 

        # Liste des dictionnaires pour l'étape de nettoyage
        self.dico_ignored = []
        self.dico_2022_marche = []
        self.dico_2022_concession = []
        self.dico_2019 = []


    def _clean_metadata_folder(self):
        """La fonction _clean_metadata_folder permet le nettoyage de /metadata/{self.source}"""
        # Lavage des dossiers dans metadata
        logging.info(f"Début du nettoyage de metadata/{self.source}")
        if os.path.exists(f"metadata/{self.source}"):
            shutil.rmtree(f"metadata/{self.source}")
        logging.info(f"Nettoyage metadata/{self.source} OK")


    def _url_init(self):
        """_url_init permet la récupération de l'ensemble des url des fichiers qui doivent être
        téléchargés pour une source. Ces urls sont conservés dans self.metadata."""

        logging.info("Initialisation")
        os.makedirs(f"metadata/{self.source}", exist_ok=True) 
        os.makedirs(f"old_metadata/{self.source}", exist_ok=True)
        self.cle_api = self.metadata[self.key]["cle_api"]
        #Liste contenant les urls à partir desquels on télécharge les fichiers
        if self.cle_api==[]:
            self.url = [self.url_source]
        else:
            self.url, self.title = self.create_metadata_file(len(self.cle_api))
           
        logging.info("Initialisation finie")
    

    def create_metadata_file(self,n:int)->tuple[list,list]:
        """
        Fonction réalisant le téléchargement des métadatas, la copie des
        fichiers métadatas et la création des listes contenant les titres
        et les urls des fichiers.
        n: nombre de clé api (=nombre de tour dans la boucle)
        """
        logging.info("Début de la récupération de la liste des url")
        title = []  
        url = []    
        for i in range(n):
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
            
            #Création de la liste des urls selon 2 cas: 1er téléchargement, i-nième téléchargement
            if old_ressources==[]:
                url = url + [d["url"] for d in ressources if
                            (d["url"].endswith("xml") or d["url"].endswith("json"))]
                title = title + [d["title"] for d in ressources]
            else: 
                url, title = self.check_date_file(url,title, ressources, old_ressources)
                print("Les urls dont le contenu a été modifié sont: ", url)

            #Cas où les fichiers old_metadata existent: on écrit dedans à nouveau
            if os.path.exists(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json"):
                with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r') as source_file:
                    contenu = source_file.read()
                with open(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json", 'w') as destination_file:
                    destination_file.write(contenu)
            #Cas où les fichiers old_metadata n'existent pas: on fait une copie
            else:
                shutil.copy(f"metadata/{self.source}/metadata_{self.key}_{i}.json",f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json")
                print(os.listdir(f"old_metadata/{self.source}"))
            logging.info("Récupération des url OK")
            return url,title


    def check_date_file(self,url:list, title: list, new_ressources:dict,old_ressources:dict)->list:
        """
        Fonction vérifiant si la date de dernière modification des fichiers ressources 
        dans les metadatas est strictement antérieure à la date de dernière modification.
        @url: la liste contenant les liens pour télécharger les fichiers
        @new_ressources: dictionnaire correspondant au champ "resources" dans le fichier metadata de la source
        @old_ressources: dictionnaire correspondant au champ "resources" dans le fichier old_metadata de la source
        """
        for i in range(len(new_ressources)):
            #condition1 : test si les nouvelles métadonnées sont plus récentes que les anciennes 
            condition1=new_ressources[i]["last_modified"]>old_ressources[i]["last_modified"]
            #condition2 : vérification de l'extension
            condition2=(new_ressources[i]["url"].endswith("xml") or new_ressources[i]["url"].endswith("json"))
            if condition1 and condition2 :
                url = url + [new_ressources[i]["url"]] 
                title = title + [new_ressources[i]["title"]] 
        return url, title


    def download_without_metadata(self):
        """
        Fonction téléchargeant un fichier n'ayant pas de clé api. Par conséquent, le
        téléchargement s'effectue grâce à l'url dans l'attribut url_source
        """
        nom_fichier = os.listdir(f"sources/{self.source}")
        if nom_fichier!=[]:   #Dossier non vide
            os.remove(f"sources/{self.source}/{nom_fichier[0]}")
            logging.info(f"Fichier : {nom_fichier[0]} existe déjà, nettoyage du doublon ")
            wget.download(self.url[0], f"sources/{self.source}/{nom_fichier[0]}")
            self.title = [ nom_fichier[0] ]
            print("TITLE :", self.title)

        #Le dossier est vide car il s'agit du 1er téléchargement. Téléchargement 
        #dans le dossier puis affectation du nom du fichier à l'attribut titre
        else:
            wget.download(self.url[0], f"sources/{self.source}/")
            print(os.listdir(f"sources/{self.source}"))
            self.title = [ os.listdir(f"sources/{self.source}")[0] ]
            print("TITLE:", self.title)
            

    def get(self):
        """
        Étape get qui permet le lavage du dossier sources/{self.source} et 
        la récupération de l'ensemble des fichiers présents sur chaque url.
        """
        logging.info("  ÉTAPE GET")
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s)")
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        print("SELF.URL:" , self.url)
        if self.cle_api==[]:
            print("Pas  de clé api")
            self.download_without_metadata()
        else:
            # Verification de l'existence d'un eventuel doublon + nettoyage + 
            # téléchargement du nouveau fichier
            for i in range(len(self.url)):
                try:
                    if os.path.exists(f"sources/{self.source}/{self.title[i]}"):
                        os.remove(f"sources/{self.source}/{self.title[i]}")
                        logging.info(f"Fichier : {self.title[i]} existe déjà, nettoyage du doublon ")
                    wget.download(self.url[i], f"sources/{self.source}/{self.title[i]}")
                except:
                    logging.error("Problème de téléchargement du fichier ", self.url[i])
        logging.info(f"Téléchargement : {len(self.url)} fichier(s) OK")


    def _has_all_field_and_date_2019(self, record:dict)->bool :
        """
        Fonction vérifiant qu'un marché/concession possède bel et bien toutes 
        les colonnes requises par notre schéma. Il vérifie également que les 
        dates contenues dans le marché/concession soient postérieures à 2024.
        @record : marché/concession que l'on souhaite traiter
        """
        liste_marche = ['Marché','Marché de partenariat', 'Accord-cadre', 'Marché subséquent','MARCHE']
        liste_concession = ['Concession de travaux',  'Concession de service', 'Concession de service public', 'Délégation de service public']
        print("nature", record['nature'])
        if record["nature"] in liste_marche : #Il faudra modifier à l'appel de sorte à avoir la nature : marché
            champs_differents = set(list(record.keys()))
            #Complémentaire de l'intersection entre les champs du dictionnaire et les champs obligatoires 
            champs_differents = champs_differents.symmetric_difference(self.columns_marche_2019)
            if len(champs_differents)>0:
                champs_en_moins = champs_differents & self.columns_marche_2019 #On récupère seulement les champs obligatoires manquants
                champs_en_plus = champs_differents.difference(self.colums_marche_opt_2019).difference(champs_en_moins)
                if len(champs_en_moins)>0 or len(champs_en_plus)>0:
                    self.champs_moins = self.champs_moins | champs_en_moins
                    self.champs_plus =  self.champs_plus  | champs_en_plus
                    # logging.info(f"Voici les champs en moins :{champs_en_moins}")
                    # logging.info(f"Voici les champs en plus :{champs_en_plus}")
                    return False
                else :
                    logging.info(f"Tous les champs sont valides.")
                    
            if self.date_before_2024(record,"marché"):
                logging.info(f"Dictionnaire conforme au format 2019")
                return True
            else:
                logging.info(f"Erreur : date précédant 2024")
                return False 
        elif record["nature"] in liste_concession :
            champs_differents = set(list(record.keys()))
            #Complémentaire de l'intersection entre les champs du dictionnaire et les champs obligatoires 
            champs_differents = champs_differents.symmetric_difference(self.columns_concession_2019)
            if len(champs_differents)>0:
                champs_en_moins = champs_differents & self.columns_concession_2019 #On récupère seulement les champs obligatoires manquants
                champs_en_plus = champs_differents.difference(self.colums_concession_opt_2019).difference(champs_en_moins)
                if len(champs_en_moins)>0 or len(champs_en_plus)>0:
                    self.champs_moins = self.champs_moins | champs_en_moins
                    self.champs_plus =  self.champs_plus  | champs_en_plus
                    #logging.info(f"Voici les champs en moins :{champs_en_moins}")
                    #logging.info(f"Voici les champs en plus :{champs_en_plus}")
                    return False
            if self.date_before_2024(record,"marché"):
                logging.info(f"Dictionnaire conforme au format 2019")
                return True
            else: 
                logging.info(f"Erreur: date après 2024")
                return False
        else:
            return False

    
    def _has_all_field_and_date_2024(self, record:dict, record_type:str)->bool :
        """
        Fonction vérifiant qu'un marché/concession possède toutes les colonnes 
        requises par notre schéma. Il vérifie également que les 
        dates contenues dans le marché/concession soient postérieures à 2024.
        @record : marché/concession que l'on souhaite traiter
        @record_type : type du marché (marché/concession)
        """
        
        if record_type == 'marche':
            champs_differents = set(list(record.keys()))
            #Complémentaire de l'intersection entre les champs du dictionnaire et les champs obligatoires
            champs_differents = champs_differents.symmetric_difference(self.columns_marche_2022)
            if len(champs_differents)>0:
                champs_en_moins = champs_differents & self.columns_marche_2022 #On récupère seulement les champs obligatoires manquants
                champs_en_plus = champs_differents.difference(self.colums_marche_opt_2022).difference(champs_en_moins)
                if len(champs_en_moins)>0 or len(champs_en_plus)>0 :
                    self.champs_moins = self.champs_moins | champs_en_moins
                    self.champs_plus =  self.champs_plus  | champs_en_plus
                    return False
                    # logging.info(f"Voici les champs en moins :{champs_en_moins}")
                    # logging.info(f"Voici les champs en plus :{champs_en_plus}")
                    
            if not self.date_after_2024(record):
                logging.info(f"Erreur : date précédant 2024")
                return False 
            else:
                logging.info(f"Dictionnaire valide au format 2024")
                return True

        elif record_type == 'contrat-concession':
            champs_differents = set(list(record.keys()))
            #Complémentaire de l'intersection entre les champs du dictionnaire et les champs obligatoires
            champs_differents = champs_differents.symmetric_difference(self.columns_concession_2022)
            if len(champs_differents)>0:
                champs_en_moins = champs_differents & self.columns_concession_2022 #On récupère seulement les champs obligatoires manquants
                champs_en_plus = champs_differents.difference(self.columns_concession_opt_2022).difference(champs_en_moins)
                if len(champs_en_moins)>0 or len(champs_en_plus)>0:
                    self.champs_moins = self.champs_moins | champs_en_moins
                    self.champs_plus =  self.champs_plus  | champs_en_plus
                    # logging.info(f"Voici les champs en moins :{champs_en_moins}")
                    # logging.info(f"Voici les champs en plus :{champs_en_plus}")
                    return False
            if not self.date_after_2024(record):
                logging.info(f"Erreur : date précédant 2024")
                return False 
            else:
                logging.info(f"Dictionnaire valide au format 2024")
                return True
        else:
            return False
    

    def date_norm(self,datestr:str):
        return datestr.replace('+','-') if datestr else datestr


    def date_before_2024(self, record,nature:str):
        """ 
        La fonction vérifie que les dates contenues dans un marché/une 
        concession sont postérieures à 2024 et sont de la forme Y-M-J
        pour les colonnes date et date_de_publication. 
        """
        if nature == "marché":
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
    

    def date_after_2024(self, record:dict)->bool:
            """
            La fonction prend en entrée un dictionnaire et renvoie un 
            booléeen.Les dates postérieures à 2024 doivent être de la
            forme Y-M-J pour les colonnes date et date_de_publication.
            @record : marché/concession que l'on souhaite traiter
            """
            first = datetime.strptime("2024-01-01", "%Y-%m-%d")
            pattern1 = r'20[0-9]{2}-[0-1]{1}[0-9]{1}-[0-9]{2}'
            pattern2 = r'20[0-9]{2}/[0-1]{1}[0-9]{1}/[0-9]{2}'
            if record['nature']=='Marché':
                col_date = 'dateNotification'
            else :
                col_date = 'dateDebutExecution'
            col_date_publication='datePublicationDonnees'
            col_list = [col_date,col_date_publication]
            for col in col_list:
                if col in record and record[col] and (re.match(pattern1,record[col]) or re.match(pattern2,record[col])):
                    try:
                        date = datetime.strptime(record[col], "%Y-%m-%d")
                        if date>=first:
                            if col == col_date_publication:
                                record[col_date] = record[col_date_publication]
                            return True
                    except:
                        None
            return False
 
        
    # def clean(self):
    #     """
    #     Fonction réalisant un tri sur le format 2022 ou le format 2019 en complétant
    #     les 3 dictionnaires et permettre la conversion en dataframe pour plus tard.
    #     """
    #     logging.info(" ÉTAPE CLEAN")
    #     logging.info("Début du tri des nouveaux fichiers")
    #     #Ouverture des fichiers
    #     for i in range(len(self.title)):            
    #         if self.format == 'xml':
    #             try:
    #                 with open(f"sources/{self.source}/{self.title[i]}", encoding='utf-8') as xml_file:
    #                     dico = xmltodict.parse(xml_file.read(), dict_constructor=dict, \
    #                                            force_list=('marche','titulaires', 'modifications', 'actesSousTraitance',
    #                                            'modificationsActesSousTraitance', 'typePrix','considerationEnvironnementale',
    #                                            'modaliteExecution'))
    #             except Exception as err:
    #                 logging.error(f"Exception lors du chargement du fichier xml {self.title[i]} - {err}")

    #         elif self.format == 'json':
    #                 try:
    #                     with open(f"sources/{self.source}/{self.title[i]}", encoding="utf-8") as json_file:
    #                         dico = json.load(json_file)

    #                 except Exception as err:  
    #                     logging.error(f"Exception lors du chargement du fichier json {self.title[i]} - {err}")

    #         self.tri_format(dico,self.title[i])    #On obtient 3 listes de dico qui sont mises à jour à chaque tour de boucle

    #     logging.info("Fin du tri selon le format")
    #     logging.info("Nettoyage OK")
    
    def clean(self):
        """
        Fonction réalisant un tri sur le format 2022 ou le format 2019 en complétant
        les 3 dictionnaires et permettre la conversion en dataframe pour plus tard.
        """
        logging.info(" ÉTAPE CLEAN")
        logging.info("Début du tri des nouveaux fichiers")
        #Ouverture des fichiers
        dico = {}
        for i in range(len(self.title)):            
            if self.format == 'xml':
                #Vérification du schéma xml
                scheme_path = 'schemes/schema_decp_v2.0.2.xsd'
                if not self.check(None,f"sources/{self.source}/{self.title[i]}"):
                    logging.warning(f"sources/{self.source}/{self.title[i]} not a valide xml")
                    continue
                try:
                    with open(f'sources/{self.source}/{self.title[i]}', 'r', encoding='utf-8') as xml_file:
                        xml_content = xml_file.read()
                    xmlschema.validate(xml_content, scheme_path)==None
                    with open(f"sources/{self.source}/{self.title[i]}", encoding='utf-8') as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict, \
                                               force_list=('marche','titulaires', 'modifications', 'actesSousTraitance',
                                               'modificationsActesSousTraitance', 'typePrix','considerationEnvironnementale',
                                               'modaliteExecution'))
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier xml {self.title[i]} - {err}")

            elif self.format == 'json':
                #Vérification du schéma json
                scheme_path = 'schemes/decp_2022.json'
                if not self.check(f"sources/{self.source}/{self.title[i]}", None):
                    logging.warning(f"sources/{self.source}/{self.title[i]} not a valid json")
                    raise Exception("Json format not valid")
                try:
                    with open(f"sources/{self.source}/{self.title[i]}", encoding="utf-8") as json_file1:
                        dico = json.load(json_file1)
                    with open(scheme_path, "r",encoding='utf-8')as json_file2:
                        jsonScheme = json.load(json_file2)
                        json_file2.close
                    if self.validateJson(json_file1,jsonScheme): 
                        print("On est allé jusque là") 
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier json {self.title[i]} - {err}")
            self.champs_plus = set()
            self.champs_moins = set()
            self.tri_format(dico,self.title[i])    #On obtient 3 listes de dico qui sont mises à jour à chaque tour de boucle
            
        logging.info("Fin du tri selon le format")
        logging.info("Nettoyage OK")



    def tri_format(self, dico:dict, file_name:str):
        """
        Cette fonction permet de vérifier la structure du dictionnaire fournit en
        entrée. Si un champ est manquant dans un marché ou une concession, ce dernier
        est stocké dans une liste pour un éventuel traitement.
        @dico : dictionnaire 
        @file_name : nom du fichier où sont stockés les fichiers non valides et ignorés
        """
        if 'marches' in dico:
            if 'marche' in dico['marches']:  #format 2022
                for m in dico['marches']['marche']:
                    if self._has_all_field_and_date_2024(m, 'marche'):
                        self.dico_2022_marche.append(m)
                    else:                   
                        self.dico_ignored.append(m)

            elif 'contrat-concession' in dico['marches']:
                for m in dico['marches']['contrat-concession']:
                    if self._has_all_field_and_date_2024(m, 'contrat-concession'):
                        self.dico_2022_concession.append(m)
                    else:
                        self.dico_ignored.append(m)

            else:                            #format 2019
                for m in dico['marches']:
                    if self._has_all_field_and_date_2019(m):                
                        self.dico_2019.append(m)
                    else:
                        self.dico_ignored.append(m)

        else:
            logging.error("Balise 'marches' inexistante")
            dico.clear()
        if len(self.champs_plus)>0:
            logging.info(f"Dans le fichier {file_name}, les champs en plus sont: {self.champs_plus} ")
        if len(self.champs_moins)>0:
            logging.info(f"Dans le fichier {file_name}, les champs en moins sont: {self.champs_moins} ")
        if len(self.dico_ignored)>0:
            logging.info(f"Nombre de marchés invalides: {len(self.dico_ignored)} dans {file_name}")
    

    def _retain_with_format(self, dico:dict, file_name:str)->dict:
        """
        Cette fonction permet de vérifier la validité des marchés et des concessions
        du dictionnaire fournit en entrée. Elle garde également en mémoire une liste
        des marchés/concessions invalides pour un éventuel traitement.
        @dico : dictionnaire 
        @file_name : nom du fichier où sont stockés les fichiers non valides et ignorés
        """
        self.dico_ignored = []
        if 'marches' in dico:
            if 'marche' in dico['marches']:
                for m in dico['marches']['marche']:
                    if not self._has_all_field_and_date_2024(m, 'marche'):
                        dico['marches']['marche'].remove(m)
                        self.dico_ignored.append(m)
                        
            if'contrat-concession' in dico['marches']:
                for m in dico['marches']['contrat-concession']:
                    if not self._has_all_field_and_date_2024(m, 'contrat-concession'):
                        dico['marches']['contrat-concession'].remove(m)
                        self.dico_ignored.append(m)
        else:
            logging.error("Balise 'marches' inexistante")
            dico.clear()
        if len(self.dico_ignored)>0:
            logging.info(f"Ignored {len(self.dico_ignored)} record(s) in {file_name}")
        return dico


    def _add_column_type(self, df: pd.DataFrame, default_type_name:str = None):
        if self.data_format=='2022' and not "_type" in df.columns and (default_type_name or "nature" in df.columns):
            if default_type_name:
                df['_type'] = default_type_name
            else:
                df['_type'] = df["nature"].apply(lambda x: "Marché" if x=="Marché" else "Concession")


    def convert_prestataire(self):
        """Étape de conversion des fichiers qui supprime les ' et concatène les fichiers présents
        dans {self.source} dans un seul DataFrame"""
        logging.info("  ÉTAPE CONVERT")
        # suppression des '
        count = 0
        list_path = []    #list_path sera la liste de tous les fichiers car self.title est la liste des noms de fichiers qui ont été téléchargés
        repertoire_source = f"sources/{self.source}"
        #on récupère le nom de chaque fichier et on le met dans liste path, en plus de compter le nombre de fichiers présent dnas le dossier source
        for path in os.listdir(repertoire_source):
            if os.path.isfile(os.path.join(repertoire_source, path)):
                list_path = list_path + [path] 
                count += 1
        for i in range(count):
            # print ("title",self.title) 
            # print ("i :",i) 
            file_path = f"sources/{self.source}/{list_path[i]}"
            file_exist = os.path.exists(file_path)
            if not file_exist:
                logging.warning(f"Le fichier {file_path} n'existe pas.")

        # if count != len(self.url):
        #     logging.warning("Nombre de fichiers en local inégal au nombre d'url trouvé")
        # if count != len(self.url):
        #     logging.warning("Nombre de fichiers en local inégal au nombre d'url trouvé")
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(count):
                if self.data_format=='2022':
                    if not self.check(None,f"sources/{self.source}/{list_path[i]}"):
                        logging.warning(f"sources/{self.source}/{list_path[i]} not a valide xml")
                        continue
                        #raise Exception(f"sources/{self.source}/{self.file_name[i]}.{self.format} not a valide xml")
                try:
                    with open(f"sources/{self.source}/{list_path[i]}", encoding='utf-8') as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
 
                    if dico['marches'] is not None:
                        dico = self.format_2022(dico,f"sources/{self.source}/{list_path[i]}_ignored.{self.format}")
                        
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
                        logging.warning(f"Le fichier {list_path[i]} est vide, il est ignoré")
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier xml {list_path[i]} - {err}")

            if len(li) != 0:
                df = pd.concat(li)
                ##del li
                df = df.reset_index(drop=True)
            else:
                # create empty dataframe
                df = pd.DataFrame()
            return df
        elif self.format == 'json':
            li = []
            for i in range(count):
                try:
                    with open(f"sources/{self.source}/{list_path[i]}", encoding="utf-8") as json_file:
                        
                        #check for format compliance (only for data_format 2022)
                        if self.data_format=='2022':
                            if not self.check(json_file,None):
                                logging.warning(f"sources/{self.source}/{list_path[i]} not a valid json")
                                raise Exception("Json format not valid")
                        
                            dico = json.load(json_file)
                            self._retain_with_format(dico,f"sources/{self.source}/{list_path[i]}_ignored.{self.format}")

                            
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
                            self._retain_with_format(dico,f"sources/{self.source}/{list_path[i]}_ignored.{self.format}")
                            df = pd.DataFrame.from_dict(dico['marches'])
                            self._add_column_type(df)
                            li.append(df)
                            ##del df
                        ##del dico
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier json {list_path[i]} - {err}")
            df = pd.concat(li)
            ##del li
            df = df.reset_index(drop=True)
            return df
        logging.info("Conversion OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")

    
    # def convert2(self):
    #     """
    #     Étape de conversion des fichiers qui supprime les ' et concatène les fichiers 
    #     présents dans {self.source} dans un seul DataFrame. Elle utilise le dictionnaire 
    #     des marchés/concessions valides de chaque fichier pour le convertir en un 
    #     dataframe. L'ensemble des dataframes est stocké dans une liste. 
    #     """
    #     logging.info("  ÉTAPE CONVERT")
    #     # suppression des '
    #     count = 0
    #     list_path = []    #list_path sera la liste de tous les fichiers car self.title est la liste des noms de fichiers qui ont été téléchargés
    #     repertoire_source = f"sources/{self.source}"
    #     #on récupère le nom de chaque fichier et on le met dans liste path, en plus de compter le nombre de fichiers présent dnas le dossier source
    #     for path in os.listdir(repertoire_source):
    #         if os.path.isfile(os.path.join(repertoire_source, path)):
    #             list_path = list_path + [path] 
    #             count += 1

    #     logging.info(f"Début de convert: mise au format DataFrame de {self.source}")

    #     #Extraction des données dans un dictionnaire, puis création d'un dataframe pour agréger les données
    #     li = []
    #     for i in range(count):
            
    #         if self.format == 'xml':
    #             try:
    #                 with open(f"sources/{self.source}/{list_path[i]}", encoding='utf-8') as xml_file:
    #                     dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
    #             except Exception as err:
    #                 logging.error(f"Exception lors du chargement du fichier xml {list_path[i]} - {err}")

    #         elif self.format == 'json':
    #                 try:
    #                     with open(f"sources/{self.source}/{list_path[i]}", encoding="utf-8") as json_file:
    #                         dico = json.load(json_file)

    #                 except Exception as err:
    #                     logging.error(f"Exception lors du chargement du fichier json {list_path[i]} - {err}")

    #         #dictionnaire contenant les dictionnaires valide par rapport au schéma 2022
    #         dico_valide = self._retain_with_format(dico,f"sources/{self.source}/{list_path[i]}_ignored.{self.format}")
        
    #         # A chaque nouveau marché/concession, on ajoute crée un dataframe d'une
    #         # colonne, on l'ajoute en précisant le type (Marché ou Contrat-concession).
    #         # Puis, on l'ajoute à la liste des dataframes.
    #         if dico_valide!=[] and 'marches' in dico_valide:

    #             # Add marchés 
    #             if 'marche' in dico_valide['marches']:   
    #                 df = pd.DataFrame.from_dict(dico_valide['marches']['marche'])
    #                 self._add_column_type(df,"Marché")
    #                 li.append(df)
    #                 ##del df

    #             # Add Concession
    #             if 'contrat-concession' in dico_valide['marches']:
    #                 df = pd.DataFrame.from_dict(dico_valide['marches']['contrat-concession'])
    #                 self._add_column_type(df,"Concession")
    #                 li.append(df)
    #                 ##del df
    #             ##del dico
    #         else:  # cas presque null
    #             logging.warning(f"Le fichier {list_path[i]} est vide, il est ignoré")
                          
    #     #Concaténation des dataframes de la liste li en une dataframe                  
    #     if len(li) != 0:
    #         df = pd.concat(li)
    #         df = df.reset_index(drop=True)
    #     else:
    #         # create empty dataframe
    #         df = pd.DataFrame()
    #     self.df = df
    #     logging.info("Conversion OK")
    #     logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")
    

    def convert(self):
        """
        Étape de conversion des fichiers qui supprime les ' et concatène les fichiers 
        présents dans {self.source} dans un seul DataFrame. Elle utilise le dictionnaire 
        des marchés/concessions valides de chaque fichier pour le convertir en un 
        dataframe. L'ensemble des dataframes est stocké dans une liste. 
        """
        logging.info("  ÉTAPE CONVERT")
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")

        #Mise à jour des dictionnaires
        old_files = list(set(os.listdir(f"sources/{self.source}")) - set(self.title))   #liste des titres des fichiers déja présents en local
        print("old files ", old_files)

        for i in range(len(old_files)):
            logging.info("Extraction des données des anciens fichiers")

            if self.format == 'xml':
                try:
                    with open(f"sources/{self.source}/{old_files[i]}", encoding='utf-8') as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict,  \
                                               force_list=('marche','titulaires', 'modifications', 'actesSousTraitance',
                                               'modificationsActesSousTraitance', 'typePrix','considerationEnvironnementale',
                                               'modaliteExecution'))
                        #Ajout du dictionnaire dans la bonne variable (dico_2022_marche, dico_2022_concession, dico_2019  ou dico_ignored)
                        self.tri_format(dico, f"sources/{self.source}/{old_files[i]}")
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier xml {old_files[i]} - {err}")

            elif self.format == 'json':
                    try:
                        with open(f"sources/{self.source}/{old_files[i]}", encoding="utf-8") as json_file:
                            dico = json.load(json_file)
                            #Ajout du dictionnaire dans la bonne variable (dico_2022_marche, dico_2022_concession, dico_2019  ou dico_ignored)
                            self.tri_format(dico, f"sources/{self.source}/{old_files[i]}")

                    except Exception as err:
                        logging.error(f"Exception lors du chargement du fichier json {old_files[i]} - {err}")

           

        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        #Liste qui conservera les dataframes. 
        li = []

        # Ajout d'un marché à la liste des dataframes
        df = pd.DataFrame.from_dict(self.dico_2022_marche)
        self._add_column_type(df,"Marché")
        li.append(df)

    
        # Ajoutd'une concession à la liste des dataframes
        df = pd.DataFrame.from_dict(self.dico_2022_concession)
        self._add_column_type(df,"Concession")
        li.append(df)
           
        #Concaténation des dataframes de la liste li en une dataframe                  
        if len(li) != 0:
            df = pd.concat(li)
            df = df.reset_index(drop=True)
        else:
            # create empty dataframe
            df = pd.DataFrame()
        self.df = df
        logging.info("Conversion OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")



    def validateJson(self,jsonData,jsonScheme)->bool:
        """
        Fonction vérifiant si le fichier jsn "jsonData" respecte
        le schéma spécifié dans le  schéma en paramètre "jsonScheme". 
        """
        try:
            #Draft7Validator.check_schema(jsonScheme)
            #Draft202012Validator.check_schema(jsonScheme)
            validate(instance=jsonData, schema=jsonScheme)
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Erreur de validation json - {err}")
            return False
        return True

    def validateXml(self, xml_path: str, xsd_path: str) -> bool:
        """
        Fonction vérifiant si le fichier xml "xml_path" respecte 
        le schéma spécifié dans le paramètre "xsd_path".
        """
        xml_schema_doc = etree.parse(xsd_path)
        xml_schema = etree.XMLSchema(xml_schema_doc)

        xml_doc = etree.parse(xml_path)

        try:
            result = xml_schema.validate(xml_doc)
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Erreur de validation xml - {err}")
            return False
        
        return result
    
    def check(self,jsonData,xml_path) -> bool:
        """
        Fonction qui prend en paramètre une donnée json ou xml 
        et qui vérifie grâce à un schéma que la donnée est valide.
        @JsonData : donnée json en entrée (nul si xml)
        @xml_path : chemin du fichier xml en entrée (nul si json)
        """
        #on vérifie que la donnée en entrée est valide par rapport au schéma
        if self.format=='json':
            scheme_path = 'schemes/decp_2022.json'
            with open(scheme_path, "r",encoding='utf-8')as jsonfile:
                jsonScheme = json.load(jsonfile)
                jsonfile.close
            return self.validateJson(jsonData,jsonScheme)
        else:    # xml
            scheme_path = 'schemes/schema_decp_v2.0.2.xsd'
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

    def convert_boolean(self,col_name:str):
        """
        Permet de remplacer les valeurs booléennes ou "Vrai" ou "Faux" par "oui" ou "non"
        """
        #Conversion si il s'agit de string
        if self.df[col_name].dtypes == 'object':  
            self.df[col_name] = self.df[col_name].astype(str).replace({'1': 'oui', 'true': 'oui', '0': 'non', 'false': 'non','True': 'oui', 'False': 'non'})
        else:
            self.df[col_name] = self.df[col_name].astype(str).replace({'True': 'oui', 'False': 'non' }) 


    def fix(self):
        """
        Étape fix qui crée la colonne source dans le DataFrame et 
        qui supprime les doublons purs.
        """
            
        def check_dico(dico):
        #Prend en entrée le dictionnaire du champ "acheteur"
            if dico=={} or dico is None or dico['id']==None:
                return True
            return False
        
        def update_id(ligne):
            #Modifie le champ "id" du champ acheteur
            if check_dico(ligne["acheteur"]):
                   ligne["acheteur"] = {"id": ligne["id"] } 
            return ligne      

        
        logging.info("  ÉTAPE FIX")
        logging.info(f"Début de fix: Ajout source et suppression des doublons de {self.source}")
        # Ajout de source
        self.df = self.df.assign(source=self.source)

        # Transformation des acheteurs
        if "acheteur" in self.df.columns:
            self.df.loc[:,['id','acheteur']]= self.df.loc[:,['id','acheteur']].apply(update_id,axis=1)
        # Force type integer on column offresRecues
        if "offresRecues" in self.df.columns: 
            self.df['offresRecues'] = self.df['offresRecues'].fillna(0).astype(int)
        if "marcheInnovant" in self.df.columns:
            #print("TYPE COLONNE MARCHE INNOVANT:", self.df['marcheInnovant'].dtype)
            self.convert_boolean('marcheInnovant')
        if "attributionAvance" in self.df.columns:
            #print("TYPE COLONNE ATTRIBUTION AVANCEE:", self.df['attributionAvance'].dtype)
            self.convert_boolean('attributionAvance')
        if "sousTraitanceDeclaree" in self.df.columns:
            #print("TYPE COLONNE sous traitance:", self.df['sousTraitanceDeclaree'].dtype)
            self.convert_boolean('sousTraitanceDeclaree')
    
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