import unittest
import sys
import os
import json
import xmltodict
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Ajoutez le chemin du projet au sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from general_process.SourceProcess import SourceProcess
from specific_process.SampleJsonProcess import SampleJsonProcess

class UneClasseDeTest(unittest.TestCase):

    def setUp(self):
        # Initialisation des données de test
        self.process = SampleJsonProcess("2022")
    
        self.url = []
        
        self.df = pd.DataFrame()

        
    
    def test_select_recent_url(self):
        """
        Test de la fonction check date file, on récupère seulement les fichiers qui sont plus récents que les anciens fichiers téléchargés
        """
        
        old_ressources = [
            {"last_modified": "2022-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml", "title": "Collectivite 1"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json", "title": "Collectivite 2"},
            {"last_modified": "2023-04-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json", "title": "Collectivite 3"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json", "title": "Collectivite 4"}
            {"last_modified": "2022-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml", "title": "Collectivite 1"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json", "title": "Collectivite 2"},
            {"last_modified": "2023-04-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json", "title": "Collectivite 3"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json", "title": "Collectivite 4"}
        ]


        new_ressources = [
            {"last_modified": "2023-01-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml", "title": "Collectivite 1"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json", "title": "Collectivite 2"},
            {"last_modified": "2023-04-16T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json", "title": "Collectivite 3"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json", "title": "Collectivite 4"}
            {"last_modified": "2023-01-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml", "title": "Collectivite 1"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json", "title": "Collectivite 2"},
            {"last_modified": "2023-04-16T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json", "title": "Collectivite 3"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json", "title": "Collectivite 4"}
        ]
        title=[]
        resultats = self.process.check_date_file(self.url, title, new_ressources, old_ressources)

        # Vérification du résultat
        expected_url = (["https://example.com/collectivite1.xml","https://example.com/collectivite3.json"] ,["Collectivite 1", "Collectivite 3"])
        self.assertEqual(resultats, expected_url)
    
    # def test_date_before_2024(self):
    #     with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
    #         dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
    #     m = dico['marches']['marche']
    #     resultats= self.process.date_before_2024(m[0])
    #     expected_res=True
    #     self.assertEqual(resultats, expected_res)

    def test_date_after_2024(self):
        """
        Test de la fonction date_after_2024, on test si la fonction détecte si le fichier date d'après 2024 ou non  
        """
        with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process.date_after_2024(m)#ici on séléctionne seuelement le premier marché contenu dans m
        expected_res=False
        self.assertEqual(resultats, expected_res)

    def test_has_all_field_and_date_2024(self):
        """
        Test de la fonction has_all_field_and_date_2024, on test si les marchés/concessions possèdent bien les colonnes et les champs necessaires
        au format 2022 ainsi que leurs date.
        """
        with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process._has_all_field_and_date_2024(m,'marche')
        expected_res=False
        self.assertEqual(resultats, expected_res)

    def test_retain_wit_forma(self):
        """
        Test de la fonction retain_with_fomat, on vérifie que le dictionnaire en paramètre est valide par rapport à notre format 2022
        et trie les dictionnaire dans deux listes différentes, une pour les dico valide que l'on va traiter dans la suite du code
        et une autre pour les dico invalide que l'on va garder de coté
        """
        with open(f"tests/test_retain.json", encoding='utf-8') as json_file:
            dico = json.load(json_file)
        resultats= self.process._retain_with_format(dico,"testFile")
        expected_res ={ "marches": {
        "marche": [
            {
            "id": "TEST2022",
            "acheteur": {
                "id": "21750001630066"
            },
            "marcheInnovant": True,
            "origineUE": 0.1,
            "origineFrance": 0.1,
            "ccag": "Travaux",
            "offresRecues": 4,
            "formePrix": "Forfaitaire",
            "typesPrix": {
                "typePrix": [
                "Définitif ferme"
                ]
            },
            "considerationsEnvironnementales": {
                "considerationEnvironnementale": [
                "Clause environnementale"
                ]
            },
            "attributionAvance": True,
            "considerationsSociales": {
                "considerationSociale": [
                "Clause sociale",
                "Critère social"
                ]
            },
            "nature": "Marché",
            "objet": "envoi pour Json test 1",
            "codeCPV": "45000000-7",
            "techniques": {
                "technique": [
                "Concours",
                "Accord-cadre"
                ]
            },
            "modalitesExecution": 
            {
                "modaliteExecution": [
                    "Tranches"]}
            ,
            "idAccordCadre": "1234567890",
            "tauxAvance": 0,
            "titulaires": [
                {
                "titulaire": {
                    "id": "55204599900869",
                    "typeIdentifiant": "SIRET"
                }
                }
            ],
            "typeGroupementOperateurs": "Pas de groupement",
            "sousTraitanceDeclaree": True,
            "datePublicationDonnees": "2024-05-25",
            "actesSousTraitance": [
                {
                "acteSousTraitance": {
                    "id": 1,
                    "sousTraitant": {
                    "id": "214356",
                    "typeIdentifiant": "SIRET"
                    },
                    "dureeMois": 12,
                    "dateNotification": "2024-10-11",
                    "montant": 123456.89,
                    "variationPrix": "Révisable",
                    "datePublicationDonnees": "2024-10-12"
                }
                }
            ],
            "modifications": [
                {
                "modification": {
                    "id": 1,
                    "dureeMois": 12,
                    "montant": 123457.87,
                    "titulaires": [
                    {
                        "titulaire": {
                        "id": "868768687576575",
                        "typeIdentifiant": "SIRET"
                        }
                    }
                    ],
                    "dateNotificationModification": "2024-10-18",
                    "datePublicationDonneesModification": "2024-10-20"
                }
                }
            ],
            "modificationsActesSousTraitance": [
                {
                "modificationActeSousTraitance": {
                    "id": 1,
                    "dureeMois": 12,
                    "dateNotificationModificationSousTraitance": "2024-10-20",
                    "montant": 23465.87,
                    "datePublicationDonnees": "2024-12-15"
                }
                }
            ],

            "procedure": "Appel d'offres ouvert",
            "lieuExecution": {
                "code": "75000",
                "typeCode": "Code postal"
            },
            "dureeMois": 48,
            "dateNotification": "2024-05-15",
            "montant": 575000
            }
            ]
            }
        }
        self.assertDictEqual(resultats,expected_res)

    def test_convert(self):
        """
        On test la fonction convert, on part du principe que la fonction convert_prestataire fonctionne et renvoi un résultat valide
        donc on créer un dataframe qui correspond au résultat de la fonction convert_prestataire et on en créer un autre qui 
        correspond au résultat de notre fonction convert, on vérifie que ces deux dataframe sont identiques
        """
        self.process.convert()
        expected_res=True
        df_notre= self.process.df
        df_prestataire = self.process.convert_prestataire()
        resultats = df_notre.equals(df_prestataire)
        self.assertEqual(resultats, expected_res)

if __name__ == '__main__':
    unittest.main()

    classe = UneClasseDeTest()
    classe.test_select_recent_url()