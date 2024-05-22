import unittest
import sys
import os
import json
import xmltodict

# Ajoutez le chemin du projet au sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from general_process.SourceProcess import SourceProcess
from specific_process.SampleJsonProcess import SampleJsonProcess

class UneClasseDeTest(unittest.TestCase):

    def setUp(self):
        # Initialisation des données de test
        self.process = SampleJsonProcess("2022")
    
        self.url = []

        
    
    def test_select_recent_url(self):
        
        old_ressources = [
            {"last_modified": "2022-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json"},
            {"last_modified": "2023-04-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json"}
        ]
        new_ressources = [
            {"last_modified": "2023-01-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json"},
            {"last_modified": "2023-04-16T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json"}
        ]
        resultats = self.process.check_date_file(self.url, new_ressources, old_ressources)

        # Vérification du résultat
        expected_url = ["https://example.com/collectivite1.xml","https://example.com/collectivite3.json"]
        self.assertEqual(resultats, expected_url)
    
    # def test_date_before_2024(self):
    #     with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
    #         dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
    #     m = dico['marches']['marche']
    #     resultats= self.process.date_before_2024(m[0])
    #     expected_res=True
    #     self.assertEqual(resultats, expected_res)

    def test_date_after_2024(self):
        with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process.date_after_2024(m)#ici on séléctionne seuelement le premier marché contenu dans m
        expected_res=False
        self.assertEqual(resultats, expected_res)

    def test_has_all_field_and_date_2024(self):
        with open(f"tests/test_before2024.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process._has_all_field_and_date_2024(m,'marche')
        expected_res=False
        self.assertEqual(resultats, expected_res)

    def test_retain_wit_forma(self):
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



if __name__ == '__main__':
    unittest.main()