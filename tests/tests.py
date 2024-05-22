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
    
    def test_date_before_2024(self):
        with open(f"sources/tests/decp-2014.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process.date_before_2024(m[0])
        expected_res=True
        self.assertEqual(resultats, expected_res)

    def test_date_after_2024(self):
        with open(f"sources/tests/decp-2014.xml", encoding='utf-8') as xml_file:
            dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
        m = dico['marches']['marche']
        resultats= self.process.date_after_2024(m[0],'dateNotification','datePublicationDonnees')
        expected_res=False
        self.assertEqual(resultats, expected_res)



if __name__ == '__main__':
    unittest.main()