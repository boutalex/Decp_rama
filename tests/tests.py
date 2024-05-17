import unittest
import general_process
from ..general_process.SourceProcess import *


class UneClasseDeTest(unittest.TestCase):

    def setUp(self):
        # Initialisation des données de test
        self.url = "https://example.com/api"
        self.new_ressources = [
            {"last_modified": "2023-01-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json"},
            {"last_modified": "2023-04-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json"}
        ]
        self.old_ressources = [
            {"last_modified": "2022-01-01T00:00:00.000000+00:00", "url": "https://example.com/collectivite1.xml"},
            {"last_modified": "2023-04-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite2.json"},
            {"last_modified": "2023-04-16T00:00:00.000000+00:00", "url": "https://example.com/collectivite3.json"},
            {"last_modified": "2023-01-02T00:00:00.000000+00:00", "url": "https://example.com/collectivite4.json"}
        ]
    
    def test_select_recent_url(self):
        resultats = general_process.SourceProcess.check_date_file(self.url, self.new_ressources, self.old_ressources)
        
        # Vérification du résultat
        expected_url = ["https://example.com/collectivite1.xml","https://example.com/collectivite4.json"]
        self.assertEqual(resultats, expected_url)



if __name__ == '__main__':
    unittest.main()