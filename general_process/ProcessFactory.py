from specific_process import * 
# Source non traitée pour l'instant
import logging




class ProcessFactory:

    def __init__(self, process=None, data_format=None):
        """Création de la liste des Processus qui correspondent chacun à une classe importée en début de document."""
        #self.processes = [PesProcess, AwsProcess, AifeProcess, EmarProcess, LyonProcess, MegaProcess]
        #self.processes = [MaxiProcess]
        # self.processes = [DecpAwsProcess, BfcProcess, PesProcess, AwsProcess, AifeProcess, EmarProcess, LyonProcess, MegaProcess]  # Supprimer le BRe car pris en compte dans megalisbre, DecpAwsProcess
        #self.processes = [DecpAwsProcess, EmarProcess, LyonProcess]  # Supprimer le BRe car pris en compte dans megalisbre, DecpAwsProcess
        #self.processes = [PesProcess, LyonProcess, EmarProcess]
        #self.processes = [AifeProcess]
        #self.processes = [MegaProcess]
        #self.processes = [AifeProcess]
        #self.processes = [PesProcess]
        self.processes = [SampleJsonProcess]
        #self.processes = [SampleXmlProcess]
        # if data_format=='2022':
        #    self.processes = [SampleXmlProcess] # For test ECO
        self.dataframes = []
        self.data_format = data_format
        # si on lance main avec un process spécifié :
        if process:
            for proc in self.processes:
                if proc.__name__ == process:
                    self.process = proc
                    break

    def run_processes(self):
        """Création d'une boucle (1 source=1 itération) qui appelle chacun des processus de chaque source."""
        for process in self.processes:
            loaded = 0
            try:
            #if True: #for debugonly
                logging.info(f"------------------------------{process.__name__}------------------------------")
                p = process(self.data_format)
                p.get()
                loaded = 1
                p.clean()
                loaded = 2
                p.convert()
                loaded = 3
                p.fix()
                loaded = 4
                #if self.data_format=='2022':
                #    p.comment()
                logging.info ("Ajout des données")
                self.dataframes.append(p.df)
                logging.info(f"----------------Fin du traitement {process.__name__}------------------------------")
            except Exception as err:
                if loaded>0:
                    logging.error(f"Erreur de traitement {loaded}  - {err}")
                else:
                    logging.error(f"Source introuvable - {err}")

    def run_process(self):
        """Lance un seul processus"""
        logging.info(f"------------------------------{self.process.__name__}------------------------------")
        p = self.process()
        p.get()
        p.clean()
        p.convert()
        p.fix()
        self.dataframes.append(p.df)
