import os
import zipfile
from typing import List, Tuple


class ExtrairZip:
    def __init__(self, logger, dcParameter):
        self.logger = logger
        self.dcParameter = dcParameter
        self.folderdownloads = self.dcParameter['folderdownloads']


    def extrair_zip(self, caminho_zip: str, destino: str) -> bool:
        try:
            if not os.path.isfile(caminho_zip):
                self.logger.log_error("extrair_zip", f"Arquivo ZIP não encontrado: {caminho_zip}")
                return False

            if not zipfile.is_zipfile(caminho_zip):
                self.logger.log_error("extrair_zip", f"O arquivo não é um ZIP válido: {caminho_zip}")
                return False

            os.makedirs(destino, exist_ok=True)
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(destino)

            self.logger.log_info('extrair_zip', f"Arquivo {caminho_zip} extraído para {destino}")
            return True
        except Exception as e:
            self.logger.log_error('extrair_zip', f"Erro ao extrair {caminho_zip}: {e}")
            return False


    def extrair_zips_em_pasta(self, pasta: str) -> List[Tuple[str, bool]]:
        """
        Percorre uma pasta, extrai todos os arquivos .zip encontrados para a própria pasta
        e exclui o .zip após extração.
        """
        resultados = []
        if not os.path.isdir(pasta):
            self.logger.log_warning("extrair_zips_em_pasta", f"Pasta não encontrada: {pasta}")
            return resultados

        try:
            for nome in os.listdir(pasta):
                caminho = os.path.join(pasta, nome)
                if os.path.isfile(caminho) and nome.lower().endswith(".zip"):
                    sucesso = self.extrair_zip(caminho_zip=caminho, destino=pasta)
                    resultados.append((caminho, sucesso))
                    if sucesso:
                        try:
                            os.remove(caminho)
                            self.logger.log_info('extrair_zips_em_pasta', f"ZIP removido após extração: {caminho}")
                        except Exception as e_rm:
                            self.logger.log_error('extrair_zips_em_pasta', f"Erro ao remover ZIP {caminho}: {e_rm}")
        except Exception as e:
            self.logger.log_error('extrair_zips_em_pasta', f"Erro ao percorrer pasta {pasta}: {e}")

        return resultados

    # def extrair_zips_em_subpastas(self, raiz: str) -> List[Tuple[str, bool]]:
    #     """
    #     Percorre todas as subpastas de 'raiz' e, em cada subpasta, extrai os zips para a própria pasta,
    #     excluindo o zip em seguida.
    #     """
    #     resultados = []
    #     if not os.path.isdir(raiz):
    #         self.logger.log_warning("extrair_zips_em_subpastas", f"Pasta raiz não encontrada: {raiz}")
    #         return resultados

    #     try:
    #         for entry in os.listdir(raiz):
    #             subpasta = os.path.join(raiz, entry)
    #             if os.path.isdir(subpasta):
    #                 self.logger.log_info('extrair_zips_em_subpastas', f"Processando subpasta: {subpasta}")
    #                 res = self.extrair_zips_em_pasta(subpasta)
    #                 resultados.extend(res)
    #     except Exception as e:
    #         self.logger.log_error('extrair_zips_em_subpastas', f"Erro ao percorrer subpastas de {raiz}: {e}")

    #     return resultados

    # def extrair_todos_em_folderdownloads(self) -> List[Tuple[str, bool]]:
    #     """
    #     Conveniência: percorre self.folderdownloads e extrai os zips de todas as subpastas.
    #     """
    #     return self.extrair_zips_em_subpastas(self.folderdownloads)
