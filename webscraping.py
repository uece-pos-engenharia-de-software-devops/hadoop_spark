import os
import time
import zipfile
import unicodedata
import re
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Utilitário para gerar nomes de arquivos amigáveis
def slugify(texto):
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^\w\s-]', '', texto).strip().lower()
    return re.sub(r'[-\s]+', '_', texto)

# Extrai e remove o ZIP
def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)

# Espera o fim do download verificando arquivos .part
def esperar_download_concluir(download_dir):
    while True:
        time.sleep(1)
        arquivos_part = [f for f in os.listdir(download_dir) if f.endswith(".part")]
        if not arquivos_part:
            arquivos_validos = [f for f in os.listdir(download_dir) if f.endswith(".csv") or f.endswith(".zip")]
            if arquivos_validos:
                arquivos_validos.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
                return arquivos_validos[0]  # Retorna o mais recente

# Renomeia arquivo para evitar duplicação
def renomear_arquivo(origem, novo_nome_base, destino_dir):
    novo_nome = f"{novo_nome_base}.csv"
    novo_path = os.path.join(destino_dir, novo_nome)
    i = 1
    while os.path.exists(novo_path):
        novo_nome = f"{novo_nome_base}_{i}.csv"
        novo_path = os.path.join(destino_dir, novo_nome)
        i += 1
    os.rename(origem, novo_path)

# Caminhos
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
geckodriver = os.path.join(diretorio_atual, "config", "geckodriver.exe")
download_dir = os.path.join(diretorio_atual, "data", "csv")

# Cria pasta se necessário
os.makedirs(download_dir, exist_ok=True)

# Configurações do Firefox
options = Options()
options.headless = False
options.set_preference("browser.download.folderList", 2)
options.set_preference("browser.download.dir", download_dir)
options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/csv,application/zip")

# Inicializa driver
driver = webdriver.Firefox(service=Service(geckodriver), options=options)
wait = WebDriverWait(driver, 30)

# Vai para a URL
URL = "https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp"
driver.get(URL)

# Expande recursos
try:
    btn_collapse = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'botao-collapse-Recursos')))
    btn_collapse.click()
except Exception:
    pass

# Filtros
tipoCombustivel = ["Álcool", "Diesel", "Combustíveis Automotivos"]
anos_desejados = [str(ano) for ano in range(2020, 2026)]

# Processa os títulos
try:    
    titles = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//h4')))

    for title in titles:
        titulo_texto = title.text.strip()

        if any(palavra in titulo_texto for palavra in tipoCombustivel) and any(ano in titulo_texto for ano in anos_desejados):
            try:
                print(f"Processando: {titulo_texto}")
                btn_xpath = f"//h4[text()='{titulo_texto}']/following-sibling::div//button[@id='btnDownloadUrl']"
                btn_download = wait.until(EC.element_to_be_clickable((By.XPATH, btn_xpath)))
                btn_download.click()

                # Espera o download concluir
                nome_arquivo = esperar_download_concluir(download_dir)
                caminho_arquivo = os.path.join(download_dir, nome_arquivo)

                # Se for zip, extrai e renomeia o CSV extraído
                if nome_arquivo.endswith(".zip"):
                    extract_zip(caminho_arquivo, download_dir)
                    # Pega o último CSV extraído
                    csv_extraido = esperar_download_concluir(download_dir)
                    renomear_arquivo(os.path.join(download_dir, csv_extraido), slugify(titulo_texto), download_dir)
                else:
                    renomear_arquivo(caminho_arquivo, slugify(titulo_texto), download_dir)

            except Exception as e:
                print(f"Erro ao processar {titulo_texto}: {e}")
                continue

except Exception as e:
    print(f"Erro geral: {e}")

# Finaliza o navegador
driver.quit()