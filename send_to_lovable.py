import json
import pathlib
import subprocess
import requests
from datetime import datetime
from dateutil import parser
import os
import sys

# Configuração via variáveis de ambiente
LOVABLE_ENDPOINT = os.getenv("LOVABLE_ENDPOINT")
LOVABLE_API_KEY = os.getenv("LOVABLE_API_KEY")

# Mapeamento de diretórios de crawler para domínios
SOURCE_MAP = {
    "infomoney": "infomoney.com.br",
    "money_times": "moneytimes.com.br",
    "suno": "suno.com.br",
    "fundamentus": "fundamentus.com.br",
    "b3": "b3.com.br",
}

def run_crawlers():
    """Clona o repositório e executa os crawlers"""
    repo_dir = pathlib.Path("BrazilianFinancialNews")
    
    # Clona o repo se não existir
    if not repo_dir.exists():
        print("📥 Clonando repositório BrazilianFinancialNews...")
        subprocess.run(
            ["git", "clone", "https://github.com/mso13/BrazilianFinancialNews.git"],
            check=True
        )
    else:
        print("📁 Repositório já existe, atualizando...")
        subprocess.run(["git", "pull"], cwd=repo_dir, check=True)
    
    # Lista de crawlers para executar
    crawlers = [
        "infomoney",
        "money_times", 
        "suno",
        # "fundamentus",  # Descomentar se quiser incluir
        # "b3",           # Descomentar se quiser incluir
    ]
    
    for crawler in crawlers:
        crawler_path = repo_dir / "src" / "crawlers" / crawler / "main.py"
        if crawler_path.exists():
            print(f"🕷️ Executando crawler: {crawler}")
            try:
                subprocess.run(
                    ["python", str(crawler_path)],
                    cwd=repo_dir,
                    check=True,
                    timeout=300  # 5 minutos máximo por crawler
                )
            except subprocess.TimeoutExpired:
                print(f"⚠️ Timeout no crawler {crawler}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Erro no crawler {crawler}: {e}")
        else:
            print(f"⚠️ Crawler não encontrado: {crawler_path}")
    
    return repo_dir

def normalize_item(item: dict, source_domain: str) -> dict:
    """Converte item do crawler para o formato esperado pelo Lovable"""
    
    # Título - tenta múltiplos campos
    title = (
        item.get("titulo") or 
        item.get("title") or 
        item.get("manchete") or 
        ""
    ).strip()
    
    # URL
    url = (item.get("url") or item.get("link") or "").strip()
    
    # Conteúdo/Texto
    text = (
        item.get("conteudo") or 
        item.get("texto") or 
        item.get("text") or 
        item.get("resumo") or 
        ""
    ).strip()
    
    # Tags - combina tags existentes com categoria se disponível
    tags = item.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    
    categoria = item.get("categoria") or item.get("category")
    if categoria and categoria not in tags:
        tags.append(categoria)
    
    # Data de publicação - tenta múltiplos campos e formatos
    raw_date = (
        item.get("data_publicacao") or 
        item.get("data") or 
        item.get("published_at") or
        item.get("date")
    )
    
    if raw_date:
        try:
            # Tenta fazer parse da data
            if isinstance(raw_date, str):
                dt = parser.parse(raw_date, dayfirst=True)
            else:
                dt = raw_date
            published_at = dt.isoformat()
        except Exception as e:
            print(f"⚠️ Erro ao parsear data '{raw_date}': {e}")
            published_at = datetime.utcnow().isoformat()
    else:
        published_at = datetime.utcnow().isoformat()
    
    return {
        "title": title,
        "url": url,
        "published_at": published_at,
        "source": source_domain,
        "text": text,
        "tags": tags,
    }

def load_all_news(repo_dir: pathlib.Path) -> list:
    """Carrega e normaliza todas as notícias dos crawlers"""
    all_items = []
    
    for crawler_name, source_domain in SOURCE_MAP.items():
        data_dir = repo_dir / "src" / "crawlers" / crawler_name / "data"
        
        if not data_dir.exists():
            print(f"📂 Diretório de dados não encontrado: {data_dir}")
            continue
        
        # Busca todos os arquivos JSON
        json_files = list(data_dir.glob("*.json"))
        
        if not json_files:
            print(f"📂 Nenhum JSON encontrado em: {data_dir}")
            continue
        
        # Pega o arquivo mais recente
        latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
        print(f"📄 Lendo: {latest_file}")
        
        try:
            with latest_file.open(encoding="utf-8") as f:
                raw_data = json.load(f)
            
            # Pode ser lista ou dict com items
            if isinstance(raw_data, list):
                items = raw_data
            elif isinstance(raw_data, dict):
                items = raw_data.get("items") or raw_data.get("news") or [raw_data]
            else:
                items = []
            
            for item in items:
                normalized = normalize_item(item, source_domain)
                # Só adiciona se tiver título e URL
                if normalized["title"] and normalized["url"]:
                    all_items.append(normalized)
                    
            print(f"✅ {len(items)} notícias carregadas de {crawler_name}")
            
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao ler JSON {latest_file}: {e}")
        except Exception as e:
            print(f"❌ Erro inesperado ao processar {latest_file}: {e}")
    
    return all_items

def send_to_lovable(items: list) -> bool:
    """Envia as notícias para a Edge Function do Lovable"""
    
    if not LOVABLE_ENDPOINT:
        print("❌ LOVABLE_ENDPOINT não configurado!")
        return False
    
    if not items:
        print("⚠️ Nenhuma notícia para enviar")
        return True
    
    headers = {
        "Content-Type": "application/json",
    }
    
    if LOVABLE_API_KEY:
        headers["apikey"] = LOVABLE_API_KEY
    
    print(f"📤 Enviando {len(items)} notícias para {LOVABLE_ENDPOINT}...")
    
    try:
        # Envia em batches de 50 para evitar timeout
        batch_size = 50
        total_processed = 0
        total_errors = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            print(f"   Batch {i // batch_size + 1}: {len(batch)} itens...")
            
            resp = requests.post(
                LOVABLE_ENDPOINT, 
                json=batch, 
                headers=headers,
                timeout=60
            )
            
            if resp.status_code == 200:
                result = resp.json()
                total_processed += result.get("processed", 0)
                total_errors += result.get("errors", 0)
                print(f"   ✅ Processadas: {result.get('processed', 0)}, Erros: {result.get('errors', 0)}")
            else:
                print(f"   ❌ Erro HTTP {resp.status_code}: {resp.text}")
                total_errors += len(batch)
        
        print(f"\n📊 Resumo Final:")
        print(f"   Total enviado: {len(items)}")
        print(f"   Processadas com sucesso: {total_processed}")
        print(f"   Erros/Duplicatas: {total_errors}")
        
        return True
        
    except requests.Timeout:
        print("❌ Timeout ao enviar para o Lovable")
        return False
    except requests.RequestException as e:
        print(f"❌ Erro de conexão: {e}")
        return False

def main():
    print("=" * 50)
    print("🚀 Iniciando ingestão de notícias financeiras")
    print("=" * 50)
    
    # Passo 1: Rodar crawlers
    repo_dir = run_crawlers()
    
    # Passo 2: Carregar e normalizar notícias
    print("\n📰 Carregando notícias...")
    items = load_all_news(repo_dir)
    print(f"📊 Total de notícias: {len(items)}")
    
    if not items:
        print("⚠️ Nenhuma notícia encontrada. Verifique os crawlers.")
        sys.exit(0)
    
    # Passo 3: Enviar para o Lovable
    success = send_to_lovable(items)
    
    if success:
        print("\n✅ Ingestão concluída com sucesso!")
        sys.exit(0)
    else:
        print("\n❌ Ingestão falhou!")
        sys.exit(1)

if __name__ == "__main__":
    main()
