import os
import sys
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from dateutil import parser

# Configuração via variáveis de ambiente
LOVABLE_ENDPOINT = os.getenv("LOVABLE_ENDPOINT")
LOVABLE_API_KEY = os.getenv("LOVABLE_API_KEY")

# Feeds RSS que vamos consumir
FEEDS = [
    {
        "name": "suno",
        "domain": "suno.com.br",
        "url": "https://www.suno.com.br/noticias/feed/",
    },
    {
        "name": "infomoney",
        "domain": "infomoney.com.br",
        "url": "https://www.infomoney.com.br/ultimas-noticias/feed/",
    },
]


def fetch_feed_xml(url: str) -> str | None:
    """Baixa o conteúdo XML de um feed RSS"""
    try:
        print(f"🌐 Baixando feed: {url}")
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            print(f"❌ Erro HTTP {resp.status_code} ao acessar {url}")
            return None
        return resp.text
    except requests.RequestException as e:
        print(f"❌ Erro de rede ao acessar {url}: {e}")
        return None


def parse_rss_items(xml_text: str, source_domain: str) -> list[dict]:
    """Converte itens de um RSS em dicionários normalizados"""
    items = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"❌ Erro ao parsear XML: {e}")
        return items

    # RSS padrão: <rss><channel><item>...</item></channel></rss>
    channel = root.find("channel")
    if channel is None:
        # Alguns feeds podem ter estrutura diferente; tentamos direto <item>
        channel = root

    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()

        # Descrição / conteúdo
        description = (item.findtext("description") or "").strip()

        # Alguns feeds usam <content:encoded>, mas xml.etree não lida bem com namespace sem mais trabalho.
        # Vamos priorizar description, que já costuma ser suficiente pro seu caso.
        text = description

        # Data de publicação
        raw_date = (
            item.findtext("pubDate")
            or item.findtext("date")
        )

        if raw_date:
            try:
                dt = parser.parse(raw_date)
                published_at = dt.isoformat()
            except Exception as e:
                print(f"⚠️ Erro ao parsear data '{raw_date}': {e}")
                published_at = datetime.utcnow().isoformat()
        else:
            published_at = datetime.utcnow().isoformat()

        # Tags (categorias do RSS)
        tags = []
        for cat in item.findall("category"):
            if cat.text:
                tags.append(cat.text.strip())

        # Só vale a pena enviar se tiver título e link
        if not title or not link:
            continue

        items.append(
            {
                "title": title,
                "url": link,
                "published_at": published_at,
                "source": source_domain,
                "text": text,
                "tags": tags,
            }
        )

    return items


def collect_all_news() -> list[dict]:
    """Busca e consolida notícias de todos os feeds"""
    all_items: list[dict] = []
    seen_urls: set[str] = set()

    for feed in FEEDS:
        xml_text = fetch_feed_xml(feed["url"])
        if not xml_text:
            continue

        items = parse_rss_items(xml_text, feed["domain"])
        print(f"✅ {len(items)} notícias brutas de {feed['name']}")

        # Deduplicar por URL
        for it in items:
            if it["url"] in seen_urls:
                continue
            seen_urls.add(it["url"])
            all_items.append(it)

    return all_items


def send_to_lovable(items: list[dict]) -> bool:
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
        batch_size = 50
        total_processed = 0
        total_errors = 0

        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            print(f"   Batch {i // batch_size + 1}: {len(batch)} itens...")

            resp = requests.post(
                LOVABLE_ENDPOINT,
                json=batch,
                headers=headers,
                timeout=60,
            )

            if resp.status_code == 200:
                try:
                    result = resp.json()
                except json.JSONDecodeError:
                    print(f"   ⚠️ Resposta não JSON: {resp.text}")
                    result = {}

                processed = result.get("processed", 0)
                errors = result.get("errors", 0)
                total_processed += processed
                total_errors += errors
                print(f"   ✅ Processadas: {processed}, Erros: {errors}")
            else:
                print(f"   ❌ Erro HTTP {resp.status_code}: {resp.text}")
                total_errors += len(batch)

        print("\n📊 Resumo Final:")
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
    print("🚀 Iniciando ingestão de notícias via RSS (Suno + Infomoney)")
    print("=" * 50)

    # Passo 1: Buscar e consolidar notícias dos feeds
    print("\n📰 Carregando notícias dos feeds RSS...")
    items = collect_all_news()
    print(f"📊 Total de notícias normalizadas: {len(items)}")

    if not items:
        print("⚠️ Nenhuma notícia encontrada. Verifique os feeds.")
        sys.exit(0)

    # Passo 2: Enviar para o Lovable
    success = send_to_lovable(items)

    if success:
        print("\n✅ Ingestão concluída com sucesso!")
        sys.exit(0)
    else:
        print("\n❌ Ingestão falhou!")
        sys.exit(1)


if __name__ == "__main__":
    main()



