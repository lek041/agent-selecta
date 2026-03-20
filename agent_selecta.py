import shutil
from tqdm import tqdm
import random
import time
import re
import csv
import sqlite3
import requests
import acoustid
from datetime import date
from mutagen import File
from pathlib import Path

PASTAS_ORIGEM = [
    "/Users/SEU_USUARIO/Music/SELECTA",   # ← pasta(s) de origem com seus arquivos novos
]
PASTA_ARCHIVE = "/Users/SEU_USUARIO/Music/ARCHIVE"               # ← biblioteca organizada
PASTA_UNKNOW  = "/Users/SEU_USUARIO/Music/Z_UNKNOW"              # ← arquivos não identificados
PASTA_LOST    = "/Users/SEU_USUARIO/Music/LOST_TRACKS_REPORT"    # ← logs de erro
PASTA_REPORT  = "/Users/SEU_USUARIO/Music/THE_ARCHIVE_REPORT"    # ← relatórios CSV
SQLITE_PATH   = "/Users/SEU_USUARIO/Music/spotify_agent.db"      # ← gerado pelo converter_spotify.py

# ── Chaves de API ─────────────────────────────────────────────────────────────
# Obtenha as suas em:
#   Last.fm   → https://www.last.fm/api/account/create
#   Discogs   → https://www.discogs.com/settings/developers
#   AcoustID  → https://acoustid.org/login  (requer fpcalc instalado)
LASTFM_KEY    = "SUA_LASTFM_KEY_AQUI"
DISCOGS_TOKEN = "SEU_DISCOGS_TOKEN_AQUI"
ACOUSTID_KEY  = "SUA_ACOUSTID_KEY_AQUI"
SLEEP_MB      = 1.0
SLEEP_API     = 0.3

SEPARADORES = r"\s*(ft\.|feat\.|b2b|vs\.?|with|\be\b)\s*"
PREFIXOS    = r"^(mc|dj|dr\.|the|os|as|\ba\b|\bo\b)\s+"
SIMBOLOS    = re.compile(r"^[\s\~\-\_\.\,\!\?\#\@\*\+\=\|\(\)\[\]\{\}\/\\]+$")

_api_falhas = {"musicbrainz": 0, "lastfm": 0, "discogs": 0, "deezer": 0}
_API_MAX_FALHAS = 5

def api_ok(nome):    return _api_falhas[nome] < _API_MAX_FALHAS
def api_falhou(nome):
    _api_falhas[nome] += 1
    if _api_falhas[nome] >= _API_MAX_FALHAS:
        print(f"  [AVISO] API {nome} desativada apos {_API_MAX_FALHAS} falhas consecutivas")
def api_sucesso(nome): _api_falhas[nome] = 0

ALIASES = {
    "makaveli": "2pac", "orange deluxe": "2pac",
    "notorious b.i.g": "notorious big", "notorious b.i.g.": "notorious big",
    "biggie smalls": "notorious big", "the notorious b.i.g": "notorious big",
    "the notorious b.i.g.": "notorious big",
}

def resolver_alias(artista):
    nome = artista.lower().strip()
    if nome in ALIASES: return ALIASES[nome]
    for alias, canonical in ALIASES.items():
        if nome.startswith(alias): return canonical
    return artista

def banner():
    print()
    print("  ┌──────────────────────────────────────────────────────────────────────────────┐")
    print("  │                                                                              │")
    print("  │  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  │")
    print("  │                                                                              │")
    print("  │  ███████╗███████╗██╗     ███████╗ ██████╗████████╗ █████╗                  │")
    print("  │  ██╔════╝██╔════╝██║     ██╔════╝██╔════╝╚══██╔══╝██╔══██╗                 │")
    print("  │  ███████╗█████╗  ██║     █████╗  ██║        ██║   ███████║                 │")
    print("  │  ╚════██║██╔══╝  ██║     ██╔══╝  ██║        ██║   ██╔══██║                 │")
    print("  │  ███████║███████╗███████╗███████╗╚██████╗   ██║   ██║  ██║                 │")
    print("  │  ╚══════╝╚══════╝╚══════╝╚══════╝ ╚═════╝   ╚═╝   ╚═╝  ╚═╝                 │")
    print("  │                                                                              │")
    print("  │  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  │")
    print("  │                                                                              │")
    print("  │            A G E N T  S E L E C T A   v 2 . 0                              │")
    print("  │                t h e  c a t a l o g e r                                     │")
    print("  │                                                                              │")
    print("  │   AcoustID  ·  MusicBrainz  ·  Last.fm  ·  Discogs  ·  Deezer             │")
    print("  │                       Spotify DB                                             │")
    print("  │                                                                              │")
    print("  └──────────────────────────────────────────────────────────────────────────────┘")
    print()

def escolher_modo():
    print("Como deseja executar?\n")
    print("  [1] UPDATE  -> processa SELECTA automaticamente -> ARCHIVE")
    print("  [2] SCAN    -> processa SELECTA com confirmacao do usuario")
    print("  [3] REVIEW  -> revisa ARCHIVE e Z_UNKNOW")
    print("  [4] AUDIT   -> verifica e corrige tags de todos os arquivos no ARCHIVE")
    print("  [5] RESCUE  -> arquivos com similaridade 0.0, todos precisam da sua ajuda\n")
    while True:
        escolha = input("Escolha (1, 2, 3, 4 ou 5): ").strip()
        if escolha == "1":
            print("\n[ UPDATE - processando SELECTA automaticamente ]\n")
            return "update"
        elif escolha == "2":
            print("\n[ SCAN - processando SELECTA com sua confirmacao ]\n")
            return "scan"
        elif escolha == "3":
            return "review"
        elif escolha == "4":
            print("\n[ AUDIT - verificando e corrigindo tags do ARCHIVE ]\n")
            return "audit"
        elif escolha == "5":
            print("\n[ RESCUE - arquivos com similaridade 0.0 precisam da sua ajuda ]\n")
            return "rescue"
        print("Digite 1, 2, 3, 4 ou 5.")

def sanitizar(nome):
    for c in ["/", "\\", ":", "*", "?", chr(34), "<", ">", "|"]:
        nome = nome.replace(c, "_")
    nome = nome.lower().strip()
    nome = re.sub(r"\s+", " ", nome)
    nome = nome.replace("_", " ")
    return nome or ""

NOMES_INVALIDOS = {"desconhecido", "unknown", "various", "various artists", "va", "artista desconhecido"}

def artista_valido(nome):
    if not nome or not nome.strip(): return False
    if SIMBOLOS.match(nome.strip()): return False
    if nome.strip().isdigit(): return False
    if nome.strip().lower() in NOMES_INVALIDOS: return False
    return True

def extrair_principal(nome):
    return re.split(SEPARADORES, nome)[0].strip()

def remover_prefixo(nome):
    resultado = re.sub(PREFIXOS, "", nome, flags=re.IGNORECASE).strip()
    return resultado if resultado else nome

def primeiro_artista(artista, verificar_archive=False):
    """Extrai o primeiro artista de colaboracoes para usar como pasta principal.
    Se verificar_archive=True, checa se o nome completo ja existe como pasta
    antes de cortar (evita cortar duos como 'Walker & Royce')."""
    if verificar_archive and artista:
        pasta_archive = Path(PASTA_ARCHIVE)
        ini = artista[0].upper() if artista[0].isalpha() else "#"
        pasta_completa = pasta_archive / ini / artista.lower().strip()
        if pasta_completa.exists():
            return artista.strip()
    separadores_pasta = re.split(
        r'\s*(&|,|\bwith\b|\band\b|\bcom\b|\bvs\.?|\bfeat\.?|\bft\.?|\bb2b\b|\bx\b)\s*',
        artista, flags=re.IGNORECASE
    )
    # Tenta combinar partes progressivamente para encontrar duo/trio no ARCHIVE
    # "walker & royce & kyle watson" → testa "walker & royce", depois "walker"
    pasta_archive = Path(PASTA_ARCHIVE)
    acumulado = ""
    for idx, parte in enumerate(separadores_pasta):
        if idx % 2 == 1:  # separador
            acumulado += " " + parte.strip() + " "
            continue
        acumulado = (acumulado + parte).strip()
        if not acumulado: continue
        ini = acumulado[0].upper() if acumulado[0].isalpha() else "#"
        if (pasta_archive / ini / acumulado.lower()).exists():
            return acumulado  # encontrou pasta existente — retorna ate aqui
    # Sem pasta existente — retorna apenas o primeiro segmento
    resultado = separadores_pasta[0].strip() if separadores_pasta else artista
    return resultado if resultado else artista.strip()

def letra_inicial(artista):
    nome = remover_prefixo(primeiro_artista(artista)).strip()
    if not nome: return "#"
    if nome[0].isdigit(): return "#"
    return nome[0].upper()

def conectar_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def buscar_rowid_artista(conn, artista):
    cur = conn.execute("SELECT rowid FROM artists WHERE LOWER(name) = ? LIMIT 1", (artista.lower().strip(),))
    row = cur.fetchone()
    return row["rowid"] if row else None

def buscar_album_sqlite(conn, artista_rowid):
    cur = conn.execute("""
        SELECT a.name FROM artist_albums aa
        JOIN albums a ON aa.album_rowid = a.rowid
        WHERE aa.artist_rowid = ? AND aa.is_appears_on = 0
        ORDER BY a.release_date ASC LIMIT 1
    """, (artista_rowid,))
    row = cur.fetchone()
    return sanitizar(row["name"]) if row else None

def limpar_titulo(filepath):
    nome = Path(filepath).stem
    nome = re.sub(r"^\d+[\s\.\-_]+", "", nome).strip()
    nome = nome.replace("_", " ")
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome

# ============================================================
# SIMILARITY2
# ============================================================
def similarity2(a, b):
    if not a or not b: return 0.0
    a, b = a.lower().strip(), b.lower().strip()
    if a == b: return 1.0
    pa, pb = set(a.split()), set(b.split())
    if not pa or not pb: return 0.0
    return len(pa & pb) / len(pa | pb)

def similarity_audit(tag, pasta):
    """Compara tag com nome da pasta normalizando hifens, underscores e pontos.
    Usa jaccard E startswith para capturar casos como:
    tag='wc no beat, nego do borel' pasta='wc no beat' -> score alto
    """
    if not tag or not pasta: return 0.0
    def norm(s):
        s = s.lower().strip()
        s = re.sub(r"[-_\.]", " ", s)
        # Remove tudo apos virgula ou & (artistas colaborativos na tag)
        s = re.split(r"[,&]", s)[0].strip()
        s = re.sub(r"\s+", " ", s).strip()
        return s
    tag_n  = norm(tag)
    pasta_n = norm(pasta)
    if not tag_n or not pasta_n: return 0.0
    if tag_n == pasta_n: return 1.0
    # startswith: tag comeca com o nome da pasta ou vice-versa
    if tag_n.startswith(pasta_n) or pasta_n.startswith(tag_n):
        return 0.9
    return similarity2(tag_n, pasta_n)

# ============================================================
# MBID LOOKUP
# ============================================================
def get_mbid_tag(filepath):
    try:
        from mutagen.id3 import ID3, TXXX
        from mutagen.flac import FLAC
        from mutagen.mp4 import MP4
        fp = str(filepath).lower()
        if fp.endswith(".mp3"):
            tags = ID3(filepath)
            for key in tags.keys():
                if "musicbrainz" in key.lower() and "artist" in key.lower():
                    return str(tags[key])
            for t in tags.getall("TXXX"):
                if "musicbrainz artist id" in t.desc.lower():
                    return t.text[0] if t.text else None
        elif fp.endswith(".flac"):
            tags = FLAC(filepath)
            for key in ["musicbrainz_artistid", "MUSICBRAINZ_ARTISTID"]:
                if key in tags: return tags[key][0]
        elif fp.endswith((".m4a", ".mp4")):
            tags = MP4(filepath)
            for key in tags.keys():
                if "musicbrainz" in key.lower():
                    return str(tags[key][0])
    except Exception:
        pass
    return None

def buscar_artista_por_mbid(mbid):
    if not mbid: return None
    try:
        url = f"https://musicbrainz.org/ws/2/artist/{mbid}?fmt=json"
        headers = {"User-Agent": "AgentSelecta/2.0 ( selecta@dj.local )"}
        r = requests.get(url, headers=headers, timeout=8)
        time.sleep(SLEEP_MB)
        if r.status_code == 200:
            return r.json().get("name")
    except Exception:
        pass
    return None

# ============================================================
# TAG REWRITING
# ============================================================
def reescrever_tags(filepath, artista=None, album=None, titulo=None):
    try:
        fp = str(filepath).lower()
        if fp.endswith(".mp3"):
            from mutagen.id3 import ID3, TPE1, TALB, TIT2, error as ID3Error
            try: tags = ID3(filepath)
            except ID3Error: tags = ID3()
            if artista: tags["TPE1"] = TPE1(encoding=3, text=artista)
            if album:   tags["TALB"] = TALB(encoding=3, text=album)
            if titulo:  tags["TIT2"] = TIT2(encoding=3, text=titulo)
            tags.save(filepath)
            return True
        elif fp.endswith(".flac"):
            from mutagen.flac import FLAC
            tags = FLAC(filepath)
            if artista: tags["artist"] = [artista]
            if album:   tags["album"]  = [album]
            if titulo:  tags["title"]  = [titulo]
            tags.save()
            return True
        elif fp.endswith((".m4a", ".mp4")):
            from mutagen.mp4 import MP4
            tags = MP4(filepath)
            if artista: tags["\xa9ART"] = [artista]
            if album:   tags["\xa9alb"] = [album]
            if titulo:  tags["\xa9nam"] = [titulo]
            tags.save()
            return True
        elif fp.endswith(".ogg"):
            from mutagen.oggvorbis import OggVorbis
            tags = OggVorbis(filepath)
            if artista: tags["artist"] = [artista]
            if album:   tags["album"]  = [album]
            if titulo:  tags["title"]  = [titulo]
            tags.save()
            return True
        elif fp.endswith(".aac"):
            from mutagen.mp4 import MP4
            try:
                tags = MP4(filepath)
                if artista: tags["\xa9ART"] = [artista]
                if album:   tags["\xa9alb"] = [album]
                if titulo:  tags["\xa9nam"] = [titulo]
                tags.save()
                return True
            except Exception:
                pass
        elif fp.endswith(".wav"):
            from mutagen.wave import WAVE
            from mutagen.id3 import TPE1, TALB, TIT2
            try:
                tags = WAVE(filepath)
                if artista: tags["TPE1"] = TPE1(encoding=3, text=artista)
                if album:   tags["TALB"] = TALB(encoding=3, text=album)
                if titulo:  tags["TIT2"] = TIT2(encoding=3, text=titulo)
                tags.save()
                return True
            except Exception:
                pass
    except Exception:
        pass
    return False

# ============================================================
# DELETAR PASTAS VAZIAS
# ============================================================
def deletar_pastas_vazias(raiz):
    raiz = Path(raiz)
    removidas = 0
    for pasta in sorted(raiz.rglob("*"), reverse=True):
        if pasta.is_dir():
            try:
                pasta.rmdir()
                removidas += 1
            except OSError:
                pass
    return removidas

def mover(arquivo, destino_dir):
    destino_dir.mkdir(parents=True, exist_ok=True)
    novo_caminho = destino_dir / arquivo.name
    contador = 1
    while novo_caminho.exists():
        novo_caminho = destino_dir / f"{arquivo.stem}_{contador}{arquivo.suffix}"
        contador += 1
    shutil.move(str(arquivo), novo_caminho)

# ============================================================
# APIS
# ============================================================
def get_artista_acoustid(filepath):
    try:
        duration, fp = acoustid.fingerprint_file(str(filepath))
        res = acoustid.lookup(ACOUSTID_KEY, fp, duration,
                              meta="recordings releasegroups releases tracks compress sources", timeout=10)
        if res.get("status") != "ok" or not res.get("results"): return None, None, None
        candidatos = [r for r in res["results"] if r.get("score", 0) >= 0.5]
        if not candidatos: return None, None, None
        melhor = candidatos[0]
        recordings = melhor.get("recordings", [])
        if not recordings: return None, None, None
        max_sources = max((r.get("sources", 1) for r in recordings), default=1)
        melhor_rec = max(recordings, key=lambda r: (r.get("sources", 1) / max_sources) * melhor.get("score", 1.0))
        artistas = melhor_rec.get("artists", [])
        artista = sanitizar(remover_prefixo(artistas[0]["name"])) if artistas else None
        titulo = sanitizar(melhor_rec.get("title", "")) or None
        releasegroups = melhor_rec.get("releasegroups", [])
        album = None
        if releasegroups:
            rg_sorted = sorted(releasegroups,
                key=lambda rg: rg.get("releases", [{}])[0].get("date", "9999") if rg.get("releases") else "9999")
            album = sanitizar(rg_sorted[0].get("title", "")) or None
        if artista and artista_valido(artista): return artista, titulo, album
    except Exception:
        pass
    return None, None, None

def get_artista_tag(filepath):
    try:
        audio = File(filepath, easy=True)
        if audio:
            artista = audio.get("artist", [""])[0].strip()
            if artista:
                artista = sanitizar(remover_prefixo(extrair_principal(artista)))
                if artista_valido(artista): return artista
    except Exception:
        pass
    return None

def get_artista_nome(filepath):
    nome = limpar_titulo(filepath)
    if " - " in nome:
        artista = sanitizar(remover_prefixo(extrair_principal(nome.split(" - ")[0].strip())))
        if artista_valido(artista): return artista
    return None

def get_artista_musicbrainz(artista_bruto):
    try:
        r = requests.get("https://musicbrainz.org/ws/2/artist/",
            params={"query": artista_bruto, "limit": 1, "fmt": "json"},
            headers={"User-Agent": "AgentSelecta/2.0 (seu@email.com)"}, timeout=8)
        artistas = r.json().get("artists", [])
        if artistas and int(artistas[0].get("score", 0)) >= 85:
            return sanitizar(remover_prefixo(artistas[0]["name"]))
    except Exception:
        pass
    return None

def get_artista_lastfm(artista_bruto):
    try:
        r = requests.get("http://ws.audioscrobbler.com/2.0/",
            params={"method": "artist.getinfo", "artist": artista_bruto,
                    "api_key": LASTFM_KEY, "format": "json"}, timeout=8)
        nome = r.json().get("artist", {}).get("name", "")
        if nome: return sanitizar(remover_prefixo(nome))
    except Exception:
        pass
    return None

def get_artista_nome_invertido(filepath):
    nome = limpar_titulo(filepath)
    if " - " in nome:
        partes = nome.split(" - ")
        if len(partes) >= 2:
            artista = sanitizar(remover_prefixo(extrair_principal(partes[-1].strip())))
            if artista_valido(artista): return artista
    return None

def buscar_artista_por_titulo_musicbrainz(titulo):
    try:
        r = requests.get("https://musicbrainz.org/ws/2/recording/",
            params={"query": f'recording:"{titulo}"', "limit": 1, "fmt": "json"},
            headers={"User-Agent": "AgentSelecta/2.0 (seu@email.com)"}, timeout=8)
        recordings = r.json().get("recordings", [])
        if recordings and int(recordings[0].get("score", 0)) >= 85:
            artistas = recordings[0].get("artist-credit", [])
            if artistas:
                nome = artistas[0].get("artist", {}).get("name", "")
                if nome: return sanitizar(remover_prefixo(nome)), recordings[0]
    except Exception:
        pass
    return None, None

def buscar_artista_por_titulo_lastfm(titulo):
    try:
        r = requests.get("http://ws.audioscrobbler.com/2.0/",
            params={"method": "track.search", "track": titulo,
                    "api_key": LASTFM_KEY, "format": "json", "limit": 1}, timeout=8)
        matches = r.json().get("results", {}).get("trackmatches", {}).get("track", [])
        if matches:
            artista = matches[0].get("artist", "")
            if artista and artista.lower() != "various artists":
                return sanitizar(remover_prefixo(artista))
    except Exception:
        pass
    return None

def artista_majoritario_da_pasta(pasta, todos_arquivos, conn):
    contagem = {}
    for arq in todos_arquivos:
        if arq.parent != pasta: continue
        artista = get_artista_tag(arq)
        if not artista:
            artista = get_artista_nome(arq)
            if artista:
                rowid = buscar_rowid_artista(conn, artista)
                if not rowid: artista = None
        if artista:
            contagem[artista] = contagem.get(artista, 0) + 1
    return max(contagem, key=contagem.get) if contagem else None

LIXO_DOWNLOAD = re.compile(
    r"\b(official|remix|extended|original|mix|radio|edit|version|prestige|"
    r"320kbps|128kbps|256kbps|kbps|mp3|flac|wav|hd|hq|"
    r"descarga|download|free|gratis|feat|ft|prod|records|music|"
    r"video|clip|lyric|lyrics|audio)\b", re.IGNORECASE)
LIXO_URL = re.compile(r"\[?www\.[^\]]*\]?|\[?http[^\]]*\]?", re.IGNORECASE)

def limpar_lixo(nome):
    nome = LIXO_URL.sub(" ", nome)
    nome = LIXO_DOWNLOAD.sub(" ", nome)
    return re.sub(r"\s+", " ", nome).strip()

cache_artistas = {}
cache_albums   = {}

PESOS = {
    "acoustid": 95, "mbid": 90, "tag": 40,
    "nome+sqlite": 70, "nome+api": 60,
    "nome+invertido+sqlite": 65, "nome+invertido+api": 55,
    "titulo+api": 30, "contexto+pasta": 20,
}

def get_artista(filepath, conn, todos_arquivos=None):
    votos = {}
    fontes = {}

    def votar(artista, fonte):
        if not artista or not artista_valido(artista): return
        nome = resolver_alias(artista)
        peso = PESOS.get(fonte, 30)
        votos[nome] = votos.get(nome, 0) + peso
        if nome not in fontes or peso > fontes[nome][1]:
            fontes[nome] = (fonte, peso)

    # 1. Tag ID3
    artista = get_artista_tag(filepath)
    if artista: votar(artista, "tag")

    # 2. Nome do arquivo
    artista_nome = get_artista_nome(filepath)
    if artista_nome:
        rowid = buscar_rowid_artista(conn, artista_nome)
        if rowid:
            votar(artista_nome, "nome+sqlite")
        else:
            chave = artista_nome
            if chave in cache_artistas:
                if cache_artistas[chave]: votar(cache_artistas[chave], "nome+api")
            else:
                confirmado = get_artista_musicbrainz(artista_nome)
                time.sleep(SLEEP_MB)
                if not confirmado:
                    confirmado = get_artista_lastfm(artista_nome)
                    time.sleep(SLEEP_API)
                if confirmado and artista_valido(confirmado):
                    cache_artistas[chave] = confirmado
                    votar(confirmado, "nome+api")
                else:
                    cache_artistas[chave] = None

    # 3. Nome invertido (MUSICA - ARTISTA)
    artista_inv = get_artista_nome_invertido(filepath)
    # Valida: nome invertido nao pode ser similar ao titulo do arquivo
    titulo_stem = limpar_titulo(filepath).lower()
    inv_parece_titulo = artista_inv and (
        similarity2(artista_inv.lower(), titulo_stem) > 0.5
        or artista_inv.lower() in titulo_stem
        or len(artista_inv.strip()) <= 3
    )
    if artista_inv and artista_inv != artista_nome and not inv_parece_titulo:
        rowid = buscar_rowid_artista(conn, artista_inv)
        if rowid:
            votar(artista_inv, "nome+invertido+sqlite")
        else:
            confirmado = get_artista_musicbrainz(artista_inv)
            time.sleep(SLEEP_MB)
            if not confirmado:
                confirmado = get_artista_lastfm(artista_inv)
                time.sleep(SLEEP_API)
            if confirmado and artista_valido(confirmado):
                votar(confirmado, "nome+invertido+api")

    # 4. Titulo nas APIs — so busca se ha artista identificavel no nome
    # Evita buscar por titulo quando o arquivo se chama "Desconhecido - ..."
    tem_artista_no_nome = " - " in limpar_titulo(str(filepath))
    titulo = limpar_lixo(limpar_titulo(filepath)) if tem_artista_no_nome else ""
    if titulo:
        # Extrai apenas o titulo real (apos " - " se houver)
        titulo_real = titulo.split(" - ")[-1].strip() if " - " in titulo else titulo
        chave_titulo = f"titulo|{titulo}"
        if chave_titulo in cache_artistas:
            if cache_artistas[chave_titulo]: votar(cache_artistas[chave_titulo], "titulo+api")
        else:
            artista_via_titulo, _ = buscar_artista_por_titulo_musicbrainz(titulo_real)
            time.sleep(SLEEP_MB)
            if not artista_via_titulo:
                artista_via_titulo = buscar_artista_por_titulo_lastfm(titulo_real)
                time.sleep(SLEEP_API)
            # Valida: artista encontrado nao pode ser similar ao titulo
            if artista_via_titulo and artista_valido(artista_via_titulo):
                titulo_norm = titulo_real.lower().strip()
                artista_norm = artista_via_titulo.lower().strip()
                if artista_norm not in titulo_norm and similarity2(artista_norm, titulo_norm) < 0.6:
                    cache_artistas[chave_titulo] = artista_via_titulo
                    votar(artista_via_titulo, "titulo+api")
                else:
                    cache_artistas[chave_titulo] = None
            else:
                cache_artistas[chave_titulo] = None

    # 5. MBID lookup (peso 90)
    score_acumulado = max(votos.values()) if votos else 0
    if score_acumulado < 80:
        mbid = get_mbid_tag(filepath)
        if mbid:
            artista_mbid_raw = buscar_artista_por_mbid(mbid)
            if artista_mbid_raw:
                artista_mbid_norm = sanitizar(remover_prefixo(extrair_principal(artista_mbid_raw)))
                if artista_mbid_norm and artista_mbid_norm not in NOMES_INVALIDOS:
                    votar(artista_mbid_norm, "mbid")
                    score_acumulado = max(votos.values()) if votos else 0

    # 6. AcoustID (peso 95)
    if score_acumulado < 80:
        artista_ac, titulo_ac, album_ac = get_artista_acoustid(filepath)
        if artista_ac:
            if album_ac:
                cache_albums[f"{resolver_alias(artista_ac)}|acoustid"] = (album_ac, "acoustid")
            # Valida AcoustID contra nome do arquivo e tag existente
            artista_nome_arq = get_artista_nome(filepath)
            artista_tag_arq  = get_artista_tag(filepath)
            referencia = artista_nome_arq or artista_tag_arq
            if referencia and similarity2(artista_ac, referencia) < 0.2:
                # AcoustID diverge da referencia — vota com peso reduzido
                votar(artista_ac, "titulo+api")  # peso 30 em vez de 95
            else:
                votar(artista_ac, "acoustid")    # peso 95 normal
    else:
        artista_ac = None

    # Resultado final
    if votos:
        if len(votos) > 1:
            candidato_top = max(votos, key=votos.get)
            votos = {a: sc for a, sc in votos.items()
                     if similarity2(a, candidato_top) >= 0.25 or a == candidato_top} or votos
        vencedor = max(votos, key=votos.get)
        fonte_principal = fontes.get(vencedor, ("votacao", 0))[0]
        return vencedor, fonte_principal

    # 7. Contexto de pasta
    artista_pasta = artista_majoritario_da_pasta(filepath.parent, todos_arquivos or [], conn)
    if artista_pasta:
        return resolver_alias(artista_pasta), "contexto+pasta"

    return None, None

def get_album_tag(filepath):
    try:
        audio = File(filepath, easy=True)
        if audio:
            album = audio.get("album", [""])[0].strip()
            if album: return sanitizar(album), "tag"
    except Exception:
        pass
    return None, None

def get_album_musicbrainz(artista, titulo):
    try:
        r = requests.get("https://musicbrainz.org/ws/2/recording/",
            params={"query": f'artist:"{artista}" recording:"{titulo}"', "limit": 1, "fmt": "json"},
            headers={"User-Agent": "AgentSelecta/2.0 (seu@email.com)"}, timeout=8)
        recordings = r.json().get("recordings", [])
        if recordings:
            releases = sorted(recordings[0].get("releases", []),
                              key=lambda x: x.get("date", "9999") or "9999")
            if releases: return sanitizar(releases[0].get("title", "")), "musicbrainz"
    except Exception:
        pass
    return None, None

def get_album_lastfm(artista, titulo):
    try:
        r = requests.get("http://ws.audioscrobbler.com/2.0/",
            params={"method": "track.getInfo", "artist": artista,
                    "track": titulo, "api_key": LASTFM_KEY, "format": "json"}, timeout=8)
        album = r.json().get("track", {}).get("album", {}).get("title", "")
        if album: return sanitizar(album), "lastfm"
    except Exception:
        pass
    return None, None

def get_album_deezer(artista, titulo):
    try:
        r = requests.get("https://api.deezer.com/search",
            params={"q": f'artist:"{artista}" track:"{titulo}"', "limit": 1}, timeout=8)
        data = r.json().get("data", [])
        if data:
            album = sanitizar(data[0].get("album", {}).get("title", ""))
            if album: return album, "deezer"
    except Exception:
        pass
    return None, None

def get_album(artista, filepath, conn):
    chave_ac = f"{artista}|acoustid"
    if chave_ac in cache_albums: return cache_albums[chave_ac]
    album, fonte = get_album_tag(filepath)
    if album: return album, fonte
    rowid = buscar_rowid_artista(conn, artista)
    if rowid:
        album = buscar_album_sqlite(conn, rowid)
        if album: return album, "sqlite"
    titulo = limpar_titulo(filepath)
    if " - " in titulo: titulo = titulo.split(" - ", 1)[1].strip()
    chave = f"{artista}|{titulo}"
    if chave in cache_albums: return cache_albums[chave]
    album, fonte = get_album_musicbrainz(artista, titulo)
    time.sleep(SLEEP_API)
    if not album:
        album, fonte = get_album_lastfm(artista, titulo)
        time.sleep(SLEEP_API)
    if not album:
        album, fonte = get_album_deezer(artista, titulo)
        time.sleep(SLEEP_API)
    cache_albums[chave] = (album, fonte)
    return album, fonte

# ============================================================
# REVIEW
# ============================================================
def _listar_arquivos_audio(pasta):
    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}
    return [f for f in Path(pasta).rglob("*") if f.suffix.lower() in extensoes]

def _detectar_problemas_pasta(pasta, duplicatas_set):
    seps = [" & ", " feat", " ft ", " with "]
    tem_multiplos = any(sep in pasta.name.lower() for sep in seps)
    arquivos = _listar_arquivos_audio(pasta)
    tem_duplicata = any(arq.name.lower() in duplicatas_set for arq in arquivos)
    return tem_multiplos or tem_duplicata, tem_multiplos, tem_duplicata

def _revisar_pasta_unica(pasta, conn):
    arquivos = _listar_arquivos_audio(pasta)
    nome_pasta = pasta.name
    pasta_archive = Path(PASTA_ARCHIVE)
    artista_principal = primeiro_artista(nome_pasta)
    inicial = letra_inicial(artista_principal)
    destino_base = pasta_archive / inicial / artista_principal

    print(f"\n  [ REVIEW: {nome_pasta} ]")
    print(f"  {len(arquivos)} arquivo(s):")
    for arq in arquivos[:5]: print(f"    - {arq.name}")
    if len(arquivos) > 5: print(f"    ... e mais {len(arquivos) - 5} arquivo(s)")
    print(f"\n  Primeiro artista : {artista_principal}")
    print(f"  Destino sugerido : ARCHIVE/{inicial}/{artista_principal}/{nome_pasta}/")
    print()
    print("  [s] Mover para dentro de pasta do primeiro artista")
    print("  [r] Revisar arquivo por arquivo")
    print("  [p] Pular")

    while True:
        acao = input("  Acao [s/r/p]: ").strip().lower()
        if acao == "p":
            break
        elif acao == "s":
            movidos = 0
            # Pasta colaborativa vira subpasta dentro do artista principal
            if nome_pasta != artista_principal:
                dest_pasta = destino_base / nome_pasta
            else:
                dest_pasta = destino_base
            for arq in arquivos:
                album_dir = arq.parent.name if arq.parent != pasta else ""
                dest = dest_pasta / album_dir if album_dir and album_dir != nome_pasta else dest_pasta
                try:
                    mover(arq, dest)
                    movidos += 1
                except Exception as e:
                    print(f"  ERRO: {e}")
            deletar_pastas_vazias(pasta)
            try: pasta.rmdir()
            except OSError: pass
            try: pasta.parent.rmdir()
            except OSError: pass
            sub = f"/{nome_pasta}" if nome_pasta != artista_principal else ""
            print(f"  -> {movidos} arquivo(s) movido(s) para ARCHIVE/{inicial}/{artista_principal}{sub}/")
            break
        elif acao == "r":
            for arq in arquivos:
                artista, fonte = get_artista(arq, conn)
                album, fonte_alb = get_album(artista, arq, conn) if artista else (None, None)
                print(f"\n    {arq.name}")
                print(f"    Artista : {artista or 'NAO IDENTIFICADO'} [{fonte or '-'}]")
                print(f"    Album   : {album or '-'}")
                while True:
                    sub = input("    [s] confirma  [e] editar  [p] pular: ").strip().lower()
                    if sub == "p": break
                    elif sub == "e":
                        artista = input_artista_com_sugestoes("    Artista: ")
                        if artista: artista = sanitizar(artista)
                        album_novo = input("    Novo album (Enter manter): ").strip()
                        if album_novo: album = sanitizar(album_novo)
                        fonte = "manual"
                        continue
                    elif sub == "s":
                        if artista:
                            ini = letra_inicial(artista)
                            art_pasta = primeiro_artista(artista, verificar_archive=True)
                            dest = pasta_archive / ini / art_pasta
                            if album: dest = dest / album
                            try:
                                reescrever_tags(arq, artista=artista, album=album)
                                mover(arq, dest)
                                print(f"    -> Tags reescritas + movido para ARCHIVE/{ini}/{art_pasta}/")
                            except Exception as e:
                                print(f"    -> ERRO: {e}")
                        break
            deletar_pastas_vazias(pasta)
            try: pasta.rmdir()
            except OSError: pass
            try: pasta.parent.rmdir()
            except OSError: pass
            break

def menu_review(conn):
    while True:
        print()
        print("  [ REVIEW - qual area deseja revisar? ]\n")
        print("  [1] ARCHIVE COMPLETO AUTOMATICO -> reorganiza sem confirmacoes")
        print("  [2] ARCHIVE POR PASTA MANUAL    -> navega por letra e pasta")
        print("  [3] Z_UNKNOW                    -> usuario autoriza cada arquivo")
        print("  [0] Voltar ao inicio\n")
        escolha = input("  Escolha: ").strip()
        if escolha == "1":   review_archive_completo(conn)
        elif escolha == "2": review_por_pasta(conn)
        elif escolha == "3": review_unknow(conn)
        elif escolha == "0": break
        else: print("  Digite 1, 2, 3 ou 0.")

def review_archive_completo(conn):
    """Reorganiza ARCHIVE completamente de forma automatica — sem confirmacoes."""
    pasta_archive = Path(PASTA_ARCHIVE)
    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}
    seps = [" & ", " feat", " ft ", " with ", " e "]

    pastas_problema = [p for p in sorted(pasta_archive.rglob("*"))
                       if p.is_dir() and any(sep in p.name.lower() for sep in seps)
                       and _listar_arquivos_audio(p)]
    todos = {}
    for f in pasta_archive.rglob("*"):
        if f.suffix.lower() in extensoes:
            todos.setdefault(f.name.lower(), []).append(f)
    duplicatas = {k: v for k, v in todos.items() if len(v) > 1}

    print(f"\n  Pastas com multiplos artistas : {len(pastas_problema)}")
    print(f"  Arquivos duplicados           : {len(duplicatas)}")
    print(f"\n  Executando automaticamente...\n")

    pastas_movidas = 0
    arquivos_movidos = 0
    duplicatas_removidas = 0

    # Move pastas colaborativas automaticamente
    for pasta in pastas_problema:
        arquivos = _listar_arquivos_audio(pasta)
        artista_principal = primeiro_artista(pasta.name)
        inicial = letra_inicial(artista_principal)
        destino_base = pasta_archive / inicial / artista_principal
        dest_pasta = destino_base / pasta.name if pasta.name != artista_principal else destino_base
        movidos = 0
        for arq in arquivos:
            album_dir = arq.parent.name if arq.parent != pasta else ""
            dest = dest_pasta / album_dir if album_dir and album_dir != pasta.name else dest_pasta
            try:
                mover(arq, dest)
                movidos += 1
            except Exception as e:
                print(f"  ERRO: {arq.name} -> {e}")
        deletar_pastas_vazias(pasta)
        try: pasta.rmdir()
        except OSError: pass
        try: pasta.parent.rmdir()
        except OSError: pass
        sub = f"/{pasta.name}" if pasta.name != artista_principal else ""
        print(f"  -> {pasta.name:<44} {movidos} arq -> ARCHIVE/{inicial}/{artista_principal}{sub}/")
        arquivos_movidos += movidos
        pastas_movidas += 1

    # Remove duplicatas — mantem o primeiro encontrado
    if duplicatas:
        print(f"\n  Removendo duplicatas (mantendo primeiro encontrado)...")
        for nome, caminhos in duplicatas.items():
            # Ordena por caminho para consistencia — mantem o primeiro
            caminhos_sorted = sorted(caminhos, key=lambda c: str(c))
            for c in caminhos_sorted[1:]:
                try:
                    c.unlink()
                    duplicatas_removidas += 1
                    print(f"  -> Deletado: {c.relative_to(pasta_archive)}")
                except Exception as e:
                    print(f"  ERRO ao deletar {c.name}: {e}")

    removidas = deletar_pastas_vazias(PASTA_ARCHIVE)
    print(f"\n  {'=' * 50}")
    print(f"  ARCHIVE COMPLETO AUTOMATICO concluido!")
    print(f"  Pastas reorganizadas : {pastas_movidas}")
    print(f"  Arquivos movidos     : {arquivos_movidos}")
    print(f"  Duplicatas removidas : {duplicatas_removidas}")
    print(f"  Pastas vazias        : {removidas}")
    print(f"  {'=' * 50}")

def review_por_pasta(conn):
    pasta_archive = Path(PASTA_ARCHIVE)
    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}

    def get_duplicatas():
        todos = {}
        for f in pasta_archive.rglob("*"):
            if f.suffix.lower() in extensoes:
                todos.setdefault(f.name.lower(), []).append(f)
        return {k for k, v in todos.items() if len(v) > 1}

    while True:
        letras = sorted([p.name for p in pasta_archive.iterdir() if p.is_dir()])
        print(f"\n  [ ARCHIVE - letras disponiveis ]\n")
        print("  " + "  ".join(letras))
        print("\n  [0] Voltar ao menu REVIEW")
        letra = input("\n  Digite uma letra: ").strip().upper()
        if letra == "0": break
        pasta_letra = pasta_archive / letra
        if not pasta_letra.exists():
            print(f"  Letra '{letra}' nao encontrada.")
            continue

        mostrar_todas = False

        while True:
            todas_pastas = sorted([p for p in pasta_letra.iterdir() if p.is_dir()])
            if not todas_pastas:
                print(f"  Nenhuma pasta em {letra}/")
                break

            duplicatas_set = get_duplicatas()
            pastas_com_problema = []
            for p in todas_pastas:
                _, tem_mult, tem_dup = _detectar_problemas_pasta(p, duplicatas_set)
                if tem_mult or tem_dup:
                    pastas_com_problema.append((p, tem_mult, tem_dup))

            pastas_exibir = todas_pastas if mostrar_todas else [p for p, _, _ in pastas_com_problema]

            if not pastas_exibir and not mostrar_todas:
                print(f"\n  Nenhum problema encontrado em {letra}/")
                print("  [t] Ver todas as pastas  [0] Escolher outra letra")
                nav = input("  Opcao: ").strip().lower()
                if nav == "t": mostrar_todas = True; continue
                else: break

            print(f"\n  [ ARCHIVE / {letra} ]", end="")
            if not mostrar_todas:
                print(f"  — {len(pastas_exibir)} pasta(s) com problema(s)")
            else:
                print(f"  — {len(pastas_exibir)} pasta(s) total")

            indice_para_pasta = {}
            for idx, item in enumerate(pastas_exibir, 1):
                p = item
                if not mostrar_todas:
                    p, tem_mult, tem_dup = pastas_com_problema[idx - 1]
                else:
                    _, tem_mult, tem_dup = _detectar_problemas_pasta(p, duplicatas_set)
                qtd = len(_listar_arquivos_audio(p))
                flags = []
                if tem_mult: flags.append("multiplos artistas")
                if tem_dup:  flags.append("duplicatas")
                marcador = f"  <- {', '.join(flags)}" if flags else ""
                print(f"  [{idx:2}] {p.name:<42} {qtd:>3} arq{marcador}")
                indice_para_pasta[idx] = p

            print()
            if not mostrar_todas: print("  [t] Ver todas as pastas")
            else: print("  [f] Mostrar apenas problemas")
            print("  [0] Escolher outra letra")
            print()
            print("  Escolha uma ou mais pastas (ex: 1  ou  1 3 5): ", end="")
            entrada = input().strip().lower()

            if entrada == "0": break
            elif entrada == "t": mostrar_todas = True; continue
            elif entrada == "f": mostrar_todas = False; continue

            try:
                indices = [int(x) for x in entrada.split()]
                pastas_selecionadas = [indice_para_pasta[i] for i in indices if i in indice_para_pasta]
            except ValueError:
                print("  Opcao invalida.")
                continue

            if not pastas_selecionadas:
                print("  Nenhuma pasta valida selecionada.")
                continue

            if len(pastas_selecionadas) == 1:
                _revisar_pasta_unica(pastas_selecionadas[0], conn)
            else:
                # Preview do lote
                print(f"\n  Preview — {len(pastas_selecionadas)} pasta(s) serao movidas:\n")
                plano = []
                for p in pastas_selecionadas:
                    artista_principal = primeiro_artista(p.name)
                    inicial = letra_inicial(artista_principal)
                    destino_base = pasta_archive / inicial / artista_principal
                    qtd = len(_listar_arquivos_audio(p))
                    sub = f"/{p.name}" if p.name != artista_principal else ""
                    print(f"  {p.name:<44} -> ARCHIVE/{inicial}/{artista_principal}{sub}/")
                    plano.append((p, artista_principal, inicial, destino_base, qtd))

                print()
                conf = input("  Confirma? [s] executar  [e] editar destinos  [n] cancelar: ").strip().lower()

                if conf == "n":
                    continue
                elif conf == "e":
                    plano_editado = []
                    for p, artista_principal, inicial, destino_base, qtd in plano:
                        sub = f"/{p.name}" if p.name != artista_principal else ""
                        print(f"\n  {p.name}")
                        print(f"  Destino atual: ARCHIVE/{inicial}/{artista_principal}{sub}/")
                        novo = input("  Novo artista (Enter para manter): ").strip()
                        if novo:
                            artista_principal = sanitizar(novo)
                            inicial = letra_inicial(artista_principal)
                            destino_base = pasta_archive / inicial / artista_principal
                            sub = f"/{p.name}" if p.name != artista_principal else ""
                            print(f"  -> Novo destino: ARCHIVE/{inicial}/{artista_principal}{sub}/")
                        plano_editado.append((p, artista_principal, inicial, destino_base, qtd))
                    plano = plano_editado
                    conf2 = input("\n  Confirma execucao? [s/n]: ").strip().lower()
                    if conf2 != "s": continue

                # Executa lote
                total_movidos = 0
                for p, artista_principal, inicial, destino_base, qtd in plano:
                    arquivos = _listar_arquivos_audio(p)
                    movidos = 0
                    # Pasta colaborativa vira subpasta dentro do artista principal
                    dest_pasta = destino_base / p.name if p.name != artista_principal else destino_base
                    for arq in arquivos:
                        album_dir = arq.parent.name if arq.parent != p else ""
                        dest = dest_pasta / album_dir if album_dir and album_dir != p.name else dest_pasta
                        try:
                            mover(arq, dest)
                            movidos += 1
                        except Exception as e:
                            print(f"  ERRO: {arq.name} -> {e}")
                    deletar_pastas_vazias(p)
                    try: p.rmdir()
                    except OSError: pass
                    try: p.parent.rmdir()
                    except OSError: pass
                    total_movidos += movidos
                    sub = f"/{p.name}" if p.name != artista_principal else ""
                    print(f"  -> {p.name:<44} {movidos} arq -> ARCHIVE/{inicial}/{artista_principal}{sub}/")

                deletar_pastas_vazias(PASTA_ARCHIVE)
                print(f"\n  Lote concluido: {total_movidos} arquivo(s) movido(s), pastas vazias removidas.")

            # Navegacao pos-revisao
            print()
            print("  O que deseja fazer agora?")
            print(f"  [1] Voltar para lista filtrada de {letra}")
            print("  [2] Escolher outra letra")
            print("  [3] Voltar ao menu REVIEW")
            print("  [0] Voltar ao inicio")
            nav = input("  Opcao: ").strip()
            if nav == "1": mostrar_todas = False; continue
            elif nav == "2": break
            elif nav in ("3", "0"): return

def review_unknow(conn):
    pasta_unknow = Path(PASTA_UNKNOW)
    arquivos = _listar_arquivos_audio(pasta_unknow)
    total = len(arquivos)
    if total == 0:
        print("\n  Z_UNKNOW esta vazia!")
        return
    print(f"\n  {total} arquivo(s) em Z_UNKNOW.")
    print("  [s] confirma  [e] editar  [p] pular  [q] sair\n")
    print("=" * 55)
    pasta_archive = Path(PASTA_ARCHIVE)
    identificados = pulados = 0
    barra = tqdm(enumerate(arquivos, 1), total=total, desc="[Z_UNKNOW]", unit="faixa",
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]", colour="yellow")
    for i, arquivo in barra:
        # MBID ja e consultado internamente por get_artista() com peso 90
        artista, fonte = get_artista(arquivo, conn)
        album, fonte_alb = get_album(artista, arquivo, conn) if artista else (None, None)
        barra.write(f"\n  [{i}/{total}] {arquivo.name}")
        barra.write(f"  Artista : {artista or 'NAO IDENTIFICADO'} [{fonte or '-'}]")
        barra.write(f"  Album   : {album or '-'}")
        while True:
            acao = input("  Acao [s/e/p/q]: ").strip().lower()
            if acao == "q":
                deletar_pastas_vazias(PASTA_UNKNOW)
                return
            elif acao == "p":
                pulados += 1; break
            elif acao == "e":
                artista = input_artista_com_sugestoes("  Artista: ")
                if artista: artista = sanitizar(artista)
                album_novo = input("  Novo album (Enter manter): ").strip()
                if album_novo: album = sanitizar(album_novo); fonte_alb = "manual"
                fonte = "manual"; continue
            elif acao == "s":
                if artista:
                    ini = letra_inicial(artista)
                    art_pasta = primeiro_artista(artista, verificar_archive=True)
                    dest = pasta_archive / ini / art_pasta
                    if album: dest = dest / album
                    try:
                        reescrever_tags(arquivo, artista=artista, album=album)
                        mover(arquivo, dest)
                        identificados += 1
                        barra.write(f"  -> Tags reescritas + movido para ARCHIVE/{ini}/{art_pasta}/")
                    except Exception as e:
                        barra.write(f"  -> ERRO: {e}")
                else:
                    barra.write("  -> Sem artista, mantido em Z_UNKNOW")
                break
    deletar_pastas_vazias(PASTA_UNKNOW)
    print(f"\n{'=' * 55}")
    print(f"  Z_UNKNOW REVIEW concluido!")
    print(f"  Identificados : {identificados}  Pulados: {pulados}")
    print(f"{'=' * 55}")

# ============================================================
# SCAN
# ============================================================
def processar_scan(conn, todos_arquivos, artistas_por_pasta):
    hoje = date.today().isoformat()
    pasta_archive = Path(PASTA_ARCHIVE)
    pasta_unknow  = Path(PASTA_UNKNOW)
    pasta_archive.mkdir(parents=True, exist_ok=True)
    pasta_unknow.mkdir(parents=True, exist_ok=True)
    Path(PASTA_LOST).mkdir(parents=True, exist_ok=True)
    Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)
    lost_path   = Path(PASTA_LOST)   / f"SCAN_lost_tracks_{hoje}.txt"
    report_path = Path(PASTA_REPORT) / f"SCAN_the_archive_report_{hoje}.csv"
    total = len(todos_arquivos)
    organizados = unknows = pulados = 0
    lost_lines = []
    report_rows = []
    print(f"\n{total} arquivos para revisar.")
    print("[s] confirma  [e] editar  [p] pular  [q] sair\n")
    print("=" * 55)
    for i, arquivo in enumerate(todos_arquivos, 1):
        # MBID ja e consultado internamente por get_artista() com peso 90
        artista, fonte = get_artista(arquivo, conn, artistas_por_pasta)
        album, fonte_alb = get_album(artista, arquivo, conn) if artista else (None, None)
        print(f"\n[{i}/{total}] {arquivo.name}")
        print(f"  Artista : {artista or 'NAO IDENTIFICADO'} [{fonte or '-'}]")
        print(f"  Album   : {album or '-'} [{fonte_alb or '-'}]")
        if artista:
            ini = letra_inicial(artista)
            art_pasta = primeiro_artista(artista, verificar_archive=True)
            print(f"  Destino : ARCHIVE/{ini}/{art_pasta}/{album or ''}")
        else:
            print(f"  Destino : Z_UNKNOW")
        while True:
            acao = input("  Acao [s/e/p/q]: ").strip().lower()
            if acao == "q":
                print("\nSaindo do SCAN..."); return
            elif acao == "p":
                pulados += 1; break
            elif acao == "e":
                artista = input_artista_com_sugestoes("  Artista: ")
                if artista: artista = sanitizar(artista)
                album_novo = input("  Novo album (Enter manter): ").strip()
                if album_novo: album = sanitizar(album_novo); fonte_alb = "manual"
                fonte = "manual"
                if artista:
                    ini = letra_inicial(artista)
                    art_pasta = primeiro_artista(artista, verificar_archive=True)
                    print(f"  -> Novo destino: ARCHIVE/{ini}/{art_pasta}/{album or ''}")
                continue
            elif acao == "s":
                if not artista:
                    unknows += 1
                    lost_lines.append(f"[{hoje}] SEM ARTISTA | {arquivo.name}")
                    report_rows.append({"data": hoje, "arquivo": arquivo.name,
                        "artista": "-", "album": "-", "fonte_artista": "-",
                        "fonte_album": "-", "destino": str(pasta_unknow), "status": "unknow"})
                    try: mover(arquivo, pasta_unknow)
                    except Exception as e: print(f"  -> ERRO: {e}")
                else:
                    ini = letra_inicial(artista)
                    art_pasta = primeiro_artista(artista, verificar_archive=True)
                    dest = pasta_archive / ini / art_pasta
                    if album: dest = dest / album
                    organizados += 1
                    report_rows.append({"data": hoje, "arquivo": arquivo.name,
                        "artista": artista, "album": album or "-",
                        "fonte_artista": fonte, "fonte_album": fonte_alb or "-",
                        "destino": str(dest), "status": "ok"})
                    try:
                        reescrever_tags(arquivo, artista=artista, album=album)
                        mover(arquivo, dest)
                        print(f"  -> Tags reescritas + movido para ARCHIVE/{ini}/{art_pasta}/")
                    except Exception as e: print(f"  -> ERRO: {e}")
                break
    with open(lost_path, "w", encoding="utf-8") as f:
        f.write("AGENT SELECTA v2.0\nMODO: SCAN\n")
        f.write(f"Data: {hoje}\nTotal: {total}\n" + "="*45 + "\n\n")
        f.write("\n".join(lost_lines) if lost_lines else "Nenhum arquivo perdido.")
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["data","arquivo","artista","album",
            "fonte_artista","fonte_album","destino","status"])
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"\n{'='*55}\nSCAN CONCLUIDO!")
    print(f"  Organizados: {organizados}  Z_UNKNOW: {unknows}  Pulados: {pulados}")
    print(f"{'='*55}")

# ============================================================
# ORGANIZAR PRINCIPAL
# ============================================================

# ============================================================
# AUDIT DE TAGS
# ============================================================

def buscar_artista_no_archive(query, max_resultados=8):
    """Busca artistas existentes no ARCHIVE que correspondam ao query."""
    pasta_archive = Path(PASTA_ARCHIVE)
    if not pasta_archive.exists():
        return []
    query_norm = re.sub(r"[-_\.]", " ", query).lower().strip()
    resultados = []
    for letra_dir in pasta_archive.iterdir():
        if not letra_dir.is_dir(): continue
        for artista_dir in letra_dir.iterdir():
            if not artista_dir.is_dir(): continue
            nome = artista_dir.name
            nome_norm = re.sub(r"[-_\.]", " ", nome).lower().strip()
            # Match exato, startswith ou similarity
            if nome_norm == query_norm:
                resultados.insert(0, nome)  # exato vai primeiro
            elif nome_norm.startswith(query_norm):
                resultados.insert(0, nome)
            elif query_norm in nome_norm:
                resultados.append(nome)
            elif similarity2(query_norm, nome_norm) >= 0.4:
                resultados.append(nome)
    # Deduplica mantendo ordem
    vistos = set()
    dedup = []
    for r in resultados:
        if r not in vistos:
            vistos.add(r)
            dedup.append(r)
    return dedup[:max_resultados]


def input_artista_com_sugestoes(prompt="  Artista: "):
    """Input com autocomplete nas pastas do ARCHIVE.
    Digita nome → mostra sugestoes → usuario confirma ou escolhe."""
    while True:
        query = input(prompt).strip()
        if not query:
            return None

        sugestoes = buscar_artista_no_archive(query)

        if not sugestoes:
            print(f"  Nenhuma pasta encontrada para '{query}'.")
            conf = input("  Usar mesmo assim? [s] sim  [n] digitar novamente: ").strip().lower()
            if conf == "s":
                return sanitizar(query)
            continue

        print(f"\n  Sugestoes do ARCHIVE:")
        for idx, s in enumerate(sugestoes, 1):
            qtd = len(_listar_arquivos_audio(
                Path(PASTA_ARCHIVE) / letra_inicial(s) / s
            ))
            print(f"  [{idx}] {s:<40} ({qtd} arq)")
        print(f"  [0] Usar '{query}' como digitado")
        print(f"  [n] Digitar novamente")

        escolha = input("  Escolha: ").strip().lower()
        if escolha == "n":
            continue
        elif escolha == "0":
            return sanitizar(query)
        else:
            try:
                return sugestoes[int(escolha) - 1]
            except (ValueError, IndexError):
                print("  Opcao invalida.")
                continue

def audit_tags():
    """Percorre ARCHIVE comparando tag artist com pasta.
    score >= 0.6 -> corrige automaticamente (mostra na barra)
    score 0.4-0.59 -> pausa e pede confirmacao do usuario
    score <  0.4 -> direciona para RESCUE"""
    pasta_archive = Path(PASTA_ARCHIVE)
    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}
    hoje = date.today().isoformat()
    Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)
    report_path = Path(PASTA_REPORT) / f"AUDIT_tags_{hoje}.csv"

    arquivos = [f for f in pasta_archive.rglob("*") if f.suffix.lower() in extensoes]
    total = len(arquivos)

    if total == 0:
        print("\n  ARCHIVE vazio!")
        return

    print(f"\n  {total} arquivo(s) no ARCHIVE para auditar.")
    print(f"  score >= 0.6 -> corrige tag automaticamente")
    print(f"  score 0.4-0.59 -> pausa e pede confirmacao do usuario")
    print(f"  score <  0.4 -> direciona para o RESCUE")
    print(f"  score == 0.0 -> tenta reidentificar antes do RESCUE\n")
    print("=" * 55)

    auto_corrigidos = 0
    confirmados = 0
    pulados = 0
    conflitos = 0
    report_rows = []

    conn = conectar_db()

    barra = tqdm(
        enumerate(arquivos, 1), total=total,
        desc="[AUDIT]", unit="faixa",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        colour="magenta"
    )
    barra.set_postfix(auto=0, conf=0)

    for i, arquivo in barra:
        # Le tag artist
        try:
            audio = File(arquivo, easy=True)
            tag_artist = audio.get("artist", [""])[0].strip() if audio else ""
        except Exception:
            tag_artist = ""

        # Identifica pasta do artista subindo ate a pasta de letra
        pasta_artista = arquivo.parent
        while pasta_artista.parent != pasta_archive and pasta_artista.parent != pasta_artista:
            if len(pasta_artista.parent.name) <= 1 or pasta_artista.parent.name == "#":
                break
            pasta_artista = pasta_artista.parent
        nome_pasta = pasta_artista.name

        score = similarity_audit(tag_artist, nome_pasta)

        tag_norm   = re.sub(r"[-_\.]", " ", tag_artist).lower().strip()
        pasta_norm = re.sub(r"[-_\.]", " ", nome_pasta).lower().strip()
        ja_igual   = (tag_norm == pasta_norm)

        if score >= 0.6:
            if not ja_igual:
                # Nao corrige se a tag parece mais completa que a pasta
                # ex: tag='walker & royce' pasta='walker' — pasta e subconjunto da tag
                tag_norm2  = re.sub(r"[-_\.]", " ", tag_artist).lower().strip()
                pasta_norm2 = re.sub(r"[-_\.]", " ", nome_pasta).lower().strip()
                pasta_e_subconjunto = (
                    tag_norm2.startswith(pasta_norm2 + " ") or
                    tag_norm2.startswith(pasta_norm2 + ",") or
                    tag_norm2.startswith(pasta_norm2 + "&")
                )
                if pasta_e_subconjunto:
                    # Tag mais completa — mantem a tag, nao corrige
                    continue
                # Corrige automaticamente
                reescrever_tags(arquivo, artista=nome_pasta)
                auto_corrigidos += 1
                barra.set_postfix(auto=auto_corrigidos, conf=conflitos)
                barra.write(
                    f"  [AUTO] {arquivo.name[:50]:<50} "
                    f"'{tag_artist}' -> '{nome_pasta}' (score:{score:.2f})"
                )
                report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                    "tag_antiga": tag_artist, "tag_nova": nome_pasta,
                    "score": round(score, 2), "acao": "auto_corrigido"})
            # Tag ja correta — segue sem mostrar nada
            continue

        if score == 0.0:
            # Tenta reidentificar automaticamente via sistema completo
            artista_id, fonte_id = get_artista(arquivo, conn, [])
            titulo_id  = ""
            album_id   = ""
            score_id   = 95 if artista_id else 0

            # Valida: artista nao pode ser similar ao titulo do arquivo
            # nem ser uma string muito curta
            titulo_arquivo = limpar_titulo(str(arquivo)).lower()
            # Remove parte apos " - " para pegar so o titulo real
            if " - " in titulo_arquivo:
                titulo_arquivo = titulo_arquivo.split(" - ")[-1].strip()
            artista_parece_titulo = (
                artista_id and (
                    similarity2(artista_id.lower(), titulo_arquivo) > 0.5
                    or len(artista_id.strip()) < 3
                )
            )

            if artista_id and score_id >= 80 and not artista_parece_titulo:
                # Confiante — corrige automaticamente
                reescrever_tags(arquivo, artista=artista_id,
                    album=album_id, titulo=titulo_id)
                ini2      = letra_inicial(artista_id)
                art2      = primeiro_artista(artista_id)
                dest2     = Path(PASTA_ARCHIVE) / ini2 / art2
                mover(arquivo, dest2)
                auto_corrigidos += 1
                barra.set_postfix(auto=auto_corrigidos, conf=conflitos)
                barra.write(
                    f"  [ID] {arquivo.name[:46]:<46} "
                    f"'{artista_id}' ({fonte_id}:{score_id}) -> ARCHIVE/{ini2}/{art2}/"
                )
                report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                    "tag_antiga": tag_artist, "tag_nova": artista_id,
                    "score": round(score_id, 2), "acao": f"reidentificado->ARCHIVE/{ini2}/{art2}"})
                continue

            # Nao conseguiu identificar — pula com registro
            pulados += 1
            barra.write(
                f"  [SKIP] {arquivo.name[:50]:<50} "
                f"tag:'{tag_artist or 'VAZIA'}' pasta:'{nome_pasta}'"
                + (f" melhor:'{artista_id}'({score_id})" if artista_id else "")
            )
            report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                "tag_antiga": tag_artist, "tag_nova": artista_id or "-",
                "score": round(score_id, 2), "acao": "skip_sem_identificacao"})
            continue

        # Score > 0 mas < 0.4 — pula automaticamente para o RESCUE
        # (faixa 0.4-0.59 cai no bloco de conflito interativo abaixo)
        if score < 0.4:
            pulados += 1
            barra.write(
                f"  [RESCUE] {arquivo.name[:48]:<48} "
                f"tag:'{tag_artist or 'VAZIA'}' pasta:'{nome_pasta}' (score:{score:.2f})"
            )
            report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                "tag_antiga": tag_artist, "tag_nova": "-",
                "score": round(score, 2), "acao": "skip_para_rescue"})
            continue

        # Score 0.4 - 0.59 — pausa a barra e pede confirmacao do usuario
        conflitos += 1
        barra.set_postfix(auto=auto_corrigidos, conf=conflitos)
        barra.write(f"\n  {'='*52}")
        barra.write(f"  [CONFLITO {conflitos}] {arquivo.name}")
        barra.write(f"  Tag artist  : '{tag_artist or 'VAZIA'}'")
        barra.write(f"  Pasta       : '{nome_pasta}'")
        barra.write(f"  Similaridade: {score:.2f}")
        barra.write(f"  [s] tag<-pasta  [m] mover pela tag  [e] editar  [p] pular  [q] sair")

        while True:
            acao = input("  Acao: ").strip().lower()
            if acao == "q":
                _salvar_audit_report(report_path, report_rows, hoje, total,
                    auto_corrigidos, confirmados, pulados)
                print(f"\n  Relatorio salvo: {report_path}")
                return
            elif acao == "p":
                pulados += 1
                report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                    "tag_antiga": tag_artist, "tag_nova": "-",
                    "score": round(score, 2), "acao": "pulado"})
                break
            elif acao == "s":
                reescrever_tags(arquivo, artista=nome_pasta)
                confirmados += 1
                report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                    "tag_antiga": tag_artist, "tag_nova": nome_pasta,
                    "score": round(score, 2), "acao": "confirmado"})
                print(f"  -> Tag reescrita: '{nome_pasta}'")
                break
            elif acao == "m":
                artista_correto = sanitizar(tag_artist) if tag_artist else None
                if artista_correto:
                    ini = letra_inicial(artista_correto)
                    art_pasta = primeiro_artista(artista_correto)
                    dest = pasta_archive / ini / art_pasta
                    try:
                        mover(arquivo, dest)
                        confirmados += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                            "tag_antiga": tag_artist, "tag_nova": artista_correto,
                            "score": round(score, 2), "acao": f"movido->ARCHIVE/{ini}/{art_pasta}"})
                        print(f"  -> Movido para ARCHIVE/{ini}/{art_pasta}/")
                    except Exception as e:
                        print(f"  -> ERRO: {e}")
                        continue
                else:
                    print("  Tag vazia — use [e] para editar.")
                    continue
                break
            elif acao == "e":
                novo = input_artista_com_sugestoes("  Artista: ")
                if novo:
                    ini = letra_inicial(novo)
                    art_pasta = primeiro_artista(novo)
                    dest = Path(PASTA_ARCHIVE) / ini / art_pasta
                    reescrever_tags(arquivo, artista=novo)
                    mover(arquivo, dest)
                    confirmados += 1
                    report_rows.append({"arquivo": arquivo.name, "pasta": nome_pasta,
                        "tag_antiga": tag_artist, "tag_nova": novo,
                        "score": round(score, 2), "acao": f"editado->ARCHIVE/{ini}/{art_pasta}"})
                    print(f"  -> Tag reescrita + movido para ARCHIVE/{ini}/{art_pasta}/")
                break
            else:
                print("  Digite s, m, e, p ou q.")

    conn.close()
    _salvar_audit_report(report_path, report_rows, hoje, total,
        auto_corrigidos, confirmados, pulados)
    print(f"\n{'='*55}")
    print(f"  AUDIT concluido!")
    print(f"  Auto-corrigidos  : {auto_corrigidos}")
    print(f"  Confirmados      : {confirmados}")
    print(f"  Conflitos        : {conflitos}")
    print(f"  Pulados          : {pulados}")
    print(f"  Relatorio        : {report_path}")
    print(f"{'='*55}")

def _salvar_audit_report(path, rows, hoje, total, auto, confirmados, pulados):
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(f"# AGENT SELECTA v2.0 - AUDIT DE TAGS\n# Data: {hoje}\n# Total: {total}\n#\n")
        writer = csv.DictWriter(f, fieldnames=["arquivo","pasta","tag_antiga","tag_nova","score","acao"])
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# RESCUE MODE — arquivos com similaridade 0.0
# ============================================================
def rescue_mode():
    """Coleta todos os arquivos com score 0.0 no ARCHIVE e
    tenta reidentificar automaticamente. O que nao resolver
    pede ajuda ao usuario."""
    pasta_archive = Path(PASTA_ARCHIVE)
    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}
    hoje = date.today().isoformat()
    Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)
    report_path = Path(PASTA_REPORT) / f"RESCUE_{hoje}.csv"

    # --- FASE 1: coleta arquivos com score 0.0 ---
    print("\n  Varrendo ARCHIVE em busca de arquivos com score 0.0...")
    todos = [f for f in pasta_archive.rglob("*") if f.suffix.lower() in extensoes]
    candidatos = []
    barra_coleta = tqdm(todos, desc="[RESCUE scan]", unit="faixa", colour="yellow")
    for arquivo in barra_coleta:
        try:
            audio = File(arquivo, easy=True)
            tag_artist = audio.get("artist", [""])[0].strip() if audio else ""
        except Exception:
            tag_artist = ""
        pasta_artista = arquivo.parent
        while pasta_artista.parent != pasta_archive and pasta_artista.parent != pasta_artista:
            if len(pasta_artista.parent.name) <= 1 or pasta_artista.parent.name == "#":
                break
            pasta_artista = pasta_artista.parent
        nome_pasta = pasta_artista.name
        score = similarity_audit(tag_artist, nome_pasta)
        if score < 0.4:
            candidatos.append((arquivo, tag_artist, nome_pasta, round(score, 2)))
    barra_coleta.close()

    total = len(candidatos)
    if total == 0:
        print("\n  Nenhum arquivo com score 0.0 encontrado no ARCHIVE!")
        return

    print(f"\n  {total} arquivo(s) com score < 0.4 encontrados.")
    print(f"  FASE 2: tentando reidentificar automaticamente...\n")

    conn = conectar_db()
    auto_resolvidos = 0
    manuais = 0
    pulados = 0
    report_rows = []

    try:
        for idx, (arquivo, tag_artist, nome_pasta, score_orig) in enumerate(candidatos, 1):
            print(f"\n  {'='*55}")
            print(f"  [RESCUE {idx}/{total}] {arquivo.name}")
            print(f"  Tag artist  : '{tag_artist or 'VAZIA'}'")
            print(f"  Pasta atual : {arquivo.parent.relative_to(pasta_archive)}")
            print(f"  Similaridade: {score_orig}")

            # Tenta reidentificar com o sistema completo
            print(f"  Reidentificando...")
            artista, fonte = get_artista(arquivo, conn, [])
            titulo   = ""
            album    = ""
            score_id = 95 if artista else 0

            if artista and score_id >= 80:
                # Confiante — mostra e pede confirmacao
                ini       = letra_inicial(artista)
                art_pasta = primeiro_artista(artista, verificar_archive=True)
                destino   = pasta_archive / ini / art_pasta
                print(f"  ─────────────────────────────────────────")
                print(f"  Encontrado  : {artista}  ({fonte}, score:{score_id})")
                if titulo: print(f"  Titulo      : {titulo}")
                if album:  print(f"  Album       : {album}")
                print(f"  Destino     : ARCHIVE/{ini}/{art_pasta}/")
                print(f"  ─────────────────────────────────────────")
                print(f"  [s] confirmar e mover  [e] editar artista  [z] Z_UNKNOW  [p] pular  [q] sair")

                while True:
                    acao = input("  Acao: ").strip().lower()
                    if acao == "q":
                        _salvar_rescue_report(report_path, report_rows, hoje)
                        conn.close()
                        return
                    elif acao == "s":
                        reescrever_tags(arquivo, artista=artista, album=album, titulo=titulo)
                        mover(arquivo, destino)
                        auto_resolvidos += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                            "artista_novo": artista, "fonte": fonte, "score": score_id,
                            "acao": f"auto->ARCHIVE/{ini}/{art_pasta}"})
                        print(f"  -> Movido para ARCHIVE/{ini}/{art_pasta}/")
                        break
                    elif acao == "e":
                        novo = input_artista_com_sugestoes("  Artista: ")
                        if novo:
                            novo = sanitizar(novo)
                            ini2 = letra_inicial(novo)
                            art2 = primeiro_artista(novo, verificar_archive=True)
                            reescrever_tags(arquivo, artista=novo)
                            mover(arquivo, pasta_archive / ini2 / art2)
                            manuais += 1
                            report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                                "artista_novo": novo, "fonte": "manual", "score": 0,
                                "acao": f"manual->ARCHIVE/{ini2}/{art2}"})
                            print(f"  -> Movido para ARCHIVE/{ini2}/{art2}/")
                        break
                    elif acao == "z":
                        mover(arquivo, Path(PASTA_UNKNOW))
                        manuais += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                            "artista_novo": "-", "fonte": "-", "score": 0, "acao": "movido->Z_UNKNOW"})
                        print(f"  -> Movido para Z_UNKNOW")
                        break
                    elif acao == "p":
                        pulados += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                            "artista_novo": "-", "fonte": "-", "score": 0, "acao": "pulado"})
                        break
                    else:
                        print("  Digite s, e, z, p ou q.")

            else:
                # Nao identificado — pede ajuda manual
                print(f"  Nao identificado automaticamente.")
                if artista: print(f"  Melhor candidato: '{artista}' ({fonte}, score:{score_id})")
                print(f"  ─────────────────────────────────────────")
                print(f"  [1] Buscar artista no ARCHIVE")
                print(f"  [2] Tentar AcoustID agora (fingerprint)")
                print(f"  [z] Mover para Z_UNKNOW")
                print(f"  [p] Pular  [q] Sair")

                while True:
                    acao = input("  Acao: ").strip().lower()
                    if acao == "q":
                        _salvar_rescue_report(report_path, report_rows, hoje)
                        conn.close()
                        return
                    elif acao == "p":
                        pulados += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                            "artista_novo": "-", "fonte": "-", "score": 0, "acao": "pulado"})
                        break
                    elif acao == "1":
                        novo = input_artista_com_sugestoes("  Artista: ")
                        if novo:
                            novo = sanitizar(novo)
                            ini2 = letra_inicial(novo)
                            art2 = primeiro_artista(novo, verificar_archive=True)
                            reescrever_tags(arquivo, artista=novo)
                            mover(arquivo, pasta_archive / ini2 / art2)
                            manuais += 1
                            report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                                "artista_novo": novo, "fonte": "manual", "score": 0,
                                "acao": f"manual->ARCHIVE/{ini2}/{art2}"})
                            print(f"  -> Movido para ARCHIVE/{ini2}/{art2}/")
                        break
                    elif acao == "2":
                        print("  Calculando fingerprint...")
                        art_ac, titulo_ac, album_ac = get_artista_acoustid(arquivo)
                        if art_ac:
                            ini2   = letra_inicial(art_ac)
                            art2   = primeiro_artista(art_ac)
                            print(f"  AcoustID encontrou: {art_ac}")
                            conf = input(f"  Confirmar e mover para ARCHIVE/{ini2}/{art2}/? [s/n]: ").strip().lower()
                            if conf == "s":
                                reescrever_tags(arquivo, artista=art_ac,
                                    album=album_ac or "", titulo=titulo_ac or "")
                                mover(arquivo, pasta_archive / ini2 / art2)
                                manuais += 1
                                report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                                    "artista_novo": art_ac, "fonte": "acoustid_manual", "score": 0,
                                    "acao": f"acoustid->ARCHIVE/{ini2}/{art2}"})
                                print(f"  -> Movido para ARCHIVE/{ini2}/{art2}/")
                                break
                        else:
                            print("  AcoustID nao encontrou resultado.")
                        continue
                    elif acao == "z":
                        mover(arquivo, Path(PASTA_UNKNOW))
                        manuais += 1
                        report_rows.append({"arquivo": arquivo.name, "pasta_antiga": nome_pasta,
                            "artista_novo": "-", "fonte": "-", "score": 0, "acao": "movido->Z_UNKNOW"})
                        print(f"  -> Movido para Z_UNKNOW")
                        break
                    else:
                        print("  Digite 1, 2, z, p ou q.")

    finally:
        conn.close()

    _salvar_rescue_report(report_path, report_rows, hoje)
    removidas = deletar_pastas_vazias(PASTA_ARCHIVE)
    print(f"\n{'='*55}")
    print(f"  RESCUE concluido!")
    print(f"  Auto-resolvidos  : {auto_resolvidos}")
    print(f"  Resolvidos manual: {manuais}")
    print(f"  Pulados          : {pulados}")
    print(f"  Pastas vazias    : {removidas}")
    print(f"  Relatorio        : {report_path}")
    print(f"{'='*55}")

def _salvar_rescue_report(path, rows, hoje):
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(f"# AGENT SELECTA v2.0 - RESCUE\n# Data: {hoje}\n#\n")
        writer = csv.DictWriter(f, fieldnames=["arquivo","pasta_antiga","artista_novo","fonte","score","acao"])
        writer.writeheader()
        writer.writerows(rows)

def organizar():
    banner()
    modo = escolher_modo()
    hoje = date.today().isoformat()
    prefixo = ""

    Path(PASTA_LOST).mkdir(parents=True, exist_ok=True)
    Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)

    # Redireciona REVIEW
    if modo == "review":
        conn = conectar_db()
        try: menu_review(conn)
        finally: conn.close()
        return

    # Redireciona AUDIT
    if modo == "audit":
        audit_tags()
        input("\nPressione Enter para fechar...")
        return

    # Redireciona RESCUE
    if modo == "rescue":
        rescue_mode()
        input("\nPressione Enter para fechar...")
        return

    lost_path   = Path(PASTA_LOST)   / f"{prefixo}lost_tracks_{hoje}.txt"
    report_path = Path(PASTA_REPORT) / f"{prefixo}the_archive_report_{hoje}.csv"

    print("Conectando ao banco Spotify local...")
    conn = conectar_db()
    print("Banco conectado!\n")

    simulacao = False
    pasta_archive = Path(PASTA_ARCHIVE)
    pasta_unknow  = Path(PASTA_UNKNOW)
    if not simulacao:
        pasta_archive.mkdir(parents=True, exist_ok=True)
        pasta_unknow.mkdir(parents=True, exist_ok=True)

    extensoes = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}
    todos_arquivos = []
    for pasta_str in PASTAS_ORIGEM:
        pasta = Path(pasta_str)
        if not pasta.exists():
            print(f"AVISO: Pasta nao encontrada: {pasta_str}")
            continue
        arquivos = [f for f in pasta.rglob("*") if f.suffix.lower() in extensoes]
        todos_arquivos.extend(arquivos)
        print(f"  {len(arquivos)} musicas em: {Path(pasta_str).name}")

    total = len(todos_arquivos)
    if total == 0:
        print("\nNenhuma musica encontrada.")
        conn.close()
        input("\nPressione Enter para fechar...")
        return

    print(f"\n{total} musicas encontradas!")
    print("=" * 45)

    # Redireciona SCAN
    if modo == "scan":
        try: processar_scan(conn, todos_arquivos, todos_arquivos)
        finally: conn.close()
        return

    erros = unknows = organizados = 0
    lost_lines = []
    report_rows = []
    modo_label = "UPDATE"

    barra = tqdm(enumerate(todos_arquivos, 1), total=total,
                 desc=f"[{modo_label}]", unit="faixa",
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                 colour="cyan" if simulacao else "green")

    for i, arquivo in barra:
        artista, fonte_artista = get_artista(arquivo, conn, todos_arquivos)
        if not artista:
            unknows += 1
            status = "simulado_unknow" if simulacao else "unknow"
            barra.write(f"  Z_UNKNOW -> {arquivo.name}")
            lost_lines.append(f"[{hoje}] SEM ARTISTA | {arquivo.name}")
            report_rows.append({"data": hoje, "arquivo": arquivo.name,
                "artista": "-", "album": "-", "fonte_artista": "-",
                "fonte_album": "-", "destino": str(pasta_unknow), "status": status})
            if not simulacao:
                try: mover(arquivo, pasta_unknow)
                except Exception as e:
                    barra.write(f"  ERRO: {arquivo.name} -> {e}")
                    erros += 1
            continue

        album, fonte_album = get_album(artista, arquivo, conn)
        artista_pasta = primeiro_artista(artista, verificar_archive=True)
        inicial = letra_inicial(artista)
        destino_dir = pasta_archive / inicial / artista_pasta
        if album: destino_dir = destino_dir / album

        organizados += 1
        barra.write(f"  [{fonte_artista}] {inicial}/{artista}/{album or '-'} -> {arquivo.name}")
        report_rows.append({"data": hoje, "arquivo": arquivo.name,
            "artista": artista, "album": album or "-",
            "fonte_artista": fonte_artista, "fonte_album": fonte_album or "-",
            "destino": str(destino_dir), "status": "simulado_ok" if simulacao else "ok"})

        if not simulacao:
            try: mover(arquivo, destino_dir)
            except Exception as e:
                barra.write(f"  ERRO: {arquivo.name} -> {e}")
                lost_lines.append(f"[{hoje}] ERRO | {arquivo.name} | {e}")
                erros += 1

    conn.close()

    with open(lost_path, "w", encoding="utf-8") as f:
        f.write("AGENT SELECTA v2.0 - the cataloger\n")
        f.write(f"MODO: {'SIMULACAO' if simulacao else 'UPDATE'}\n")
        f.write(f"Data: {hoje}\nTotal analisado: {total}\n" + "=" * 45 + "\n\n")
        f.write("\n".join(lost_lines) if lost_lines else "Nenhum arquivo perdido.")

    with open(report_path, "w", newline="", encoding="utf-8") as f:
        f.write(f"# AGENT SELECTA v2.0 - the cataloger\n# Data: {hoje}\n# Total: {total}\n#\n")
        writer = csv.DictWriter(f, fieldnames=["data","arquivo","artista","album",
            "fonte_artista","fonte_album","destino","status"])
        writer.writeheader()
        writer.writerows(report_rows)

    print("\n" + "=" * 45)
    print("CONCLUIDO!")
    print(f"Total analisado : {total}")
    print(f"Organizados     : {organizados}")
    print(f"Z_UNKNOW        : {unknows}")
    print(f"Erros           : {erros}")
    print(f"\nRelatorios salvos em:\n  {lost_path}\n  {report_path}")
    print("=" * 45 + "\n")


if __name__ == "__main__":
    organizar()
