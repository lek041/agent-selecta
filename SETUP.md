# Agent Selecta v2.0 — Setup Guide

Catalogador automático de arquivos de áudio com interface TUI (Textual).
Identifica artista e álbum via múltiplas APIs e organiza a biblioteca em pastas estruturadas.

---

## Requisitos

- **Python 3.10+** — [python.org](https://www.python.org/downloads/)
- **macOS 12+** ou Linux (Ubuntu 20.04+)
- Conexão à internet (para APIs externas)
- `fpcalc` (opcional, para fingerprint acústico):
  ```bash
  brew install chromaprint       # macOS
  sudo apt install libchromaprint-tools  # Ubuntu
  ```

---

## Instalação

```bash
# 1. Clone ou copie a pasta do projeto
cd ~/Desktop/AGENT\ SELECTA

# 2. Dê permissão de execução ao launcher
chmod +x run.sh

# 3. Execute — o venv e as dependências são criados automaticamente
./run.sh
```

Na primeira execução, `run.sh` cria um ambiente virtual `.venv/` isolado e instala todos os pacotes listados em `requirements.txt`. Nas execuções seguintes, ele ativa o ambiente e abre a interface diretamente.

---

## Chaves de API

Abra `agent_selecta.py` e preencha as constantes no topo do arquivo:

```python
ACOUSTID_KEY   = "SUA_CHAVE_AQUI"   # https://acoustid.org/login
LASTFM_KEY     = "SUA_CHAVE_AQUI"   # https://www.last.fm/api/account/create
DISCOGS_TOKEN  = "SUA_CHAVE_AQUI"   # https://www.discogs.com/settings/developers
```

> MusicBrainz e Deezer não exigem chave de API.
> As APIs são opcionais — o sistema usa fallback automático se alguma não responder.

---

## Configurar pastas

Ainda em `agent_selecta.py`, ajuste as pastas de acordo com seu ambiente:

```python
# Pasta(s) de origem — onde ficam os arquivos novos a catalogar
PASTAS_ORIGEM = [
    "/Users/SEU_USUARIO/Music/SELECTA",
]

# Pasta de destino — onde a biblioteca será organizada
PASTA_ARCHIVE = "/Users/SEU_USUARIO/Music/ARCHIVE"

# Pastas auxiliares (criadas automaticamente se não existirem)
PASTA_UNKNOW  = "/Users/SEU_USUARIO/Music/Z_UNKNOW"
PASTA_LOST    = "/Users/SEU_USUARIO/Music/Z_LOST"
PASTA_REPORT  = "/Users/SEU_USUARIO/Music/Z_REPORTS"
```

---

## Banco de dados do Spotify (opcional, mas recomendado)

O banco local do Spotify é a fonte de identificação mais rápida e precisa.
Para gerá-lo, você precisa ter o Spotify instalado no mesmo computador:

```bash
source .venv/bin/activate
python converter_spotify.py
```

Isso cria o arquivo `spotify_agent.db` na pasta do projeto.
Execute novamente sempre que quiser atualizar a base com novas adições à sua biblioteca.

---

## Estrutura de arquivos

```
AGENT SELECTA/
├── agent_selecta.py       # Engine de catalogação
├── agent_selecta_ui.py    # Interface TUI (Textual)
├── converter_spotify.py   # Conversor Spotify → SQLite
├── requirements.txt       # Dependências Python
├── run.sh                 # Launcher (cria venv + abre UI)
├── SETUP.md               # Este arquivo
├── MANUAL - Agent Selecta v2.0.docx
├── spotify_agent.db       # Gerado pelo converter_spotify.py
└── .venv/                 # Gerado pelo run.sh
```

---

## Modos de operação

| Modo | Descrição |
|------|-----------|
| **UPDATE** | Processa SELECTA automaticamente, sem pausa |
| **SCAN** | Confirma cada arquivo manualmente antes de mover |
| **REVIEW** | Reorganiza o ARCHIVE e revisa arquivos não identificados |
| **AUDIT** | Verifica e corrige tags ID3/Vorbis de todo o ARCHIVE |
| **RESCUE** | Reidentifica arquivos com baixo score de similaridade |

---

## Modo terminal (sem TUI)

```bash
./run.sh engine
```

Executa o engine original no terminal, no modo interativo via `input()`/`print()`.

---

## Dependências Python

Listadas em `requirements.txt`:

```
textual>=0.50.0
mutagen
requests
pyacoustid
tqdm
pandas
pyarrow
```

Para instalar manualmente em um venv existente:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Resolução de problemas

**`ModuleNotFoundError: No module named 'textual'`**
Execute sempre via `./run.sh`. Nunca diretamente com `python3 agent_selecta_ui.py` fora do venv.

**`error: externally-managed-environment`**
Use o venv criado pelo `run.sh`. Não instale dependências no Python do sistema.

**UI trava após clicar em um botão**
O worker está aguardando sua decisão. Clique em **Confirmar**, **Pular** ou **Editar** para continuar.

**`spotify_agent.db` não encontrado**
Execute `python converter_spotify.py` com o Spotify instalado. Sem esse arquivo, o sistema usa apenas as APIs externas.

---

## Atalhos de teclado

| Tecla | Ação |
|-------|------|
| `Ctrl+Q` | Sair da aplicação |
| `Q` (tela inicial) | Sair da aplicação |
| `Esc` | Voltar / fechar modal |

---

*Agent Selecta v2.0 — Desenvolvido por Kel — 2026*
