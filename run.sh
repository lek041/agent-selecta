#!/bin/bash
# ─────────────────────────────────────────────
#  Agent Selecta v2.0 — launcher
#  Uso: ./run.sh          → abre a interface Textual
#       ./run.sh engine   → abre o modo terminal original
# ─────────────────────────────────────────────

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$DIR/.venv"

# ── 1. Cria o venv se ainda não existir ──────
if [ ! -d "$VENV" ]; then
    echo "⚙  Primeira execução: criando ambiente virtual..."
    python3 -m venv "$VENV"
    echo "✓  Ambiente criado em .venv/"
fi

# ── 2. Ativa o venv ──────────────────────────
source "$VENV/bin/activate"

# ── 3. Instala dependências se necessário ────
if ! python -c "import textual" 2>/dev/null; then
    echo "⚙  Instalando dependências (apenas na primeira vez)..."
    pip install -q -r "$DIR/requirements.txt"
    echo "✓  Dependências instaladas!"
fi

# ── 4. Executa ───────────────────────────────
cd "$DIR"

if [ "$1" = "engine" ]; then
    echo "▶  Abrindo modo terminal (engine original)..."
    python agent_selecta.py
else
    echo "▶  Abrindo Agent Selecta UI..."
    python agent_selecta_ui.py
fi
