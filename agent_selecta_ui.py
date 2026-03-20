"""
agent_selecta_ui.py — Interface Textual para o Agent Selecta v2.0
Autor: Kel

Execucao:
    ./run.sh               (recomendado)
    python agent_selecta_ui.py
"""
from __future__ import annotations

import re
import csv
import io
import contextlib
import threading
from pathlib import Path
from datetime import date
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Button, Label, Static, ProgressBar, RichLog, Input
from textual.containers import Container, Horizontal, VerticalScroll
from textual.screen import Screen, ModalScreen
from textual import on, work

# ── Engine (logica pura, sem print/input) ────────────────────────────
from agent_selecta import (
    PASTA_ARCHIVE, PASTA_UNKNOW, PASTA_LOST, PASTA_REPORT, PASTAS_ORIGEM,
    conectar_db, get_artista, get_album, reescrever_tags, mover,
    deletar_pastas_vazias, sanitizar, letra_inicial, primeiro_artista,
    _listar_arquivos_audio, buscar_artista_no_archive,
    similarity_audit, _salvar_audit_report, _salvar_rescue_report,
    review_archive_completo, File,
)

EXTENSOES = {".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"}


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════

def listar_arquivos_selecta() -> list[Path]:
    arquivos: list[Path] = []
    for pasta_str in PASTAS_ORIGEM:
        p = Path(pasta_str)
        if p.exists():
            arquivos.extend(f for f in p.rglob("*") if f.suffix.lower() in EXTENSOES)
    return arquivos


class _Decision:
    """Canal thread-safe: worker bloqueia ate o usuario decidir."""
    def __init__(self):
        self._event  = threading.Event()
        self.action: str           = ""
        self.artist: Optional[str] = None
        self.album:  Optional[str] = None

    def reset(self) -> None:
        self._event.clear()
        self.action = ""
        self.artist = None
        self.album  = None

    def wait(self) -> "_Decision":
        self._event.wait()
        self._event.clear()
        return self

    def set(self, action: str, artist: Optional[str] = None, album: Optional[str] = None):
        self.action = action
        self.artist = artist
        self.album  = album
        self._event.set()


# ═════════════════════════════════════════════════════════════════════
# CSS
# ═════════════════════════════════════════════════════════════════════
CSS = """
Screen { background: #0d1117; color: #e6edf3; }

#logo-box {
    height: auto; content-align: center middle;
    padding: 2 4 1 4; color: #58a6ff; text-style: bold;
}
#apis-line {
    height: 1; content-align: center middle;
    color: #8b949e; margin-bottom: 2;
}
#home-grid {
    layout: grid; grid-size: 2; grid-gutter: 1 2;
    padding: 0 6; height: auto; margin-bottom: 1;
}
.mode-btn { height: 5; text-style: bold; }
#btn-rescue-row { padding: 0 6; height: 5; align: center middle; }
#btn-rescue { width: 60%; height: 5; text-style: bold; }
#home-status {
    height: 1; content-align: center middle; color: #8b949e;
    dock: bottom; background: #161b22; padding: 0 2;
}

#screen-title {
    height: 1; background: #161b22; color: #58a6ff;
    text-style: bold; padding: 0 2;
}

.file-panel {
    border: round #30363d; padding: 1 2;
    margin: 1 1 0 1; height: auto; background: #161b22;
}
.panel-row { height: 1; }
.lbl-key { width: 12; color: #8b949e; }
.lbl-val { color: #e6edf3; text-style: bold; }
.lbl-src { color: #58a6ff; margin-left: 1; }

ProgressBar { margin: 1 2; }
.progress-label { height: 1; content-align: center middle; color: #8b949e; }

#log {
    border: round #30363d; margin: 0 1;
    height: 1fr; background: #0d1117;
}

.action-bar { height: 4; align: center middle; margin: 0 1; }
.action-bar Button { margin: 0 1; min-width: 16; }

#btn-confirm  { background: #0d2818; border: tall #3fb950; color: #3fb950; }
#btn-edit     { background: #1e1a00; border: tall #d29922; color: #d29922; }
#btn-skip     { background: #161b22; border: tall #30363d; color: #8b949e; }
#btn-stop     { background: #2d0f0f; border: tall #f85149; color: #f85149; }
#btn-unknow   { background: #1a1a00; border: tall #d29922; color: #d29922; }
#btn-back     { background: #161b22; border: tall #30363d; color: #8b949e; margin: 0 1 1 1; width: auto; }
#btn-auto         { background: #0d2818; border: tall #3fb950; color: #3fb950; height: 5; }
#btn-unknow-review{ background: #1e1a00; border: tall #d29922; color: #d29922; height: 5; }

EditArtistModal { align: center middle; }
#modal-box {
    width: 64; height: auto; max-height: 32;
    background: #161b22; border: thick #58a6ff; padding: 1 2;
}
#modal-title { height: 1; color: #58a6ff; text-style: bold; margin-bottom: 1; }
#modal-input { margin: 0 0 1 0; }
#suggestions { height: auto; max-height: 12; border: round #30363d; margin-bottom: 1; }
.sug-btn {
    height: 2; width: 100%; background: #0d1117;
    border: none; color: #e6edf3; text-align: left;
}
.sug-btn:hover { background: #1f2d3d; color: #58a6ff; }
#modal-actions { height: 3; align: center middle; }
#modal-confirm { background: #0d2818; border: tall #3fb950; color: #3fb950; width: 16; margin: 0 1; }
#modal-cancel  { background: #161b22; border: tall #30363d; color: #8b949e; width: 16; margin: 0 1; }
"""


# ═════════════════════════════════════════════════════════════════════
# Modal — Editar Artista
# ═════════════════════════════════════════════════════════════════════
class EditArtistModal(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", "Cancelar")]

    def __init__(self, artista_atual: str = ""):
        super().__init__()
        self._atual = artista_atual

    def compose(self) -> ComposeResult:
        with Container(id="modal-box"):
            yield Label("✎  Editar Artista", id="modal-title")
            yield Input(value=self._atual, placeholder="Nome do artista...", id="modal-input")
            yield VerticalScroll(id="suggestions")
            with Horizontal(id="modal-actions"):
                yield Button("✓ Confirmar", id="modal-confirm")
                yield Button("✕ Cancelar",  id="modal-cancel")

    def on_mount(self) -> None:
        self.query_one("#modal-input", Input).focus()
        if self._atual:
            self._buscar(self._atual)

    @on(Input.Changed, "#modal-input")
    def on_input_changed(self, event: Input.Changed) -> None:
        self._buscar(event.value)

    def _buscar(self, query: str) -> None:
        sugestoes = buscar_artista_no_archive(query, max_resultados=7)
        container = self.query_one("#suggestions", VerticalScroll)
        container.remove_children()
        for s in sugestoes:
            ini = s[0].upper() if s[0].isalpha() else "#"
            qtd = len(_listar_arquivos_audio(Path(PASTA_ARCHIVE) / ini / s))
            container.mount(Button(f"  {s}   ({qtd} arq)", classes="sug-btn", name=s))

    @on(Button.Pressed, ".sug-btn")
    def on_sug_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(sanitizar(event.button.name or ""))

    @on(Button.Pressed, "#modal-confirm")
    def on_confirm(self) -> None:
        val = self.query_one("#modal-input", Input).value.strip()
        self.dismiss(sanitizar(val) if val else None)

    @on(Button.Pressed, "#modal-cancel")
    def action_cancel(self) -> None:
        self.dismiss(None)


# ═════════════════════════════════════════════════════════════════════
# HomeScreen
# ═════════════════════════════════════════════════════════════════════
class HomeScreen(Screen):
    BINDINGS = [Binding("q", "quit", "Sair")]

    def compose(self) -> ComposeResult:
        yield Static(
            "  ╔══════════════════════════════════════╗\n"
            "  ║      A G E N T   S E L E C T A      ║\n"
            "  ║          v 2 . 0   T U I             ║\n"
            "  ╚══════════════════════════════════════╝",
            id="logo-box",
        )
        yield Static(
            "AcoustID  ·  MusicBrainz  ·  Last.fm  ·  Discogs  ·  Deezer  ·  Spotify DB",
            id="apis-line",
        )
        with Container(id="home-grid"):
            yield Button("▶  UPDATE\nSELECTA → ARCHIVE automático",    id="btn-update", classes="mode-btn")
            yield Button("⌕  SCAN\nConfirmação arquivo por arquivo",   id="btn-scan",   classes="mode-btn")
            yield Button("↺  REVIEW\nReorganizar e revisar ARCHIVE",   id="btn-review", classes="mode-btn")
            yield Button("⚙  AUDIT\nVerificar e corrigir tags",        id="btn-audit",  classes="mode-btn")
        with Horizontal(id="btn-rescue-row"):
            yield Button("✦  RESCUE  —  Reidentificar arquivos problemáticos", id="btn-rescue")
        yield Static("Carregando...", id="home-status")

    def on_mount(self) -> None:
        self._atualizar_status()

    def _atualizar_status(self) -> None:
        try:
            arqs = listar_arquivos_selecta()
            msg  = f"SELECTA: {len(arqs)} arquivo(s) aguardando"
        except Exception:
            msg = "SELECTA: pasta não encontrada"
        self.query_one("#home-status", Static).update(msg)

    @on(Button.Pressed, "#btn-update")
    def go_update(self) -> None: self.app.push_screen(UpdateScreen())

    @on(Button.Pressed, "#btn-scan")
    def go_scan(self) -> None: self.app.push_screen(ScanScreen())

    @on(Button.Pressed, "#btn-review")
    def go_review(self) -> None: self.app.push_screen(ReviewScreen())

    @on(Button.Pressed, "#btn-audit")
    def go_audit(self) -> None: self.app.push_screen(AuditScreen())

    @on(Button.Pressed, "#btn-rescue")
    def go_rescue(self) -> None: self.app.push_screen(RescueScreen())

    def action_quit(self) -> None: self.app.exit()


# ═════════════════════════════════════════════════════════════════════
# UpdateScreen
# ═════════════════════════════════════════════════════════════════════
class UpdateScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]
    _stop_flag = False

    def compose(self) -> ComposeResult:
        yield Static("▶  UPDATE — processando SELECTA automaticamente", id="screen-title")
        with Container(classes="file-panel"):
            with Horizontal(classes="panel-row"):
                yield Label("Arquivo:", classes="lbl-key"); yield Label("—", id="val-arquivo", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Artista:", classes="lbl-key"); yield Label("—", id="val-artista", classes="lbl-val")
                yield Label("", id="val-fonte", classes="lbl-src")
            with Horizontal(classes="panel-row"):
                yield Label("Álbum:",   classes="lbl-key"); yield Label("—", id="val-album",   classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Destino:", classes="lbl-key"); yield Label("—", id="val-destino", classes="lbl-val")
        yield ProgressBar(id="progress", total=100, show_eta=True)
        yield Label("0 / 0", id="progress-label", classes="progress-label")
        yield RichLog(id="log", highlight=True, markup=True)
        with Horizontal(classes="action-bar"):
            yield Button("⏹  Parar",  id="btn-stop")
            yield Button("← Voltar",  id="btn-back")

    # ── helpers chamados pela thread via app.call_from_thread ─────────
    def _init_progress(self, total: int) -> None:
        self.query_one("#progress", ProgressBar).update(total=total)

    def _tick(self, label: str) -> None:
        self.query_one("#progress", ProgressBar).advance(1)
        self.query_one("#progress-label", Label).update(label)

    def _atualizar_painel(self, nome, artista, fonte, album, destino) -> None:
        self.query_one("#val-arquivo",  Label).update(nome)
        self.query_one("#val-artista",  Label).update(artista or "[red]—[/]")
        self.query_one("#val-fonte",    Label).update(f"[{fonte}]" if fonte else "")
        self.query_one("#val-album",    Label).update(album or "—")
        self.query_one("#val-destino",  Label).update(destino)

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    def on_mount(self) -> None:
        self._stop_flag = False
        self._run_update()

    @work(thread=True)
    def _run_update(self) -> None:
        hoje = date.today().isoformat()
        for p in [PASTA_LOST, PASTA_REPORT, PASTA_ARCHIVE, PASTA_UNKNOW]:
            Path(p).mkdir(parents=True, exist_ok=True)

        arquivos = listar_arquivos_selecta()
        total    = len(arquivos)
        if total == 0:
            self.app.call_from_thread(self._log, "[yellow]Nenhum arquivo em SELECTA.[/]")
            return

        self.app.call_from_thread(self._init_progress, total)
        conn = conectar_db()
        org = unk = erros = 0
        lost_lines: list[str] = []
        report_rows: list[dict] = []

        try:
            for i, arq in enumerate(arquivos, 1):
                if self._stop_flag:
                    self.app.call_from_thread(self._log, "[red]Interrompido pelo usuário.[/]")
                    break

                artista, fonte       = get_artista(arq, conn, arquivos)
                album_val, fonte_alb = get_album(artista, arq, conn) if artista else (None, None)
                ini   = letra_inicial(artista) if artista else "?"
                art_p = primeiro_artista(artista, verificar_archive=True) if artista else None
                dest_s = f"ARCHIVE/{ini}/{art_p}/{album_val or ''}" if artista else "Z_UNKNOW"
                dest_d = (Path(PASTA_ARCHIVE) / ini / art_p / (album_val or "")
                          if artista else Path(PASTA_UNKNOW))

                self.app.call_from_thread(self._atualizar_painel,
                    arq.name, artista, fonte, album_val, dest_s)
                self.app.call_from_thread(self._tick,
                    f"{i} / {total}  •  ✓ {org}  ? {unk}  ✕ {erros}")

                if artista:
                    try:
                        reescrever_tags(arq, artista=artista, album=album_val)
                        mover(arq, dest_d)
                        org += 1
                        self.app.call_from_thread(self._log,
                            f"[green]✓[/] [{fonte}] [bold]{ini}/{art_p}[/] — {arq.name}")
                        report_rows.append({"data": hoje, "arquivo": arq.name,
                            "artista": artista, "album": album_val or "-",
                            "fonte_artista": fonte, "fonte_album": fonte_alb or "-",
                            "destino": str(dest_d), "status": "ok"})
                    except Exception as e:
                        erros += 1
                        self.app.call_from_thread(self._log, f"[red]ERRO:[/] {arq.name} → {e}")
                        lost_lines.append(f"[{hoje}] ERRO | {arq.name} | {e}")
                else:
                    unk += 1
                    try: mover(arq, Path(PASTA_UNKNOW))
                    except Exception: erros += 1
                    self.app.call_from_thread(self._log, f"[yellow]?[/] Z_UNKNOW → {arq.name}")
                    lost_lines.append(f"[{hoje}] SEM ARTISTA | {arq.name}")
                    report_rows.append({"data": hoje, "arquivo": arq.name,
                        "artista": "-", "album": "-", "fonte_artista": "-",
                        "fonte_album": "-", "destino": str(PASTA_UNKNOW), "status": "unknow"})
        finally:
            conn.close()

        lost_path   = Path(PASTA_LOST)   / f"UPDATE_lost_{hoje}.txt"
        report_path = Path(PASTA_REPORT) / f"UPDATE_report_{hoje}.csv"
        with open(lost_path, "w", encoding="utf-8") as f:
            f.write(f"AGENT SELECTA v2.0 — UPDATE\nData: {hoje}\n{'='*45}\n\n")
            f.write("\n".join(lost_lines) if lost_lines else "Sem perdas.")
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["data","arquivo","artista","album",
                "fonte_artista","fonte_album","destino","status"])
            w.writeheader(); w.writerows(report_rows)

        self.app.call_from_thread(self._log,
            f"\n[bold green]UPDATE CONCLUÍDO![/]  ✓ {org}  ? {unk}  ✕ {erros}\n"
            f"[dim]Relatório: {report_path}[/]")

    @on(Button.Pressed, "#btn-stop")
    def on_stop(self) -> None: self._stop_flag = True

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None: self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# ScanScreen
# ═════════════════════════════════════════════════════════════════════
class ScanScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]

    def compose(self) -> ComposeResult:
        yield Static("⌕  SCAN — confirmação arquivo por arquivo", id="screen-title")
        with Container(classes="file-panel"):
            with Horizontal(classes="panel-row"):
                yield Label("Arquivo:", classes="lbl-key"); yield Label("—", id="val-arquivo", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Artista:", classes="lbl-key"); yield Label("—", id="val-artista", classes="lbl-val")
                yield Label("", id="val-fonte", classes="lbl-src")
            with Horizontal(classes="panel-row"):
                yield Label("Álbum:",   classes="lbl-key"); yield Label("—", id="val-album",   classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Destino:", classes="lbl-key"); yield Label("—", id="val-destino", classes="lbl-val")
        yield ProgressBar(id="progress", total=100, show_eta=False)
        yield Label("Identificando...", id="progress-label", classes="progress-label")
        yield RichLog(id="log", highlight=True, markup=True)
        with Horizontal(classes="action-bar"):
            yield Button("✓  Confirmar", id="btn-confirm")
            yield Button("✎  Editar",    id="btn-edit")
            yield Button("▷  Pular",     id="btn-skip")
            yield Button("✕  Sair",      id="btn-stop")
        yield Button("← Voltar", id="btn-back")

    def _init_progress(self, total: int) -> None:
        self.query_one("#progress", ProgressBar).update(total=total)

    def _tick(self, label: str) -> None:
        self.query_one("#progress", ProgressBar).advance(1)
        self.query_one("#progress-label", Label).update(label)

    def _atualizar_painel(self, nome, artista, fonte, album, destino) -> None:
        self.query_one("#val-arquivo",  Label).update(nome)
        self.query_one("#val-artista",  Label).update(artista or "[red]NÃO IDENTIFICADO[/]")
        self.query_one("#val-fonte",    Label).update(f"[{fonte}]" if fonte else "")
        self.query_one("#val-album",    Label).update(album or "—")
        self.query_one("#val-destino",  Label).update(destino)

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    def on_mount(self) -> None:
        self._decision = _Decision()
        self._run_scan()

    @work(thread=True)
    def _run_scan(self) -> None:
        hoje = date.today().isoformat()
        for p in [PASTA_LOST, PASTA_REPORT, PASTA_ARCHIVE, PASTA_UNKNOW]:
            Path(p).mkdir(parents=True, exist_ok=True)

        arquivos = listar_arquivos_selecta()
        total    = len(arquivos)
        if total == 0:
            self.app.call_from_thread(self._log, "[yellow]Nenhum arquivo em SELECTA.[/]")
            return

        self.app.call_from_thread(self._init_progress, total)
        conn = conectar_db()
        org = unk = pulados = 0
        lost_lines: list[str] = []
        report_rows: list[dict] = []

        try:
            for i, arq in enumerate(arquivos, 1):
                self._decision.reset()
                artista, fonte       = get_artista(arq, conn, arquivos)
                album_val, fonte_alb = get_album(artista, arq, conn) if artista else (None, None)
                ini   = letra_inicial(artista) if artista else "?"
                art_p = primeiro_artista(artista, verificar_archive=True) if artista else None
                dest_s = f"ARCHIVE/{ini}/{art_p}/{album_val or ''}" if artista else "Z_UNKNOW"

                self.app.call_from_thread(self._atualizar_painel,
                    f"[{i}/{total}]  {arq.name}", artista, fonte, album_val, dest_s)
                self.app.call_from_thread(self._tick, f"{i} / {total}")

                d = self._decision.wait()

                if d.action == "q":
                    self.app.call_from_thread(self._log, "[red]SCAN interrompido.[/]")
                    break
                elif d.action == "p":
                    pulados += 1
                    self.app.call_from_thread(self._log, f"[dim]→ Pulado: {arq.name}[/]")
                    continue
                elif d.action == "e":
                    if d.artist: artista   = d.artist
                    if d.album:  album_val = d.album
                    if artista:
                        ini   = letra_inicial(artista)
                        art_p = primeiro_artista(artista, verificar_archive=True)

                if artista:
                    dest_d = Path(PASTA_ARCHIVE) / ini / art_p
                    if album_val: dest_d = dest_d / album_val
                    try:
                        reescrever_tags(arq, artista=artista, album=album_val)
                        mover(arq, dest_d)
                        org += 1
                        self.app.call_from_thread(self._log,
                            f"[green]✓[/] [{fonte}] ARCHIVE/{ini}/{art_p}/ — {arq.name}")
                        report_rows.append({"data": hoje, "arquivo": arq.name,
                            "artista": artista, "album": album_val or "-",
                            "fonte_artista": fonte or "-", "fonte_album": fonte_alb or "-",
                            "destino": str(dest_d), "status": "ok"})
                    except Exception as e:
                        self.app.call_from_thread(self._log, f"[red]ERRO:[/] {e}")
                else:
                    unk += 1
                    try: mover(arq, Path(PASTA_UNKNOW))
                    except Exception: pass
                    self.app.call_from_thread(self._log, f"[yellow]?[/] Z_UNKNOW → {arq.name}")
                    report_rows.append({"data": hoje, "arquivo": arq.name,
                        "artista": "-", "album": "-", "fonte_artista": "-",
                        "fonte_album": "-", "destino": str(PASTA_UNKNOW), "status": "unknow"})
        finally:
            conn.close()

        lost_path   = Path(PASTA_LOST)   / f"SCAN_lost_{hoje}.txt"
        report_path = Path(PASTA_REPORT) / f"SCAN_report_{hoje}.csv"
        with open(lost_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lost_lines) if lost_lines else "Sem perdas.")
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["data","arquivo","artista","album",
                "fonte_artista","fonte_album","destino","status"])
            w.writeheader(); w.writerows(report_rows)

        self.app.call_from_thread(self._log,
            f"\n[bold green]SCAN CONCLUÍDO![/]  ✓ {org}  ? {unk}  Pulados: {pulados}")

    @on(Button.Pressed, "#btn-confirm")
    def on_confirm(self) -> None: self._decision.set("s")

    @on(Button.Pressed, "#btn-edit")
    async def on_edit(self) -> None:
        atual = str(self.query_one("#val-artista", Label).renderable)
        novo  = await self.app.push_screen_wait(EditArtistModal(atual))
        if novo:
            ini   = letra_inicial(novo)
            art_p = primeiro_artista(novo, verificar_archive=True)
            album = str(self.query_one("#val-album", Label).renderable)
            self.query_one("#val-artista", Label).update(novo)
            self.query_one("#val-fonte",   Label).update("[manual]")
            self.query_one("#val-destino", Label).update(
                f"ARCHIVE/{ini}/{art_p}/{album if album != '—' else ''}")
        self._decision.set("e", artist=novo)

    @on(Button.Pressed, "#btn-skip")
    def on_skip(self) -> None: self._decision.set("p")

    @on(Button.Pressed, "#btn-stop")
    def on_stop(self) -> None: self._decision.set("q")

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None:
        self._decision.set("q")
        self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# AuditScreen
# ═════════════════════════════════════════════════════════════════════
class AuditScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]

    def compose(self) -> ComposeResult:
        yield Static("⚙  AUDIT — verificar e corrigir tags do ARCHIVE", id="screen-title")
        with Container(classes="file-panel"):
            with Horizontal(classes="panel-row"):
                yield Label("Arquivo:",  classes="lbl-key"); yield Label("—", id="val-arquivo", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Tag atual:", classes="lbl-key"); yield Label("—", id="val-tag", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Pasta:",    classes="lbl-key"); yield Label("—", id="val-pasta", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Score:",    classes="lbl-key"); yield Label("—", id="val-score", classes="lbl-val")
        yield ProgressBar(id="progress", total=100, show_eta=True)
        yield Label("", id="progress-label", classes="progress-label")
        yield RichLog(id="log", highlight=True, markup=True)
        with Horizontal(classes="action-bar", id="action-bar"):
            yield Button("✓  Tag←Pasta", id="btn-confirm")
            yield Button("→  Mover/Tag", id="btn-edit")
            yield Button("▷  Pular",     id="btn-skip")
            yield Button("✕  Sair",      id="btn-stop")
        yield Button("← Voltar", id="btn-back")

    def _init_progress(self, total: int) -> None:
        self.query_one("#progress", ProgressBar).update(total=total)

    def _tick(self, label: str) -> None:
        self.query_one("#progress", ProgressBar).advance(1)
        self.query_one("#progress-label", Label).update(label)

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    def _mostrar_conflito(self, nome, tag, pasta, score) -> None:
        self.query_one("#val-arquivo", Label).update(nome)
        self.query_one("#val-tag",     Label).update(tag or "[red]VAZIA[/]")
        self.query_one("#val-pasta",   Label).update(pasta)
        self.query_one("#val-score",   Label).update(
            f"[yellow]{score:.2f}[/]  (0.4–0.59 — confirme)")
        self.query_one("#action-bar").display = True

    def _esconder_conflito(self) -> None:
        self.query_one("#action-bar").display = False

    def on_mount(self) -> None:
        self._decision = _Decision()
        self.query_one("#action-bar").display = False
        self._run_audit()

    @work(thread=True)
    def _run_audit(self) -> None:
        hoje = date.today().isoformat()
        Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)
        report_path   = Path(PASTA_REPORT) / f"AUDIT_tags_{hoje}.csv"
        pasta_archive = Path(PASTA_ARCHIVE)
        arquivos = [f for f in pasta_archive.rglob("*") if f.suffix.lower() in EXTENSOES]
        total    = len(arquivos)

        if total == 0:
            self.app.call_from_thread(self._log, "[yellow]ARCHIVE vazio![/]")
            return

        self.app.call_from_thread(self._init_progress, total)
        conn = conectar_db()
        auto = confirmados = pulados = conflitos = 0
        report_rows: list[dict] = []

        try:
            for i, arq in enumerate(arquivos, 1):
                self._decision.reset()
                try:
                    audio      = File(arq, easy=True)
                    tag_artist = audio.get("artist", [""])[0].strip() if audio else ""
                except Exception:
                    tag_artist = ""

                pasta_a = arq.parent
                while pasta_a.parent != pasta_archive and pasta_a.parent != pasta_a:
                    if len(pasta_a.parent.name) <= 1 or pasta_a.parent.name == "#":
                        break
                    pasta_a = pasta_a.parent
                nome_pasta = pasta_a.name

                score    = similarity_audit(tag_artist, nome_pasta)
                tag_norm = re.sub(r"[-_\.]", " ", tag_artist).lower().strip()
                pst_norm = re.sub(r"[-_\.]", " ", nome_pasta).lower().strip()
                ja_igual = tag_norm == pst_norm

                self.app.call_from_thread(self._tick,
                    f"{i} / {total}  •  auto:{auto}  conflitos:{conflitos}")

                if score >= 0.6:
                    if not ja_igual:
                        pasta_e_sub = (
                            tag_norm.startswith(pst_norm + " ") or
                            tag_norm.startswith(pst_norm + ",") or
                            tag_norm.startswith(pst_norm + "&")
                        )
                        if not pasta_e_sub:
                            reescrever_tags(arq, artista=nome_pasta)
                            auto += 1
                            self.app.call_from_thread(self._log,
                                f"[green][AUTO][/] '{tag_artist}' → '{nome_pasta}'  "
                                f"[dim](score:{score:.2f})[/]")
                            report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                                "tag_antiga": tag_artist, "tag_nova": nome_pasta,
                                "score": round(score, 2), "acao": "auto_corrigido"})
                    continue

                if score == 0.0:
                    artista_id, _ = get_artista(arq, conn, [])
                    if artista_id:
                        reescrever_tags(arq, artista=artista_id)
                        auto += 1
                        self.app.call_from_thread(self._log,
                            f"[cyan][ID][/] '{artista_id}'  [dim]{arq.name[:50]}[/]")
                        report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                            "tag_antiga": tag_artist, "tag_nova": artista_id,
                            "score": 95, "acao": "reidentificado"})
                    else:
                        pulados += 1
                        self.app.call_from_thread(self._log,
                            f"[dim][SKIP][/] '{tag_artist or 'VAZIA'}' × '{nome_pasta}'[/]")
                        report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                            "tag_antiga": tag_artist, "tag_nova": "-",
                            "score": 0, "acao": "skip_sem_id"})
                    continue

                if score < 0.4:
                    pulados += 1
                    self.app.call_from_thread(self._log,
                        f"[yellow][RESCUE][/] score:{score:.2f}  "
                        f"'{tag_artist}' × '{nome_pasta}'")
                    report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                        "tag_antiga": tag_artist, "tag_nova": "-",
                        "score": round(score, 2), "acao": "skip_para_rescue"})
                    continue

                # 0.4 ≤ score < 0.6 → pausa para confirmação
                conflitos += 1
                self.app.call_from_thread(self._mostrar_conflito,
                    arq.name, tag_artist, nome_pasta, score)
                d = self._decision.wait()
                self.app.call_from_thread(self._esconder_conflito)

                if d.action == "q":
                    self.app.call_from_thread(self._log, "[red]AUDIT interrompido.[/]")
                    break
                elif d.action == "p":
                    pulados += 1
                    report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                        "tag_antiga": tag_artist, "tag_nova": "-",
                        "score": round(score, 2), "acao": "pulado"})
                elif d.action == "s":
                    reescrever_tags(arq, artista=nome_pasta)
                    confirmados += 1
                    self.app.call_from_thread(self._log,
                        f"[green]✓[/] tag reescrita: '{nome_pasta}'")
                    report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                        "tag_antiga": tag_artist, "tag_nova": nome_pasta,
                        "score": round(score, 2), "acao": "confirmado"})
                elif d.action == "m" and tag_artist:
                    ini2 = letra_inicial(sanitizar(tag_artist))
                    art2 = primeiro_artista(sanitizar(tag_artist))
                    mover(arq, pasta_archive / ini2 / art2)
                    confirmados += 1
                    self.app.call_from_thread(self._log,
                        f"[cyan]→[/] Movido: ARCHIVE/{ini2}/{art2}/")
                    report_rows.append({"arquivo": arq.name, "pasta": nome_pasta,
                        "tag_antiga": tag_artist, "tag_nova": sanitizar(tag_artist),
                        "score": round(score, 2), "acao": f"movido->ARCHIVE/{ini2}/{art2}"})
        finally:
            conn.close()

        _salvar_audit_report(report_path, report_rows, hoje, total, auto, confirmados, pulados)
        self.app.call_from_thread(self._log,
            f"\n[bold green]AUDIT CONCLUÍDO![/]  auto:{auto}  confirmados:{confirmados}  "
            f"conflitos:{conflitos}  pulados:{pulados}")

    @on(Button.Pressed, "#btn-confirm")
    def on_confirm(self) -> None: self._decision.set("s")

    @on(Button.Pressed, "#btn-edit")
    def on_edit(self) -> None: self._decision.set("m")

    @on(Button.Pressed, "#btn-skip")
    def on_skip(self) -> None: self._decision.set("p")

    @on(Button.Pressed, "#btn-stop")
    def on_stop(self) -> None: self._decision.set("q")

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None:
        self._decision.set("q")
        self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# RescueScreen
# ═════════════════════════════════════════════════════════════════════
class RescueScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]

    def compose(self) -> ComposeResult:
        yield Static("✦  RESCUE — reidentificar arquivos com score < 0.4", id="screen-title")
        with Container(classes="file-panel"):
            with Horizontal(classes="panel-row"):
                yield Label("Arquivo:",    classes="lbl-key"); yield Label("—", id="val-arquivo",   classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Tag atual:",  classes="lbl-key"); yield Label("—", id="val-tag",       classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Pasta atual:",classes="lbl-key"); yield Label("—", id="val-pasta",     classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Candidato:",  classes="lbl-key"); yield Label("—", id="val-candidato", classes="lbl-val")
                yield Label("", id="val-fonte", classes="lbl-src")
        yield ProgressBar(id="progress", total=100, show_eta=False)
        yield Label("", id="progress-label", classes="progress-label")
        yield RichLog(id="log", highlight=True, markup=True)
        with Horizontal(classes="action-bar"):
            yield Button("✓  Confirmar", id="btn-confirm")
            yield Button("✎  Editar",    id="btn-edit")
            yield Button("Z  Z_UNKNOW",  id="btn-unknow")
            yield Button("▷  Pular",     id="btn-skip")
            yield Button("✕  Sair",      id="btn-stop")
        yield Button("← Voltar", id="btn-back")

    def _init_progress(self, total: int) -> None:
        self.query_one("#progress", ProgressBar).update(total=total)

    def _tick(self, label: str) -> None:
        self.query_one("#progress", ProgressBar).advance(1)
        self.query_one("#progress-label", Label).update(label)

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    def _atualizar_painel(self, nome, tag, pasta, candidato, fonte) -> None:
        self.query_one("#val-arquivo",   Label).update(nome)
        self.query_one("#val-tag",       Label).update(tag or "[red]VAZIA[/]")
        self.query_one("#val-pasta",     Label).update(pasta)
        self.query_one("#val-candidato", Label).update(candidato or "[yellow]não identificado[/]")
        self.query_one("#val-fonte",     Label).update(f"[{fonte}]" if fonte else "")

    def on_mount(self) -> None:
        self._decision = _Decision()
        self._run_rescue()

    @work(thread=True)
    def _run_rescue(self) -> None:
        hoje = date.today().isoformat()
        Path(PASTA_REPORT).mkdir(parents=True, exist_ok=True)
        report_path   = Path(PASTA_REPORT) / f"RESCUE_{hoje}.csv"
        pasta_archive = Path(PASTA_ARCHIVE)

        self.app.call_from_thread(self._log, "  Varrendo ARCHIVE em busca de score < 0.4...")
        todos      = [f for f in pasta_archive.rglob("*") if f.suffix.lower() in EXTENSOES]
        candidatos = []
        for arq in todos:
            try:
                audio = File(arq, easy=True)
                tag_a = audio.get("artist", [""])[0].strip() if audio else ""
            except Exception:
                tag_a = ""
            pa = arq.parent
            while pa.parent != pasta_archive and pa.parent != pa:
                if len(pa.parent.name) <= 1 or pa.parent.name == "#":
                    break
                pa = pa.parent
            if similarity_audit(tag_a, pa.name) < 0.4:
                candidatos.append((arq, tag_a, pa.name))

        total = len(candidatos)
        if total == 0:
            self.app.call_from_thread(self._log, "[green]Nenhum arquivo com score < 0.4![/]")
            return

        self.app.call_from_thread(self._log, f"  {total} arquivo(s) encontrados.")
        self.app.call_from_thread(self._init_progress, total)
        conn = conectar_db()
        auto = manuais = pulados = 0
        report_rows: list[dict] = []

        try:
            for idx, (arq, tag_a, nome_pasta) in enumerate(candidatos, 1):
                self._decision.reset()
                self.app.call_from_thread(self._tick, f"{idx} / {total}")
                artista, fonte = get_artista(arq, conn, [])
                self.app.call_from_thread(self._atualizar_painel,
                    arq.name, tag_a, nome_pasta, artista, fonte)

                d = self._decision.wait()

                if d.action == "q":
                    self.app.call_from_thread(self._log, "[red]RESCUE interrompido.[/]")
                    break
                elif d.action == "p":
                    pulados += 1
                    report_rows.append({"arquivo": arq.name, "pasta_antiga": nome_pasta,
                        "artista_novo": "-", "fonte": "-", "score": 0, "acao": "pulado"})
                    continue
                elif d.action == "z":
                    mover(arq, Path(PASTA_UNKNOW))
                    manuais += 1
                    self.app.call_from_thread(self._log, f"[yellow]Z[/] → Z_UNKNOW: {arq.name}")
                    report_rows.append({"arquivo": arq.name, "pasta_antiga": nome_pasta,
                        "artista_novo": "-", "fonte": "-", "score": 0, "acao": "->Z_UNKNOW"})
                    continue
                elif d.action in ("s", "e"):
                    art_final = d.artist if d.action == "e" else artista
                    if art_final:
                        ini2 = letra_inicial(art_final)
                        art2 = primeiro_artista(art_final, verificar_archive=True)
                        reescrever_tags(arq, artista=art_final)
                        mover(arq, pasta_archive / ini2 / art2)
                        auto += 1
                        self.app.call_from_thread(self._log,
                            f"[green]✓[/] → ARCHIVE/{ini2}/{art2}/  {arq.name}")
                        report_rows.append({"arquivo": arq.name, "pasta_antiga": nome_pasta,
                            "artista_novo": art_final, "fonte": fonte or "manual",
                            "score": 95, "acao": f"->ARCHIVE/{ini2}/{art2}"})
        finally:
            conn.close()

        _salvar_rescue_report(report_path, report_rows, hoje)
        deletar_pastas_vazias(PASTA_ARCHIVE)
        self.app.call_from_thread(self._log,
            f"\n[bold green]RESCUE CONCLUÍDO![/]  ✓ {auto}  manual:{manuais}  pulados:{pulados}")

    @on(Button.Pressed, "#btn-confirm")
    def on_confirm(self) -> None: self._decision.set("s")

    @on(Button.Pressed, "#btn-edit")
    async def on_edit(self) -> None:
        atual = str(self.query_one("#val-candidato", Label).renderable)
        novo  = await self.app.push_screen_wait(EditArtistModal(atual))
        self._decision.set("e", artist=novo)

    @on(Button.Pressed, "#btn-unknow")
    def on_unknow(self) -> None: self._decision.set("z")

    @on(Button.Pressed, "#btn-skip")
    def on_skip(self) -> None: self._decision.set("p")

    @on(Button.Pressed, "#btn-stop")
    def on_stop(self) -> None: self._decision.set("q")

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None:
        self._decision.set("q")
        self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# ReviewScreen
# ═════════════════════════════════════════════════════════════════════
class ReviewScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]

    def compose(self) -> ComposeResult:
        yield Static("↺  REVIEW — reorganizar ARCHIVE", id="screen-title")
        with Container(id="home-grid"):
            yield Button(
                "⚡ ARCHIVE COMPLETO AUTO\n"
                "Reorganiza pastas colaborativas e remove duplicatas",
                id="btn-auto", classes="mode-btn",
            )
            yield Button(
                "?  Z_UNKNOW\n"
                "Revisa e identifica arquivos desconhecidos",
                id="btn-unknow-review", classes="mode-btn",
            )
        yield RichLog(id="log", highlight=True, markup=True)
        yield Button("← Voltar", id="btn-back")

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    @on(Button.Pressed, "#btn-auto")
    def on_auto(self) -> None: self._run_auto()

    @work(thread=True)
    def _run_auto(self) -> None:
        buf = io.StringIO()
        try:
            conn = conectar_db()
            with contextlib.redirect_stdout(buf):
                review_archive_completo(conn)
            conn.close()
        except Exception as e:
            self.app.call_from_thread(self._log, f"[red]ERRO: {e}[/]")
            return
        for linha in buf.getvalue().splitlines():
            if linha.strip():
                self.app.call_from_thread(self._log, linha)
        self.app.call_from_thread(self._log, "\n[bold green]REVIEW AUTO CONCLUÍDO![/]")

    @on(Button.Pressed, "#btn-unknow-review")
    def on_unknow(self) -> None: self.app.push_screen(UnknowScreen())

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None: self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# UnknowScreen
# ═════════════════════════════════════════════════════════════════════
class UnknowScreen(Screen):
    BINDINGS = [Binding("escape", "back", "Voltar")]

    def compose(self) -> ComposeResult:
        yield Static("?  Z_UNKNOW — confirmar cada arquivo", id="screen-title")
        with Container(classes="file-panel"):
            with Horizontal(classes="panel-row"):
                yield Label("Arquivo:", classes="lbl-key"); yield Label("—", id="val-arquivo", classes="lbl-val")
            with Horizontal(classes="panel-row"):
                yield Label("Artista:", classes="lbl-key"); yield Label("—", id="val-artista", classes="lbl-val")
                yield Label("", id="val-fonte", classes="lbl-src")
            with Horizontal(classes="panel-row"):
                yield Label("Álbum:",   classes="lbl-key"); yield Label("—", id="val-album",   classes="lbl-val")
        yield ProgressBar(id="progress", total=100, show_eta=False)
        yield Label("", id="progress-label", classes="progress-label")
        yield RichLog(id="log", highlight=True, markup=True)
        with Horizontal(classes="action-bar"):
            yield Button("✓  Confirmar", id="btn-confirm")
            yield Button("✎  Editar",    id="btn-edit")
            yield Button("▷  Pular",     id="btn-skip")
            yield Button("✕  Sair",      id="btn-stop")
        yield Button("← Voltar", id="btn-back")

    def _init_progress(self, total: int) -> None:
        self.query_one("#progress", ProgressBar).update(total=total)

    def _tick(self, label: str) -> None:
        self.query_one("#progress", ProgressBar).advance(1)
        self.query_one("#progress-label", Label).update(label)

    def _log(self, texto: str) -> None:
        self.query_one("#log", RichLog).write(texto)

    def _atualizar_painel(self, nome, artista, fonte, album) -> None:
        self.query_one("#val-arquivo", Label).update(nome)
        self.query_one("#val-artista", Label).update(artista or "[red]NÃO IDENTIFICADO[/]")
        self.query_one("#val-fonte",   Label).update(f"[{fonte}]" if fonte else "")
        self.query_one("#val-album",   Label).update(album or "—")

    def on_mount(self) -> None:
        self._decision = _Decision()
        self._run_unknow()

    @work(thread=True)
    def _run_unknow(self) -> None:
        pasta_unknow  = Path(PASTA_UNKNOW)
        pasta_archive = Path(PASTA_ARCHIVE)
        arquivos = _listar_arquivos_audio(pasta_unknow)
        total    = len(arquivos)

        if total == 0:
            self.app.call_from_thread(self._log, "[green]Z_UNKNOW está vazia![/]")
            return

        self.app.call_from_thread(self._init_progress, total)
        conn = conectar_db()
        identificados = pulados = 0

        try:
            for i, arq in enumerate(arquivos, 1):
                self._decision.reset()
                artista, fonte     = get_artista(arq, conn)
                album_val, _       = get_album(artista, arq, conn) if artista else (None, None)
                self.app.call_from_thread(self._tick, f"{i} / {total}")
                self.app.call_from_thread(self._atualizar_painel,
                    f"[{i}/{total}]  {arq.name}", artista, fonte, album_val)

                d = self._decision.wait()

                if d.action == "q": break
                elif d.action == "p":
                    pulados += 1; continue
                elif d.action == "e":
                    if d.artist: artista   = d.artist
                    if d.album:  album_val = d.album
                    fonte = "manual"

                if artista:
                    ini   = letra_inicial(artista)
                    art_p = primeiro_artista(artista, verificar_archive=True)
                    dest  = pasta_archive / ini / art_p
                    if album_val: dest = dest / album_val
                    try:
                        reescrever_tags(arq, artista=artista, album=album_val)
                        mover(arq, dest)
                        identificados += 1
                        self.app.call_from_thread(self._log,
                            f"[green]✓[/] → ARCHIVE/{ini}/{art_p}/  {arq.name}")
                    except Exception as e:
                        self.app.call_from_thread(self._log, f"[red]ERRO:[/] {e}")
        finally:
            conn.close()

        deletar_pastas_vazias(PASTA_UNKNOW)
        self.app.call_from_thread(self._log,
            f"\n[bold green]Z_UNKNOW CONCLUÍDO![/]  "
            f"Identificados:{identificados}  Pulados:{pulados}")

    @on(Button.Pressed, "#btn-confirm")
    def on_confirm(self) -> None: self._decision.set("s")

    @on(Button.Pressed, "#btn-edit")
    async def on_edit(self) -> None:
        atual = str(self.query_one("#val-artista", Label).renderable)
        novo  = await self.app.push_screen_wait(EditArtistModal(atual))
        self._decision.set("e", artist=novo)

    @on(Button.Pressed, "#btn-skip")
    def on_skip(self) -> None: self._decision.set("p")

    @on(Button.Pressed, "#btn-stop")
    def on_stop(self) -> None: self._decision.set("q")

    @on(Button.Pressed, "#btn-back")
    def action_back(self) -> None:
        self._decision.set("q")
        self.app.pop_screen()


# ═════════════════════════════════════════════════════════════════════
# App
# ═════════════════════════════════════════════════════════════════════
class AgentSelectaApp(App):
    TITLE    = "Agent Selecta v2.0"
    CSS      = CSS
    BINDINGS = [Binding("ctrl+q", "quit", "Sair")]

    def on_mount(self) -> None:
        self.push_screen(HomeScreen())


if __name__ == "__main__":
    AgentSelectaApp().run()
