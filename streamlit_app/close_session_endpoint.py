"""
HTTP Endpoint para receber notificacoes de fechamento de sessao.

Este endpoint e chamado via navigator.sendBeacon() quando o usuario fecha a aba,
garantindo que o evento seja processado mesmo apos o navegador fechar a pagina.
"""

import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


def handle_session_close(session_id: str, reason: str = "page_close") -> Dict[str, Any]:
    """
    Processa notificacao de fechamento de sessao.

    Args:
        session_id: ID da sessao a ser fechada
        reason: Razao do fechamento (page_close, tab_close, etc.)

    Returns:
        Dicionario com status da operacao
    """
    from src.shared_lib.utils.session_logger import SessionLogger
    from streamlit_app.session_activity_tracker import mark_session_closed
    from pathlib import Path
    import json

    try:
        logger.info(f"Recebida notificacao de fechamento: session_id={session_id}, reason={reason}")

        # Encontrar arquivo de sessao
        today = datetime.now().strftime("%Y-%m-%d")
        session_file = Path("logs/sessions") / today / f"session_{session_id}.json"

        if not session_file.exists():
            logger.warning(f"Arquivo de sessao nao encontrado: {session_file}")
            return {"status": "error", "message": "Session file not found"}

        # Ler dados da sessao
        with open(session_file, "r", encoding="utf-8") as f:
            session_data = json.load(f)

        # Verificar se ja esta fechada
        current_status = session_data.get("session_metadata", {}).get("session_status")
        if current_status == "closed":
            logger.info(f"Sessao {session_id} ja estava fechada")
            return {"status": "already_closed", "message": "Session already closed"}

        # Atualizar status para closed
        session_data["session_metadata"]["session_status"] = "closed"
        session_data["session_metadata"]["session_end"] = datetime.now().isoformat()
        session_data["session_metadata"]["session_last_update"] = datetime.now().isoformat()
        session_data["session_metadata"]["close_reason"] = reason

        # Escrever de volta
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(session_data, f, ensure_ascii=False, indent=2)

        # Remover do activity tracker
        mark_session_closed(session_id)

        # Sincronizar com Supabase
        try:
            from src.shared_lib.utils.logger_supabase import sync_log_to_supabase
            sync_log_to_supabase(session_data)
        except Exception as e:
            logger.debug(f"Supabase sync failed: {e}")

        logger.info(f"Session {session_id} closed successfully via {reason}")

        return {
            "status": "success",
            "session_id": session_id,
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Erro ao processar fechamento de sessao: {e}")
        return {"status": "error", "message": str(e)}
